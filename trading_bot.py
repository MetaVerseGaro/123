"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import json
import asyncio
import traceback
import sys
import random
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, List, Tuple, Set, Any
from collections import deque, OrderedDict

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot


class _AsyncCache:
    """Lightweight async cache with TTL and debug logging."""

    def __init__(self, logger: TradingLogger, debug: bool = False):
        self._store: Dict[str, Tuple[float, float, Any]] = {}
        self.logger = logger
        self.debug = debug

    def invalidate(self, *keys: str):
        for key in keys:
            self._store.pop(key, None)
            if self.debug:
                self.logger.log(f"[CACHE] invalidate {key}", "DEBUG")

    async def get(self, key: str, ttl: float, fetcher, force: bool = False):
        now = time.time()
        if (not force) and key in self._store:
            ts, cache_ttl, val = self._store[key]
            if now - ts < cache_ttl:
                if self.debug:
                    self.logger.log(f"[CACHE] hit {key}", "DEBUG")
                return val
        if self.debug:
            self.logger.log(f"[CACHE] miss {key}", "DEBUG")
        val = await fetcher()
        self._store[key] = (now, ttl, val)
        return val

    def peek(self, key: str, max_age: float):
        """Non-blocking read; return value if not older than max_age."""
        now = time.time()
        if key not in self._store:
            return None
        ts, ttl, val = self._store[key]
        if now - ts < max_age:
            return val
        return None

    def keys(self) -> List[str]:
        return list(self._store.keys())


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool
    min_order_size: Optional[Decimal] = None
    max_position_count: int = 0
    basic_release_timeout_minutes: int = 0
    enable_advanced_risk: bool = True
    enable_basic_risk: bool = True
    webhook_sl: bool = False
    webhook_sl_fast: bool = False
    webhook_reverse: bool = False
    zigzag_pivot_file: Optional[str] = None
    webhook_basic_direction_file: Optional[str] = None
    max_fast_close_qty: Optional[Decimal] = None

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


@dataclass
class PivotPoint:
    """External ZigZag pivot enriched with price information."""
    label: str
    price: Decimal
    close_time: datetime
    timeframe: str
    raw_ticker: str


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Cache helper
        debug_flag = ("--debug" in sys.argv) or (str(os.getenv("DEBUG_CACHE", "false")).lower() == "true")
        self.cache = _AsyncCache(self.logger, debug=debug_flag)
        # TTL settings (seconds)
        self.ttl_bbo = float(os.getenv("TTL_BBO_SEC", "0.3"))
        self.ttl_bbo_cache = float(os.getenv("BBO_CACHE_TTL_S", "0.5"))
        self.bbo_force_timeout = float(os.getenv("BBO_FORCE_TIMEOUT_MS", "150")) / 1000.0
        self.ttl_last_price = float(os.getenv("TTL_LAST_PRICE_SEC", "0.5"))
        self.ttl_active_orders = float(os.getenv("TTL_ACTIVE_ORDERS_SEC", "2"))
        self.ttl_position = float(os.getenv("TTL_POSITION_SEC", "1.5"))
        # Balance TTL is short (3-5s) and does not reuse equity TTL
        self.ttl_balance = float(os.getenv("TTL_BALANCE_SEC", "4"))
        self.ttl_equity = float(os.getenv("TTL_EQUITY_SEC", "60"))
        self.ttl_order_info = float(os.getenv("TTL_ORDER_INFO_SEC", "1.5"))
        self.pivot_debounce_ms = int(os.getenv("PIVOT_DEBOUNCE_MS", "150"))
        self._last_pivot_reload_ts: float = 0.0
        self._pivot_reload_in_progress: bool = False

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None
        self.stop_loss_triggered = False
        self.dynamic_stop_price: Optional[Decimal] = None
        self.dynamic_stop_direction: Optional[str] = None
        self.current_direction: Optional[str] = self.config.direction.lower()
        self.reversing = False
        self.last_confirmed_low: Optional[Decimal] = None
        self.last_confirmed_high: Optional[Decimal] = None
        self.account_name = os.getenv('ACCOUNT_NAME', '').strip()
        self.stop_loss_enabled = str(os.getenv('STOP_LOSS_ENABLED', 'true')).lower() == 'true'
        self.enable_auto_reverse = str(os.getenv('ENABLE_AUTO_REVERSE', 'true')).lower() == 'true'
        self._equity_cache: Optional[Decimal] = None
        self._equity_cache_ts: float = 0.0
        self._equity_cache_ttl: float = 60.0  # seconds
        self._equity_last_nonzero: Optional[Decimal] = None
        self._last_stop_eval_price: Optional[Decimal] = None
        self._pivot_file_mtime: float = 0.0
        self.net_failure_count: int = 0
        self.last_error_notified_msg: Optional[str] = None
        self.last_error_notified_ts: float = 0.0
        self.last_pnl_fetch_ts: float = 0.0
        self._orders_cache = None
        self._orders_cache_ts: float = 0.0
        self._orders_cache_ttl: float = 100.0  # seconds
        self._position_cache: Optional[Decimal] = None
        self._position_cache_ts: float = 0.0
        self._position_cache_ttl: float = 100.0  # seconds
        # 动态 SL 无单独开关：开启止损即开启动态 SL
        self.enable_dynamic_sl = self.stop_loss_enabled
        self.enable_zigzag = str(os.getenv('ENABLE_ZIGZAG', 'true')).lower() == 'true'
        self.auto_reverse_fast = str(os.getenv('AUTO_REVERSE_FAST', 'true')).lower() == 'true'
        self.enable_notifications = str(os.getenv('ENABLE_NOTIFICATIONS', 'false')).lower() == 'true'
        self.daily_pnl_report = str(os.getenv('DAILY_PNL_REPORT', 'false')).lower() == 'true'
        self.current_sl_order_id: Optional[Decimal] = None
        self.break_buffer_ticks = Decimal(os.getenv('ZIGZAG_BREAK_BUFFER_TICKS', '10'))
        self.zigzag_timeframe = os.getenv('ZIGZAG_TIMEFRAME', '1m')
        self.zigzag_timeframe_sec = self._parse_timeframe_to_seconds(self.zigzag_timeframe)
        self.zigzag_timeframe_key = self._normalize_timeframe_key(self.zigzag_timeframe)
        self.symbol_base = self._extract_symbol_base(self.config.ticker)
        self.base_dir = Path(__file__).resolve().parent
        pivot_path_cfg = getattr(config, "zigzag_pivot_file", None)
        dir_path_cfg = getattr(config, "webhook_basic_direction_file", None)
        self.zigzag_pivot_file = self._resolve_path(pivot_path_cfg or os.getenv('ZIGZAG_PIVOT_FILE', 'zigzag_pivots.json'))
        self.basic_direction_file = self._resolve_path(dir_path_cfg or os.getenv('WEBHOOK_BASIC_DIRECTION_FILE', 'webhook_basic_direction.json'))
        self.recent_pivots: deque[PivotPoint] = deque(maxlen=12)
        self._processed_pivot_keys: Set[Tuple[str, str]] = set()
        # Cache built pivot points to avoid repeated OHLC fetches for the same pivot
        self._pivot_point_cache: Dict[Tuple[str, str], PivotPoint] = {}
        self._last_pivot_poll: float = 0.0
        default_interval = float(self.zigzag_timeframe_sec or 60)
        self._pivot_poll_interval: float = float(os.getenv("PIVOT_POLL_INTERVAL_SEC", str(int(default_interval))))
        self._webhook_poll_interval: float = float(os.getenv("WEBHOOK_POLL_INTERVAL_SEC", "5"))
        self._last_webhook_poll: float = 0.0
        self.webhook_sl = bool(getattr(config, "webhook_sl", False) or str(os.getenv("WEBHOOK_SL", "false")).lower() == "true")
        self.webhook_sl_fast = bool(getattr(config, "webhook_sl_fast", False) or str(os.getenv("WEBHOOK_SL_FAST", "false")).lower() == "true")
        self.webhook_reverse = bool(getattr(config, "webhook_reverse", False) or str(os.getenv("WEBHOOK_REVERSE", "false")).lower() == "true")
        self.webhook_latest_direction: Optional[str] = None
        self.webhook_stop_mode: Optional[str] = None  # None | fast | slow
        self.webhook_target_direction: Optional[str] = None
        self.webhook_reverse_pending: bool = False
        self.webhook_block_trading: bool = False
        self.webhook_unwind_last_ts: float = 0.0
        # fast close chunk size for fast reverse/fast webhook stop
        try:
            cfg_fast_close = getattr(config, "max_fast_close_qty", None)
            env_fast_close = os.getenv("MAX_FAST_CLOSE_QTY", None)
            val = cfg_fast_close if cfg_fast_close not in (None, "", 0) else env_fast_close
            self.max_fast_close_qty = Decimal(str(val)) if val not in (None, "") else Decimal("0.5")
        except Exception:
            self.max_fast_close_qty = Decimal("0.5")
        # Risk control
# Risk control
        self.risk_pct = Decimal(os.getenv("RISK_PCT", "0.5"))
        self.release_timeout_minutes = int(os.getenv("RELEASE_TIMEOUT_MINUTES", "10"))
        self.basic_release_timeout_minutes = getattr(config, "basic_release_timeout_minutes", 0) or 0
        self.stop_new_orders_equity_threshold = Decimal(os.getenv("STOP_NEW_ORDERS_EQUITY_THRESHOLD", "50"))
        self.stop_new_orders = False
        self.stop_new_since: Optional[float] = None
        self.basic_full_since: Optional[float] = None
        self.redundancy_insufficient_since: Optional[float] = None
        self.last_new_order_time = time.time()
        self.last_release_attempt = 0
        self.last_stop_new_notify = False
        self.pending_reverse_direction: Optional[str] = None
        self.pending_reverse_state: Optional[str] = None  # None | waiting_next_pivot | unwinding
        self.pending_original_direction: Optional[str] = None
        self.last_daily_pnl_date: Optional[str] = None
        self.daily_pnl_baseline: Optional[Decimal] = None
        self.last_daily_sent_date: Optional[str] = None
        cfg_adv_risk = getattr(config, "enable_advanced_risk", None)
        cfg_basic_risk = getattr(config, "enable_basic_risk", None)
        self.enable_advanced_risk = (str(cfg_adv_risk).lower() == "true") if cfg_adv_risk is not None else str(os.getenv("ENABLE_ADVANCED_RISK", "true")).lower() == "true"
        self.enable_basic_risk = (str(cfg_basic_risk).lower() == "true") if cfg_basic_risk is not None else True
        # 止损关闭：不止损、不冗余、不自动反手，但仍记录 ZigZag
        if not self.stop_loss_enabled:
            self.enable_dynamic_sl = False
            self.enable_auto_reverse = False
            self.enable_advanced_risk = False
        # 进阶风险关闭且未启用止损，则不执行进阶风控（可记录 ZigZag）
        if not self.enable_advanced_risk and self.stop_loss_enabled:
            self.enable_zigzag = False
            self.enable_dynamic_sl = False
            self.enable_auto_reverse = False
        if self.webhook_sl:
            if self.enable_advanced_risk:
                self.logger.log("[CONFIG] Advanced risk enabled, ignoring webhook_sl to keep modes exclusive", "WARNING")
                self.webhook_sl = False
            else:
                self.enable_zigzag = False
                self.enable_dynamic_sl = False
                self.enable_auto_reverse = False
                self.enable_advanced_risk = False
        # 如果未开启ZigZag 或未开启自动反手，则不使用 fast 模式
        if not self.enable_zigzag:
            self.enable_auto_reverse = False
        if not self.enable_auto_reverse:
            self.auto_reverse_fast = False
        # 交易所/交易对最小下单量：优先配置文件，其次 ENV 覆盖
        try:
            cfg_min = config.min_order_size if getattr(config, "min_order_size", None) not in (None, 0) else None
            env_min = Decimal(os.getenv("MIN_ORDER_SIZE", "0.005"))
            self.min_order_size = Decimal(cfg_min) if cfg_min is not None else env_min
        except Exception:
            self.min_order_size = Decimal("0.005")
        # 基础风险最大持仓量 = quantity * max_position_count
        self.max_position_count = int(getattr(config, "max_position_count", 0) or 0)
        self.max_position_limit = (self.config.quantity * self.max_position_count) if self.max_position_count > 0 else None

        # Register order callback
        self._setup_websocket_handlers()

    def _invalidate_order_cache(self):
        self.cache.invalidate(f"orders:{self.config.contract_id}")

    def _invalidate_position_cache(self):
        self.cache.invalidate(f"position:{self.config.contract_id}")

    def _invalidate_order_info_cache(self, order_id: Optional[str] = None):
        if order_id:
            self.cache.invalidate(f"order_info:{order_id}")
        else:
            # Broad invalidation (used after bulk cancel)
            keys = [k for k in self.cache.keys() if k.startswith("order_info:")]
            if keys:
                self.cache.invalidate(*keys)

    async def _get_bbo_cached(self, force: bool = False) -> Tuple[Decimal, Decimal]:
        key = f"bbo:{self.config.contract_id}"
        async def fetch_bbo():
            return await self.exchange_client.fetch_bbo_prices(self.config.contract_id)

        if force:
            try:
                val = await asyncio.wait_for(fetch_bbo(), timeout=self.bbo_force_timeout)
                if self.cache.debug:
                    self.logger.log("[CACHE] BBO force ok", "DEBUG")
                return val
            except (asyncio.TimeoutError, asyncio.CancelledError, ConnectionError):
                cached = self.cache.peek(key, self.ttl_bbo_cache)
                if cached:
                    if self.cache.debug:
                        self.logger.log("[CACHE] BBO timeout→cached", "DEBUG")
                    return cached
                if self.cache.debug:
                    self.logger.log("[CACHE] BBO timeout→no-cache", "DEBUG")
                raise
        return await self.cache.get(
            key,
            self.ttl_bbo,
            fetch_bbo,
            force=False
        )

    async def _get_last_trade_price_cached(self, best_bid: Optional[Decimal] = None, best_ask: Optional[Decimal] = None, force: bool = False) -> Optional[Decimal]:
        key = f"last_price:{self.config.contract_id}"

        async def _fetch():
            # Prefer given bid/ask if provided
            if best_bid is not None and best_ask is not None:
                try:
                    return (Decimal(best_bid) + Decimal(best_ask)) / 2
                except Exception:
                    pass
            try:
                bid, ask = await self._get_bbo_cached(force=force)
                return (bid + ask) / 2
            except Exception as exc:
                self.logger.log(f"[PRICE] Fallback trade price failed: {exc}", "WARNING")
                return None

        return await self.cache.get(key, self.ttl_last_price, _fetch, force=force)

    async def _get_active_orders_cached(self) -> list:
        key = f"orders:{self.config.contract_id}"
        return await self.cache.get(
            key,
            self.ttl_active_orders,
            lambda: self.exchange_client.get_active_orders(self.config.contract_id)
        )

    async def _get_position_signed_cached(self, force: bool = False) -> Decimal:
        key = f"position:{self.config.contract_id}"
        return await self.cache.get(
            key,
            self.ttl_position,
            lambda: self.exchange_client.get_account_positions(),
            force=force
        )

    def _order_ws_available(self) -> bool:
        return bool(getattr(self.exchange_client, "ws_manager", None)) or bool(getattr(self.exchange_client, "order_ws_enabled", False))

    async def _get_order_info_cached(self, order_id: str, force: bool = False):
        key = f"order_info:{order_id}"
        return await self.cache.get(
            key,
            self.ttl_order_info,
            lambda: self.exchange_client.get_order_info(order_id),
            force=force
        )


    def _set_stop_new_orders(self, enable: bool):
        """Set stop_new_orders flag and track duration."""
        if enable:
            if not self.stop_new_orders:
                self.stop_new_since = time.time()
            self.stop_new_orders = True
        else:
            self.stop_new_orders = False
            self.stop_new_since = None
            self.last_stop_new_notify = False
            self.redundancy_insufficient_since = None

    def _parse_timeframe_to_seconds(self, tf: str) -> int:
        """Convert timeframe string like '1m','5m','1h' to seconds."""
        try:
            if tf.endswith("m"):
                return int(tf[:-1]) * 60
            if tf.endswith("h"):
                return int(tf[:-1]) * 3600
            if tf.endswith("s"):
                return int(tf[:-1])
            return int(tf)
        except Exception:
            return 60

    def _resolve_path(self, path_str: str) -> Path:
        """Resolve a file path relative to the project root."""
        path = Path(str(path_str))
        if not path.is_absolute():
            return (self.base_dir / path).resolve()
        return path

    def _extract_symbol_base(self, ticker: str) -> str:
        """Extract base symbol, trimming common quote suffixes after the leading letters."""
        base_chars = []
        for ch in ticker:
            if ch.isalpha():
                base_chars.append(ch)
            else:
                break
        candidate = "".join(base_chars) or ticker
        for quote in ("USDT", "USD", "USDC", "PERP"):
            if candidate.upper().endswith(quote) and len(candidate) > len(quote):
                return candidate.upper()[:-len(quote)]
        return candidate.upper()

    def _normalize_timeframe_key(self, tf: str) -> str:
        """Normalize timeframe to minutes string for pivot store matching."""
        tf = str(tf).strip().lower()
        if tf.endswith("m"):
            return tf[:-1] or "0"
        if tf.endswith("h"):
            try:
                return str(int(tf[:-1]) * 60)
            except Exception:
                return tf
        if tf.endswith("s"):
            try:
                sec = int(tf[:-1])
                return str(int(sec // 60)) if sec % 60 == 0 else str(sec)
            except Exception:
                return tf
        return tf

    def _parse_pivot_time(self, value: str) -> Optional[datetime]:
        """Parse pivot close time into UTC datetime."""
        candidates = [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in candidates:
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _determine_initial_direction_and_stop(self, entries: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """From latest pivots determine direction via first HH/LL, then nearest stop pivot for that direction."""
        if not entries:
            return None, None
        entries_desc = sorted(entries, key=lambda x: x["close_time"], reverse=True)
        direction: Optional[str] = None
        stop_entry: Optional[Dict[str, Any]] = None
        # First pass: find the most recent HH/LL to set direction
        for entry in entries_desc:
            label = entry.get("label")
            if not label:
                continue
            if label == "HH":
                direction = "buy"
                break
            if label == "LL":
                direction = "sell"
                break
        if direction is None:
            return None, None
        # Second pass: find nearest stop pivot matching direction
        for entry in entries_desc:
            label = entry.get("label")
            if not label:
                continue
            if direction == "buy" and label in ("HL", "LL"):
                stop_entry = entry
                break
            if direction == "sell" and label in ("HH", "LH"):
                stop_entry = entry
                break
        return direction, stop_entry

    def _load_json_file(self, path: Path) -> Optional[Dict]:
        """Load JSON content from a file if it exists."""
        try:
            if not path.exists():
                return None
            try:
                self._pivot_file_mtime = path.stat().st_mtime
            except Exception:
                pass
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            self.logger.log(f"[WEBHOOK] Failed to load {path}: {exc}", "WARNING")
            return None

    def _persist_pivot_price(self, entry: Dict[str, Any], price: Decimal):
        """Persist price into pivot store to avoid future REST fetch."""
        try:
            store = self._load_json_file(self.zigzag_pivot_file) or {}
            base_bucket = store.get(self.symbol_base, {}) if isinstance(store, dict) else {}
            tf_bucket = base_bucket.get(self.zigzag_timeframe_key) or base_bucket.get(str(self.zigzag_timeframe_key))
            if not tf_bucket:
                return
            updated = False
            for item in tf_bucket:
                try:
                    label = str(item.get("label")).upper()
                    close_raw = item.get("close_time_utc") or item.get("pivot_bar_close") or item.get("close_time")
                    close_dt = self._parse_pivot_time(str(close_raw)) if close_raw else None
                    if close_dt and close_dt == entry["close_time"] and label == entry["label"]:
                        # store per-exchange price to avoid ticker drift
                        exchange_key = entry.get("raw_ticker") or self.config.exchange
                        price_key = f"price_{exchange_key}"
                        item[price_key] = str(price)
                        updated = True
                        break
                except Exception:
                    continue
            if updated:
                # Write back
                self.zigzag_pivot_file.parent.mkdir(parents=True, exist_ok=True)
                tmp = self.zigzag_pivot_file.with_suffix(".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(store, f, ensure_ascii=False, indent=2, default=str)
                tmp.replace(self.zigzag_pivot_file)
                try:
                    self._pivot_file_mtime = self.zigzag_pivot_file.stat().st_mtime
                except Exception:
                    pass
        except Exception as exc:
            self.logger.log(f"[ZIGZAG] persist pivot price failed: {exc}", "WARNING")

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Try to close positions and orders before disconnect
            await self._close_all_positions_and_orders()
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")
            if self.enable_notifications:
                await self.send_notification(f"[STOP] Bot stopped. Reason: {reason}")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")
        # Reset failure counters after a clean shutdown
        self.net_failure_count = 0
        self.last_error_notified_msg = None
        self.last_error_notified_ts = 0.0

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)
                            
                    # PATCH
                    if self.config.exchange == "extended":
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{Decimal(message.get('size')) - filled_size} @ {message.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                # Invalidate caches on any order event
                self._invalidate_order_cache()
                self._invalidate_position_cache()
                self._invalidate_order_info_cache(order_id)

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def _calculate_wait_time(self) -> Decimal:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        # if the program detects active_close_orders during startup, it is necessary to consider cooldown_time
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _get_equity_snapshot(self) -> Optional[Decimal]:
        """Fetch equity with fallbacks."""
        now_ts = time.time()
        if self._equity_cache is not None and (now_ts - self._equity_cache_ts) < self.ttl_equity:
            return self._equity_cache

        async def _fetch_equity():
            equity_val = None
            if hasattr(self.exchange_client, "get_account_equity"):
                try:
                    equity_val = await self.exchange_client.get_account_equity()
                except Exception as e:
                    self.logger.log(f"[RISK] get_account_equity failed: {e}", "WARNING")
            if equity_val is None:
                try:
                    pos_signed = await self._get_position_signed_cached(force=True)
                    best_bid, best_ask = await self._get_bbo_cached()
                    mid_price = (best_bid + best_ask) / 2
                    equity_val = abs(pos_signed) * mid_price
                except Exception as e:
                    self.logger.log(f"[RISK] equity fallback failed: {e}", "ERROR")
                    equity_val = None
            return equity_val

        equity = await self.cache.get("equity", self.ttl_equity, _fetch_equity, force=False)
        if equity is None or equity <= 0:
            self.cache.invalidate("equity")
            return self._equity_last_nonzero
        self._equity_cache = equity
        self._equity_cache_ts = now_ts
        self._equity_last_nonzero = equity
        return equity

    async def _get_balance_snapshot(self) -> Optional[Decimal]:
        """Fetch balance with short TTL; reuse equity failure semantics."""
        key = "balance"

        async def _fetch_balance():
            bal = None
            if hasattr(self.exchange_client, "get_available_balance"):
                try:
                    bal = await self.exchange_client.get_available_balance()
                except Exception as e:
                    self.logger.log(f"[RISK] get_available_balance failed: {e}", "WARNING")
            return bal

        bal = await self.cache.get(key, self.ttl_balance, _fetch_balance, force=False)
        if bal is None or bal <= 0:
            self.cache.invalidate(key)
            return self._equity_last_nonzero
        return bal

    async def _maybe_send_daily_pnl(self):
        """Send daily PnL at 20:00 UTC+8 (12:00 UTC) via Telegram only."""
        if not (self.enable_notifications and self.daily_pnl_report):
            return
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not (telegram_token and telegram_chat_id):
            return
        now = time.gmtime()
        current_date = f"{now.tm_year:04d}-{now.tm_mon:02d}-{now.tm_mday:02d}"
        equity = await self._get_equity_snapshot()
        now_ts = time.time()
        if self.last_daily_pnl_date != current_date:
            self.daily_pnl_baseline = equity
            self.last_daily_pnl_date = current_date
            self.last_daily_sent_date = None
            self.last_pnl_fetch_ts = 0.0
            return
        if now.tm_hour == 12 and self.last_daily_sent_date != current_date:
            if hasattr(self.exchange_client, "get_account_pnl"):
                if now_ts - self.last_pnl_fetch_ts > 300:
                    try:
                        pnl_val = await self.exchange_client.get_account_pnl()
                        self.last_pnl_fetch_ts = now_ts
                        if pnl_val is not None:
                            equity = pnl_val
                    except Exception as e:
                        self.logger.log(f"[PNL] get_account_pnl failed: {e}", "WARNING")
            if equity is None or self.daily_pnl_baseline is None:
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text("[PNL] Daily PnL unavailable (missing equity data)")
            else:
                pnl = equity - self.daily_pnl_baseline
                msg = f"[PNL] {current_date} Equity: {equity} | PnL: {pnl}"
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text(msg)
            self.last_daily_sent_date = current_date

    async def _place_take_profit_order(self, quantity: Decimal, filled_price: Decimal) -> bool:
        """Place a take-profit order; on Lighter, chase better-than-target prices with 5s timeout retries."""
        close_side = self.config.close_order_side
        target_price = (filled_price * (1 + self.config.take_profit/100)
                        if close_side == 'sell'
                        else filled_price * (1 - self.config.take_profit/100))

        # Non-lighter: single placement at target
        if self.config.exchange != "lighter":
            close_order_result = await self.exchange_client.place_close_order(
                self.config.contract_id,
                quantity,
                target_price,
                close_side
            )
            if not close_order_result.success:
                raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")
            return True

        # Lighter: adaptive maker pricing with retries when price is better than target
        while True:
            best_bid, best_ask = await self._get_bbo_cached(force=True)
            if close_side == 'sell':
                desired_price = max(target_price, best_ask + self.config.tick_size)
                has_better = desired_price > target_price
            else:
                desired_price = min(target_price, best_bid - self.config.tick_size)
                if desired_price <= 0:
                    desired_price = target_price
                has_better = desired_price < target_price

            place_fn = getattr(self.exchange_client, "place_tp_order", None)
            if place_fn:
                close_order_result = await place_fn(
                    self.config.contract_id,
                    quantity,
                    desired_price,
                    close_side
                )
            else:
                close_order_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    quantity,
                    desired_price,
                    close_side
                )
            if not close_order_result.success:
                raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")

            # 如果下单后立即被 post-only 拒绝（CANCELED-POST-ONLY），立即重试
            curr = getattr(self.exchange_client, "current_order", None)
            if curr and curr.status == "CANCELED-POST-ONLY":
                self.logger.log("[CLOSE] TP post-only rejected, retrying with adjusted price", "WARNING")
                continue

            # If we are at target (no better price), keep the order and exit
            if not has_better or desired_price == target_price:
                return True

            # Otherwise, wait up to 5s; if not filled, cancel and retry
            start_time = time.time()
            while time.time() - start_time < 5:
                current_order = getattr(self.exchange_client, "current_order", None)
                if current_order and current_order.status == "FILLED":
                    return True
                await asyncio.sleep(0.2)

            # Timeout: cancel and retry with fresh price
            try:
                await self.exchange_client.cancel_order(close_order_result.order_id)
            except Exception as e:
                self.logger.log(f"[CLOSE] Timeout cancel failed: {e}", "WARNING")
            self.logger.log("[CLOSE] Reposting take-profit due to timeout at better-than-target price", "WARNING")

    async def _place_stop_loss_native(self, quantity: Decimal, trigger_price: Decimal, side: str):
        """Place native stop-loss via exchange client if supported."""
        if hasattr(self.exchange_client, "place_stop_loss_order"):
            # Cancel previous SL
            if self.current_sl_order_id:
                try:
                    await self.exchange_client.cancel_order(self.current_sl_order_id)
                except Exception as e:
                    self.logger.log(f"Cancel previous SL failed: {e}", "WARNING")
            sl_result = await self.exchange_client.place_stop_loss_order(quantity, trigger_price, side)
            if sl_result.success:
                self.current_sl_order_id = sl_result.order_id
                self.logger.log(f"[SL] Placed native stop-loss {side} qty={quantity} trig={trigger_price}", "INFO")
            else:
                self.logger.log(f"[SL] Failed to place native stop-loss: {sl_result.error_message}", "ERROR")
        else:
            self.logger.log("[SL] Exchange does not support native stop-loss", "WARNING")

    async def _close_all_positions_and_orders(self):
        """Cancel all orders and close any open position reduce-only."""
        try:
            if hasattr(self.exchange_client, "cancel_all_orders"):
                await self.exchange_client.cancel_all_orders()
        except Exception as e:
            self.logger.log(f"Error cancelling all orders: {e}", "ERROR")

        try:
            pos_signed = await self._get_position_signed_cached()
        except Exception as e:
            self.logger.log(f"Error fetching position during cleanup: {e}", "ERROR")
            pos_signed = Decimal(0)

        pos_abs = abs(pos_signed)
        if pos_abs > 0:
            close_side = 'sell' if pos_signed > 0 else 'buy'
            try:
                await self.exchange_client.reduce_only_close_with_retry(pos_abs, close_side)
            except Exception as e:
                self.logger.log(f"Error closing position during cleanup: {e}", "ERROR")

        # Cancel SL
        if self.current_sl_order_id:
            try:
                await self.exchange_client.cancel_order(self.current_sl_order_id)
            except Exception:
                pass
        self.current_sl_order_id = None

    def _update_confirmed_pivots(self, pivot: PivotPoint):
        """Update confirmed high/low based on external ZigZag pivot."""
        if pivot.label in ("HH", "LH"):
            self.last_confirmed_high = pivot.price
        if pivot.label in ("LL", "HL"):
            self.last_confirmed_low = pivot.price
        self.recent_pivots.append(pivot)

    def _compute_dynamic_stop(self, direction: str) -> Optional[Decimal]:
        """Compute dynamic stop based on confirmed pivots and tick buffer (10 ticks)."""
        tick = self.config.tick_size
        buffer_ticks = Decimal("10") * tick
        if direction == "buy" and self.last_confirmed_low is not None:
            return self.last_confirmed_low - buffer_ticks
        if direction == "sell" and self.last_confirmed_high is not None:
            return self.last_confirmed_high + buffer_ticks
        return None

    def _get_candle_close_ts(self, candle: Any) -> Optional[int]:
        """Extract candle close timestamp in ms from various field names."""
        keys = ["timestamp", "close_time", "close_timestamp", "ts", "t"]
        for key in keys:
            val = None
            if isinstance(candle, dict):
                val = candle.get(key)
            else:
                val = getattr(candle, key, None)
            if val is None:
                continue
            try:
                return int(val)
            except Exception:
                continue
        return None

    async def _fetch_candle_for_time(self, close_time: datetime) -> Optional[Tuple[Decimal, Decimal]]:
        """Fetch OHLC candle that ends at the given UTC close time."""
        target_ms = int(close_time.replace(tzinfo=timezone.utc).timestamp() * 1000)
        tf = self.zigzag_timeframe
        tf_ms = int(self.zigzag_timeframe_sec or 60) * 1000
        tolerance_ms = max(1000, tf_ms // 2)
        cache_key = (self.config.exchange, self.config.ticker, tf, target_ms)
        now_ts = time.time()
        if not hasattr(self, "_candle_lru"):
            self._candle_lru = {}
        if not hasattr(self, "_candle_lru_order"):
            self._candle_lru_order = OrderedDict()
        # TTL reuse: align with pivot file (approx 30 min)
        candle_ttl = float(os.getenv("CANDLE_CACHE_TTL_SEC", "1800"))
        cached = self._candle_lru.get(cache_key)
        if cached:
            ts, val = cached
            if now_ts - ts < candle_ttl:
                # move to end
                self._candle_lru_order.pop(cache_key, None)
                self._candle_lru_order[cache_key] = True
                if self.cache.debug:
                    self.logger.log("[CACHE] candle hit", "DEBUG")
                return val
            else:
                self._candle_lru.pop(cache_key, None)
                self._candle_lru_order.pop(cache_key, None)
        if not hasattr(self, "_candle_pending"):
            self._candle_pending = {}
        if cache_key in self._candle_pending:
            if self.cache.debug:
                self.logger.log("[CACHE] candle join pending", "DEBUG")
            return await self._candle_pending[cache_key]
        async def _do_fetch():
            if hasattr(self.exchange_client, "fetch_candle_by_close_time"):
                try:
                    candle = await self.exchange_client.fetch_candle_by_close_time(
                        timeframe=tf,
                        close_time_ms=target_ms
                    )
                    if candle:
                        if isinstance(candle, tuple) and len(candle) == 2:
                            return candle
                        high_val = getattr(candle, "high", None) if not isinstance(candle, dict) else candle.get("high")
                        low_val = getattr(candle, "low", None) if not isinstance(candle, dict) else candle.get("low")
                        if high_val is not None and low_val is not None:
                            return Decimal(str(high_val)), Decimal(str(low_val))
                except Exception as exc:
                    self.logger.log(f"[ZIGZAG] fetch_candle_by_close_time failed: {exc}", "WARNING")
            if hasattr(self.exchange_client, "fetch_history_candles"):
                try:
                    candles = await self.exchange_client.fetch_history_candles(limit=300, timeframe=tf)
                    for c in candles:
                        ts_val = self._get_candle_close_ts(c)
                        if ts_val is None:
                            continue
                        try:
                            ts_ms = int(ts_val)
                        except Exception:
                            continue
                        if ts_ms < 1_000_000_000_000:
                            ts_ms *= 1000
                        if abs(ts_ms - target_ms) > tolerance_ms:
                            continue
                        high_val = getattr(c, "high", None) if not isinstance(c, dict) else c.get("high")
                        low_val = getattr(c, "low", None) if not isinstance(c, dict) else c.get("low")
                        if high_val is None or low_val is None:
                            continue
                        return Decimal(str(high_val)), Decimal(str(low_val))
                except Exception as exc:
                    self.logger.log(f"[ZIGZAG] fetch_history_candles failed: {exc}", "WARNING")
            return None

        fut = asyncio.create_task(_do_fetch())
        self._candle_pending[cache_key] = fut
        try:
            candle = await fut
            if candle:
                # maintain LRU size 128
                self._candle_lru[cache_key] = (now_ts, candle)
                self._candle_lru_order.pop(cache_key, None)
                self._candle_lru_order[cache_key] = True
                while len(self._candle_lru_order) > 128:
                    oldest, _ = self._candle_lru_order.popitem(last=False)
                    self._candle_lru.pop(oldest, None)
            return candle
        finally:
            self._candle_pending.pop(cache_key, None)

    def _load_pivots_for_config(self) -> List[Dict[str, Any]]:
        """Load pivot entries for current symbol/timeframe from persisted store."""
        store = self._load_json_file(self.zigzag_pivot_file) or {}
        base_bucket = store.get(self.symbol_base, {}) if isinstance(store, dict) else {}
        tf_bucket = base_bucket.get(self.zigzag_timeframe_key) or base_bucket.get(str(self.zigzag_timeframe_key))
        if not tf_bucket:
            return []
        pivots: List[Dict[str, Any]] = []
        for item in tf_bucket[-10:]:
            try:
                label = str(item.get("label")).upper()
                raw_ticker = item.get("raw_ticker") or item.get("ticker") or self.config.ticker
                tf_val = item.get("tf") or item.get("timeframe") or self.zigzag_timeframe_key
                if self._normalize_timeframe_key(tf_val) != self.zigzag_timeframe_key:
                    continue
                close_raw = item.get("close_time_utc") or item.get("pivot_bar_close") or item.get("close_time")
                close_dt = self._parse_pivot_time(str(close_raw)) if close_raw else None
                if not label or close_dt is None:
                    continue
                # prefer exchange-specific stored price
                price_val = None
                exchange_key = raw_ticker
                if exchange_key:
                    price_val = item.get(f"price_{exchange_key}")
                if price_val is None:
                    price_val = item.get("price") or item.get("high") or item.get("low")
                pivots.append({
                    "label": label,
                    "raw_ticker": raw_ticker,
                    "close_time": close_dt,
                    "price": Decimal(str(price_val)) if price_val is not None else None,
                })
            except Exception:
                continue
        pivots.sort(key=lambda x: x["close_time"])
        return pivots

    async def _build_pivot_point(self, entry: Dict[str, Any]) -> Optional[PivotPoint]:
        """Build PivotPoint with price derived from exchange OHLC data."""
        price = entry.get("price")
        if price is None:
            candle = await self._fetch_candle_for_time(entry["close_time"])
            if not candle:
                self.logger.log(f"[ZIGZAG] Missing OHLC for pivot at {entry['close_time']}", "WARNING")
                return None
            high, low = candle
            price = high if entry["label"] in ("HH", "LH") else low
            self._persist_pivot_price(entry, price)
        return PivotPoint(
            label=entry["label"],
            price=price,
            close_time=entry["close_time"],
            timeframe=self.zigzag_timeframe_key,
            raw_ticker=entry.get("raw_ticker", self.config.ticker),
        )

    async def _build_pivot_points(self, entries: List[Dict[str, Any]]) -> List[Tuple[Tuple[str, str], PivotPoint]]:
        """Convert raw entries into pivot points with processed keys."""
        result: List[Tuple[Tuple[str, str], PivotPoint]] = []
        for entry in entries:
            key = (entry["close_time"].isoformat(), entry["label"])
            # Skip fully processed pivots to avoid repeated OHLC fetch
            if key in self._processed_pivot_keys:
                if key in self._pivot_point_cache:
                    result.append((key, self._pivot_point_cache[key]))
                continue
            if key in self._pivot_point_cache:
                pivot = self._pivot_point_cache[key]
            else:
                pivot = await self._build_pivot_point(entry)
                if pivot:
                    self._pivot_point_cache[key] = pivot
            if not pivot:
                continue
            result.append((key, pivot))
        result.sort(key=lambda x: x[1].close_time)
        return result

    async def _sync_external_pivots(self, force: bool = False, notify: bool = True):
        """Poll pivot store for new entries and refresh state."""
        if not self.enable_zigzag:
            return
        now = time.time()
        current_mtime = 0.0
        try:
            if self.zigzag_pivot_file.exists():
                current_mtime = self.zigzag_pivot_file.stat().st_mtime
        except Exception:
            current_mtime = 0.0
        if (not force) and (now - self._last_pivot_poll < self._pivot_poll_interval) and (current_mtime == self._pivot_file_mtime):
            return
        self._last_pivot_poll = now
        entries = self._load_pivots_for_config()
        if not entries:
            return
        # Debounce rapid successive file writes
        if current_mtime and current_mtime != self._pivot_file_mtime:
            await asyncio.sleep(self.pivot_debounce_ms / 1000)
            try:
                current_mtime = self.zigzag_pivot_file.stat().st_mtime
            except Exception:
                current_mtime = self._pivot_file_mtime
        pivot_points = await self._build_pivot_points(entries)
        for key, pivot in pivot_points:
            if key in self._processed_pivot_keys:
                continue
            self._processed_pivot_keys.add(key)
            await self._handle_pivot_event(pivot, notify=notify)

    async def _set_initial_direction_from_pivots(self, pivots: List[PivotPoint]):
        """Set initial direction based on latest HH/LL pivot when advanced risk is on."""
        if not pivots or not (self.enable_advanced_risk and self.stop_loss_enabled and self.enable_zigzag):
            return
        candidates = [p for p in pivots if p.label in ("HH", "LL")]
        if not candidates:
            return
        latest = max(candidates, key=lambda p: p.close_time)
        desired_dir = "buy" if latest.label == "HH" else "sell"
        if desired_dir != self.config.direction:
            self.config.direction = desired_dir
            self.current_direction = desired_dir
            self.logger.log(f"[INIT] Direction set to {desired_dir.upper()} via pivot {latest.label} at {latest.close_time}", "INFO")

    async def _initialize_pivots_from_store(self):
        """Load existing pivots on startup to seed state without notifications."""
        if not self.enable_zigzag:
            return
        entries = self._load_pivots_for_config()
        if not entries:
            return
        # 标记已有 pivot，避免启动后重复通知
        for entry in entries:
            try:
                key = (entry["close_time"].isoformat(), entry["label"])
                self._processed_pivot_keys.add(key)
            except Exception:
                continue
        # 仅根据最近 pivot 判向并获取匹配方向的最近止损位，减少 OHLC 查询
        direction, stop_entry = self._determine_initial_direction_and_stop(entries)
        if direction and self.enable_advanced_risk and self.stop_loss_enabled and self.enable_zigzag:
            if direction != self.config.direction:
                self.config.direction = direction
                self.current_direction = direction
                self.logger.log(f"[INIT] Direction set to {direction.upper()} via recent pivots (fast init)", "INFO")
            if stop_entry:
                key = (stop_entry["close_time"].isoformat(), stop_entry["label"])
                pivot_point = self._pivot_point_cache.get(key)
                if not pivot_point:
                    pivot_point = await self._build_pivot_point(stop_entry)
                    if pivot_point:
                        self._pivot_point_cache[key] = pivot_point
                if pivot_point:
                    self._update_confirmed_pivots(pivot_point)
                    key = (pivot_point.close_time.isoformat(), pivot_point.label)
                    self._processed_pivot_keys.add(key)
                    if self.enable_dynamic_sl:
                        await self._refresh_stop_loss(force=True)

    async def _notify_recent_pivots(self):
        """Send notification with recent pivots (latest first)."""
        if not self.enable_notifications or not self.recent_pivots:
            return
        latest = list(self.recent_pivots)[-5:][::-1]
        lines = ["zigzag info:", ""]
        for pivot in latest:
            price_val = pivot.price.quantize(self.config.tick_size) if self.config.tick_size else pivot.price
            close_str = pivot.close_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
            lines.append(
                f"- {self.symbol_base} | TF={self.zigzag_timeframe_key} | {self.config.exchange} | {pivot.label} | close time {close_str}(UTC) | price @{price_val}"
            )
        await self.send_notification("\n".join(lines))

    async def _refresh_stop_loss(self, force: bool = False):
        """Refresh native stop-loss according to current position and dynamic SL."""
        if not self.enable_dynamic_sl:
            return
        # 慢反手过程中不自动下动态止损，待方向确认后再处理
        if self.pending_reverse_state in ("waiting_next_pivot", "unwinding"):
            return
        try:
            pos_signed = await self._get_position_signed_cached(force=force)
        except Exception as e:
            self.logger.log(f"[SL] Failed to fetch position for refresh: {e}", "WARNING")
            return

        pos_abs = abs(pos_signed)
        if pos_abs == 0:
            if self.current_sl_order_id:
                try:
                    await self.exchange_client.cancel_order(self.current_sl_order_id)
                except Exception:
                    pass
                self.current_sl_order_id = None
            self.dynamic_stop_price = None
            self.dynamic_stop_direction = None
            return

        direction = "buy" if pos_signed > 0 else "sell"
        if self.current_direction and self.current_direction != direction:
            direction = self.current_direction
        prev_stop = self.dynamic_stop_price
        if self.dynamic_stop_direction and self.dynamic_stop_direction != direction:
            self.dynamic_stop_price = None
        struct_stop = self._compute_dynamic_stop(direction)
        if struct_stop is None:
            return

        if (not force) and self._last_stop_eval_price is not None:
            if abs(struct_stop - self._last_stop_eval_price) < self.config.tick_size:
                if self.cache.debug:
                    self.logger.log("[SL] Dynamic stop debounced (no meaningful change)", "DEBUG")
                return

        dyn_stop = struct_stop
        if self.dynamic_stop_price is not None:
            if direction == "buy":
                dyn_stop = max(struct_stop, self.dynamic_stop_price)
            else:
                dyn_stop = min(struct_stop, self.dynamic_stop_price)

        if (not force) and self.dynamic_stop_price is not None:
            if abs(dyn_stop - self.dynamic_stop_price) < self.config.tick_size:
                # No meaningful change
                return

        self.dynamic_stop_price = dyn_stop
        self.dynamic_stop_direction = direction
        self._last_stop_eval_price = dyn_stop
        await self._place_stop_loss_native(pos_abs, dyn_stop, 'sell' if direction == "buy" else 'buy')
        if self.enable_notifications and prev_stop != self.dynamic_stop_price:
            try:
                await self.send_notification(f"[SL] Dynamic stop updated from {prev_stop} to {self.dynamic_stop_price} for {direction.upper()}")
            except Exception:
                pass
        # After SL update, re-evaluate redundancy and optionally stop new orders
        await self._run_redundancy_check(direction, pos_signed)
        # Cancel open orders that are beyond the new SL
        try:
            await self._cancel_open_orders_beyond_stop(dyn_stop)
        except Exception as e:
            self.logger.log(f"[SL] Cancel open orders beyond stop failed: {e}", "WARNING")

    async def _cancel_open_orders_beyond_stop(self, stop_price: Decimal):
        """Cancel open-side orders that sit beyond the new stop price."""
        active_orders = await self._get_active_orders_cached()
        for order in active_orders:
            if order.side != self.config.direction:
                continue
            if self.config.direction == "buy" and order.price <= stop_price:
                await self.exchange_client.cancel_order(order.order_id)
            if self.config.direction == "sell" and order.price >= stop_price:
                await self.exchange_client.cancel_order(order.order_id)
        self._invalidate_order_cache()
        if active_orders:
            for order in active_orders:
                self._invalidate_order_info_cache(order.get("order_id", None) if isinstance(order, dict) else getattr(order, "order_id", None))

    async def _run_redundancy_check(self, direction: str, pos_signed: Decimal):
        """Re-evaluate redundancy and update stop_new_orders state."""
        if not (self.stop_loss_enabled and self.enable_advanced_risk):
            return
        try:
            position_amt = abs(pos_signed)
            best_bid, best_ask = await self._get_bbo_cached()
            mid_price = (best_bid + best_ask) / 2
            # Need avg entry price; fallback: mid_price
            avg_price = mid_price
            stop_price = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price) else None
            if hasattr(self.exchange_client, "get_position_detail"):
                try:
                    pos_signed_detail, avg_price_detail = await self.exchange_client.get_position_detail()
                    if pos_signed_detail != 0:
                        avg_price = avg_price_detail
                except Exception as e:
                    self.logger.log(f"[RISK] get_position_detail failed: {e}", "WARNING")

            if stop_price and position_amt > 0:
                if pos_signed > 0:
                    potential_loss = (avg_price - stop_price) * position_amt
                    per_base_loss = (avg_price - stop_price)
                else:
                    potential_loss = (stop_price - avg_price) * position_amt
                    per_base_loss = (stop_price - avg_price)
                if potential_loss < 0:
                    potential_loss = Decimal(0)
                equity = await self._get_equity_snapshot()
                if equity is None:
                    return
                max_loss = equity * (self.risk_pct / Decimal(100))
                redundancy_u = max_loss - potential_loss
                if redundancy_u < 0:
                    redundancy_u = Decimal(0)
                redundancy_base = redundancy_u / per_base_loss if per_base_loss > 0 else Decimal(0)
                # 若冗余为 0 且持仓超额且超额 >= 最小下单量，则主动削减超额
                excess = position_amt - redundancy_base
                if redundancy_base <= 0 and excess >= self.min_order_size and position_amt > 0:
                    try:
                        close_qty = excess
                        close_side = 'sell' if pos_signed > 0 else 'buy'
                        await self.exchange_client.reduce_only_close_with_retry(close_qty, close_side)
                        self.logger.log(f"[RISK] Reduced excess position {close_qty} to keep stop-loss exposure within limit", "WARNING")
                    except Exception as e:
                        self.logger.log(f"[RISK] Failed to trim excess position: {e}", "ERROR")
                if redundancy_base < self.config.quantity:
                    if not self.stop_new_orders and self.enable_notifications:
                        await self.send_notification(f"[RISK] Stop new orders after SL update: redundancy {redundancy_base} < qty {self.config.quantity}")
                    self._set_stop_new_orders(True)
                    if self.redundancy_insufficient_since is None:
                        self.redundancy_insufficient_since = time.time()
                    self.last_stop_new_notify = True
                    # Cancel open orders to avoid new entries
                    try:
                        active_orders = await self._get_active_orders_cached()
                        for order in active_orders:
                            if order.side != self.config.close_order_side:
                                await self.exchange_client.cancel_order(order.order_id)
                    except Exception as e:
                        self.logger.log(f"[RISK] Cancel open orders after SL update failed: {e}", "WARNING")
                else:
                    if self.stop_new_orders and self.enable_notifications:
                        await self.send_notification("[RISK] Resume new orders after SL update: redundancy restored")
                    self._set_stop_new_orders(False)
                    self.redundancy_insufficient_since = None
        except Exception as e:
            self.logger.log(f"[RISK] redundancy check after SL update failed: {e}", "WARNING")

    async def _handle_pivot_event(self, pivot: PivotPoint, notify: bool = True):
        """Handle external ZigZag pivot: update pivots, dynamic SL, and slow reverse follow-up."""
        self._update_confirmed_pivots(pivot)

        if self.pending_reverse_state:
            handled = await self._process_slow_reverse_followup(pivot)
            if handled:
                return

        if self.enable_dynamic_sl:
            await self._refresh_stop_loss(force=True)

        if notify and self.enable_notifications:
            await self._notify_recent_pivots()

    async def _handle_reverse_signal(self, new_direction: str, pivot_price: Decimal):
        """Handle reverse signal based on configured mode."""
        if not self.enable_auto_reverse:
            return
        if self.auto_reverse_fast:
            await self._reverse_position(new_direction, pivot_price)
        else:
            await self._schedule_slow_reverse(new_direction, pivot_price)

    async def _close_position_in_chunks(self, close_side: str, allow_reverse_after: bool = False) -> Decimal:
        """Close position using chunked reduce-only orders to avoid oversized single orders."""
        remaining = abs(await self._get_position_signed_cached())
        max_chunk = self.max_fast_close_qty if self.max_fast_close_qty and self.max_fast_close_qty > 0 else remaining
        loops = 0
        while remaining > 0 and loops < 20:
            chunk = min(remaining, max_chunk)
            try:
                await self.exchange_client.reduce_only_close_with_retry(chunk, close_side)
            except Exception as exc:
                self.logger.log(f"[FAST-CLOSE] Chunk close failed: {exc}", "WARNING")
            await asyncio.sleep(0.5)
            new_remaining = abs(await self._get_position_signed_cached())
            if new_remaining < remaining:
                remaining = new_remaining
            else:
                loops += 1
                if loops % 3 == 0:
                    await asyncio.sleep(1)
        return remaining

    async def _reverse_position(self, new_direction: str, pivot_price: Decimal):
        """Fast reverse: cancel orders, close position, flip direction, re-enter."""
        if self.reversing:
            return
        self.reversing = True
        self.logger.log(f"[REV] Trigger reverse to {new_direction.upper()} via ZigZag at {pivot_price}", "WARNING")
        # Cancel all existing orders on this contract
        if hasattr(self.exchange_client, "cancel_all_orders"):
            try:
                await self.exchange_client.cancel_all_orders()
            except Exception as e:
                self.logger.log(f"[REV] Cancel all orders failed: {e}", "ERROR")

        # Close existing position
        try:
            pos_signed = await self._get_position_signed_cached()
        except Exception as e:
            self.logger.log(f"[REV] Failed to fetch position: {e}", "ERROR")
            pos_signed = Decimal(0)

        pos_abs = abs(pos_signed)
        if pos_abs > 0:
            close_side = 'sell' if pos_signed > 0 else 'buy'
            remaining = await self._close_position_in_chunks(close_side, allow_reverse_after=True)
            if remaining >= self.min_order_size:
                self.logger.log(f"[REV] Unable to fully close position, remaining {remaining}", "WARNING")
                # Proceed but note residual
            else:
                # Residual below min order size is tolerated
                pass

        # Update direction and reset timers
        self.config.direction = new_direction
        self.current_direction = new_direction
        self.dynamic_stop_price = None
        self.dynamic_stop_direction = None
        self.last_open_order_time = 0
        self._invalidate_position_cache()
        self._invalidate_order_cache()
        # Ensure new direction can place orders
        self._set_stop_new_orders(False)

        # Refresh stop loss for new direction
        if self.enable_dynamic_sl:
            await self._refresh_stop_loss(force=True)

        if self.enable_notifications:
            await self.send_notification(f"[DIRECTION] Switched to {new_direction.upper()} (fast reverse)")

        # Immediately place a new open order in new direction
        placed = await self._place_and_monitor_open_order()
        if not placed:
            self.logger.log("[REV] Failed to place new open order after fast reverse (stop_new_orders?)", "WARNING")
        self.reversing = False

    async def _schedule_slow_reverse(self, new_direction: str, pivot_price: Decimal):
        """Slow reverse: pause new opens, observe next pivot before acting."""
        self.logger.log(f"[REV-SLOW] Observe next pivot for potential reverse to {new_direction.upper()} via ZigZag at {pivot_price}", "WARNING")
        self.pending_reverse_direction = new_direction
        self.pending_reverse_state = "waiting_next_pivot"
        self.pending_original_direction = self.config.direction
        self._set_stop_new_orders(True)  # 暂停新开仓，保留现有 TP

    async def _cancel_close_orders(self):
        """Cancel existing close/TP orders."""
        try:
            active_orders = await self._get_active_orders_cached()
            for order in active_orders:
                if order.side == self.config.close_order_side:
                    try:
                        await self.exchange_client.cancel_order(order.order_id)
                    except Exception as e:
                        self.logger.log(f"[REV-SLOW] Cancel close order {order.order_id} failed: {e}", "WARNING")
        except Exception as e:
            self.logger.log(f"[REV-SLOW] Fetch active orders failed: {e}", "WARNING")

    async def _resume_after_invalid_reverse(self):
        """Resume original direction when slow reverse is invalidated."""
        self.pending_reverse_direction = None
        self.pending_reverse_state = None
        self.pending_original_direction = None
        self._set_stop_new_orders(False)

    async def _process_slow_reverse_followup(self, pivot: PivotPoint) -> bool:
        """Process next confirmed pivot for slow reverse logic. Returns True if handled."""
        if self.pending_reverse_state != "waiting_next_pivot":
            return False

        if self.pending_reverse_direction == "sell":
            if pivot.label == "LH":
                await self._cancel_close_orders()
                self.pending_reverse_state = "unwinding"
                return True
            if pivot.label == "HH":
                await self._resume_after_invalid_reverse()
                return True

        if self.pending_reverse_direction == "buy":
            if pivot.label == "HL":
                await self._cancel_close_orders()
                self.pending_reverse_state = "unwinding"
                return True
            if pivot.label == "LL":
                await self._resume_after_invalid_reverse()
                return True

        return False

    async def _perform_slow_unwind(self):
        """Gradually close current position before flipping direction."""
        try:
            pos_signed = await self._get_position_signed_cached()
        except Exception as e:
            self.logger.log(f"[REV-SLOW] Failed to fetch position: {e}", "ERROR")
            return

        pos_abs = abs(pos_signed)
        if pos_abs == 0:
            if self.pending_reverse_direction:
                self.config.direction = self.pending_reverse_direction
                self.pending_reverse_direction = None
                self.pending_reverse_state = None
                self.pending_original_direction = None
                self._set_stop_new_orders(False)
                self.last_open_order_time = 0
                self._invalidate_position_cache()
                self._invalidate_order_cache()
                if self.enable_notifications:
                    await self.send_notification(f"[DIRECTION] Switched to {self.config.direction.upper()} (slow reverse complete)")
                if self.enable_dynamic_sl:
                    await self._refresh_stop_loss(force=True)
                placed = await self._place_and_monitor_open_order()
                if not placed:
                    self.logger.log("[REV-SLOW] Failed to place new open after slow reverse", "WARNING")
            return

        close_side = 'sell' if pos_signed > 0 else 'buy'
        unwind_qty = min(self.config.quantity, pos_abs)
        try:
            await self.exchange_client.reduce_only_close_with_retry(unwind_qty, close_side)
            self.last_release_attempt = time.time()
        except Exception as e:
            self.logger.log(f"[REV-SLOW] Unwind attempt failed: {e}", "WARNING")

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Risk gate: stop new orders when flag is set
            if self.stop_new_orders or self.webhook_block_trading:
                return False
            # Force refresh BBO/last before placing orders
            await self._get_bbo_cached(force=True)
            await self._get_last_trade_price_cached(force=True)
            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = 0.0

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )
            if order_result and order_result.order_id:
                self._invalidate_order_info_cache(order_result.order_id)

            if not order_result.success:
                return False

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass
            await asyncio.sleep(0.25)

            # Handle order result
            handled = await self._handle_order_result(order_result)
            if handled and self.enable_dynamic_sl:
                await self._refresh_stop_loss()
            if handled:
                self.last_new_order_time = time.time()
                self._invalidate_order_cache()
                self._invalidate_position_cache()
            return handled

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
        return False

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price
        self.config.direction = self.config.direction.lower()

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            if self.config.boost_mode:
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side
                )
            else:
                self.last_open_order_time = time.time()
                await self._place_take_profit_order(self.config.quantity, filled_price)
                return True

        else:
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
                if direction == "buy":
                    return new_order_price <= order_result_price
                elif direction == "sell":
                    return new_order_price >= order_result_price
                return False

            current_order_status = None
            if self.config.exchange == "lighter":
                current_order_status = getattr(self.exchange_client.current_order, "status", None)
            if current_order_status is None:
                if not self._order_ws_available():
                    order_info = await self._get_order_info_cached(order_id)
                else:
                    order_info = await self.exchange_client.get_order_info(order_id)
                current_order_status = getattr(order_info, "status", None)
            if current_order_status is None:
                current_order_status = order_result.status or "OPEN"

            while (
                should_wait(self.config.direction, new_order_price, order_result.price)
                and current_order_status == "OPEN"
            ):
                self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
                await asyncio.sleep(5)
                if self.config.exchange == "lighter":
                    current_order_status = self.exchange_client.current_order.status
                else:
                    if not self._order_ws_available():
                        order_info = await self._get_order_info_cached(order_id)
                    else:
                        order_info = await self.exchange_client.get_order_info(order_id)
                    if order_info is not None:
                        current_order_status = order_info.status
                new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
            if self.config.exchange == "lighter":
                cancel_result = await self.exchange_client.cancel_order(order_id)
                start_time = time.time()
                while (time.time() - start_time < 10
                       and self.exchange_client.current_order.status not in ['CANCELED', 'FILLED', 'CANCELED-POST-ONLY']):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED', 'CANCELED-POST-ONLY']:
                    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                try:
                    cancel_result = await self.exchange_client.cancel_order(order_id)
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack" or self.config.exchange == "extended":
                    self.order_filled_amount = cancel_result.filled_size
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            if not self._order_ws_available():
                                order_info = await self._get_order_info_cached(order_id, force=True)
                            else:
                                order_info = await self.exchange_client.get_order_info(order_id)
                            self.order_filled_amount = order_info.filled_size
                self._invalidate_order_info_cache(order_id)

            if self.order_filled_amount > 0:
                close_side = self.config.close_order_side
                if self.config.boost_mode:
                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        filled_price,
                        close_side
                    )
                    success = close_order_result.success
                else:
                    success = await self._place_take_profit_order(self.order_filled_amount, filled_price)

                self.last_open_order_time = time.time()
                if self.config.boost_mode and not success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 120 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders (throttled)
                active_orders = await self._get_active_orders_cached()

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Get positions
                position_signed = await self._get_position_signed_cached()
                position_amt = abs(position_signed)
                equity = await self._get_equity_snapshot()
                # Fallback equity proxy
                best_bid, best_ask = await self._get_bbo_cached()
                mid_price = (best_bid + best_ask) / 2
                if equity is None:
                    if position_amt > 0:
                        equity = position_amt * mid_price
                    else:
                        equity = self._equity_last_nonzero or self._equity_cache

                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                # Check for position mismatch
                mismatch_detected = False
                # 基础风险：最大持仓限制（启用时）
                if self.enable_basic_risk and self.max_position_limit is not None and position_amt >= self.max_position_limit:
                    self._set_stop_new_orders(True)
                    if self.basic_full_since is None:
                        self.basic_full_since = time.time()
                    if position_amt > self.max_position_limit:
                        excess_pos = position_amt - self.max_position_limit
                        close_side = self.config.close_order_side
                        try:
                            await self.exchange_client.reduce_only_close_with_retry(excess_pos, close_side)
                            self.logger.log(f"[RISK-BASIC] Trimmed excess position {excess_pos} over limit {self.max_position_limit}", "WARNING")
                        except Exception as e:
                            self.logger.log(f"[RISK-BASIC] Failed to trim excess position: {e}", "ERROR")

                # 进阶风险：仅当开启时执行冗余检查
                mismatch = position_amt - active_close_amount
                try:
                    if mismatch > 0:
                        # Position larger than active close orders: add reduce-only maker close to trim excess
                        close_qty = mismatch
                        # 若缺口小于最小下单量，则附加一手常规 quantity 一起平掉
                        if close_qty < self.min_order_size:
                            close_qty = close_qty + self.config.quantity
                        close_side = self.config.close_order_side
                        self.logger.log(f"Auto-closing excess position {close_qty} via reduce-only post-only", "WARNING")
                        fix_result = await self.exchange_client.reduce_only_close_with_retry(
                            close_qty, close_side, timeout_sec=5.0, max_attempts=5
                        )
                        if not fix_result.success:
                            raise Exception(f"Reduce-only close failed: {fix_result.error_message}")
                        # 如果平掉数量超过缺口，按超出量取消远端挂单
                        if close_qty > mismatch:
                            excess = close_qty - mismatch
                            cancelled = Decimal(0)
                            sorted_close = sorted(
                                self.active_close_orders,
                                key=lambda o: abs(Decimal(o["price"]) - mid_price),
                                reverse=True
                            )
                            for order in sorted_close:
                                if cancelled >= excess:
                                    break
                                await self.exchange_client.cancel_order(order['id'])
                                cancelled += Decimal(order.get('size', 0))
                                self.logger.log(f"Canceled excess close order {order['id']} size {order.get('size')}", "WARNING")
                    elif mismatch < 0:
                        # Active close orders exceed position: cancel farthest-from-mid until aligned
                        excess = abs(mismatch)
                        cancelled = Decimal(0)
                        # Sort by distance from mid price (farthest first)
                        sorted_close = sorted(self.active_close_orders, key=lambda o: abs(Decimal(o["price"]) - mid_price), reverse=True)
                        for order in sorted_close:
                            if cancelled >= excess:
                                break
                            await self.exchange_client.cancel_order(order['id'])
                            cancelled += Decimal(order.get('size', 0))
                            self.logger.log(f"Canceled excess close order {order['id']} size {order.get('size')}", "WARNING")
                except Exception as fix_err:
                    self.logger.log(f"Auto-fix for position mismatch failed: {fix_err}", "ERROR")
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected, auto-fix failed\n"
                    error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                    self.logger.log(error_message, "ERROR")
                    await self.send_notification(error_message.lstrip())
                    mismatch_detected = True

                # Risk gating: stop new orders if equity < threshold
                if equity is not None and equity < self.stop_new_orders_equity_threshold:
                    self.logger.log(f"[RISK] Equity below threshold ({equity}<{self.stop_new_orders_equity_threshold}), slow unwind.", "ERROR")
                    if self.enable_notifications:
                        await self.send_notification(f"[RISK] Equity below threshold {equity} < {self.stop_new_orders_equity_threshold}, stop new and unwind.")
                    self._set_stop_new_orders(True)
                    try:
                        active_orders = await self._get_active_orders_cached()
                        for order in active_orders:
                            await self.exchange_client.cancel_order(order.order_id)
                    except Exception as e:
                        self.logger.log(f"[RISK] Cancel orders during low equity unwind failed: {e}", "WARNING")
                    try:
                        while position_amt > 0:
                            close_side = 'sell' if position_signed > 0 else 'buy'
                            release_qty = min(self.config.quantity, position_amt)
                            await self.exchange_client.reduce_only_close_with_retry(release_qty, close_side)
                            await asyncio.sleep(0.5)
                            position_signed = await self._get_position_signed_cached()
                            position_amt = abs(position_signed)
                    except Exception as e:
                        self.logger.log(f"[RISK] Unwind during low equity failed: {e}", "ERROR")
                    await self.graceful_shutdown("Equity below threshold - unwound")
                    mismatch_detected = True

                if self.enable_advanced_risk and self.stop_loss_enabled and equity is not None:
                    # Redundancy calculation for stop-new-orders gating
                    avg_price = mid_price
                    if hasattr(self.exchange_client, "get_position_detail"):
                        try:
                            pos_signed_detail, avg_price_detail = await self.exchange_client.get_position_detail()
                            if pos_signed_detail != 0:
                                avg_price = avg_price_detail
                        except Exception as e:
                            self.logger.log(f"[RISK] get_position_detail failed: {e}", "WARNING")

                stop_price = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price) else None
                if self.enable_advanced_risk and self.stop_loss_enabled and stop_price and position_amt > 0:
                    if position_signed > 0:
                        potential_loss = (avg_price - stop_price) * position_amt
                        per_base_loss = (avg_price - stop_price)
                    else:
                        potential_loss = (stop_price - avg_price) * position_amt
                        per_base_loss = (stop_price - avg_price)
                    if potential_loss < 0:
                        potential_loss = Decimal(0)
                    max_loss = equity * (self.risk_pct / Decimal(100))
                    redundancy_u = max_loss - potential_loss
                    if redundancy_u < 0:
                        redundancy_u = Decimal(0)
                    redundancy_base = redundancy_u / per_base_loss if per_base_loss > 0 else Decimal(0)
                    if redundancy_base < self.config.quantity:
                        if not self.stop_new_orders:
                            msg = f"[RISK] Stop new orders: redundancy {redundancy_base} < quantity {self.config.quantity}"
                            self.logger.log(msg, "WARNING")
                            if self.enable_notifications:
                                await self.send_notification(msg)
                        self._set_stop_new_orders(True)
                        if self.redundancy_insufficient_since is None:
                            self.redundancy_insufficient_since = time.time()
                        self.last_stop_new_notify = True
                    else:
                        if self.stop_new_orders and self.enable_notifications and self.last_stop_new_notify:
                            await self.send_notification("[RISK] Resume new orders: redundancy restored")
                        self._set_stop_new_orders(False)
                        self.redundancy_insufficient_since = None

                # Release liquidity: advanced uses release_timeout_minutes；基础风险也可用 basic_release_timeout_minutes
                should_release_advanced = (
                    self.enable_advanced_risk
                    and self.stop_loss_enabled
                    and self.redundancy_insufficient_since is not None
                    and (time.time() - self.redundancy_insufficient_since > self.release_timeout_minutes * 60)
                )
                should_release_basic = (
                    self.enable_basic_risk
                    and not self.enable_advanced_risk
                    and self.basic_release_timeout_minutes > 0
                    and self.basic_full_since is not None
                    and (time.time() - self.basic_full_since > self.basic_release_timeout_minutes * 60)
                )
                if should_release_advanced or should_release_basic:
                    interval = self.release_timeout_minutes if should_release_advanced else self.basic_release_timeout_minutes
                    if time.time() - self.last_release_attempt > interval * 60:
                        self.last_release_attempt = time.time()
                        try:
                            release_qty = min(self.config.quantity, position_amt)
                            if release_qty > 0:
                                close_side = self.config.close_order_side
                                release_result = await self.exchange_client.reduce_only_close_with_retry(
                                    release_qty, close_side, timeout_sec=5.0, max_attempts=5
                                )
                                if release_result.success:
                                    self._invalidate_position_cache()
                                    # 取消一笔最远的 TP 单，其余挂单不动
                                    try:
                                        active_orders_for_cancel = await self._get_active_orders_cached()
                                        close_orders = [
                                            o for o in active_orders_for_cancel
                                            if o.side == self.config.close_order_side
                                        ]
                                        if close_orders:
                                            best_bid, best_ask = await self._get_bbo_cached()
                                            mid_price = (best_bid + best_ask) / 2
                                            farthest = sorted(
                                                close_orders,
                                                key=lambda o: abs(Decimal(o.price) - Decimal(mid_price)),
                                                reverse=True
                                            )[0]
                                            await self.exchange_client.cancel_order(farthest.order_id)
                                            self._invalidate_order_cache()
                                    except Exception as e_cancel:
                                        self.logger.log(f"[RISK] Release cancel TP failed: {e_cancel}", "WARNING")
                                    if self.enable_notifications:
                                        await self.send_notification(
                                            f"[RISK] Released {release_qty} after sustained stop-new (advanced)"
                                            if should_release_advanced
                                            else f"[RISK-BASIC] Released {release_qty} after sustained full position"
                                        )
                        except Exception as e:
                            self.logger.log(f"[RISK] Release attempt failed: {e}", "WARNING")
                if self.enable_basic_risk and self.max_position_limit is not None and position_amt < self.max_position_limit:
                    self.basic_full_since = None

                if self.webhook_sl:
                    await self._poll_webhook_direction(force=True)

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def _cancel_all_orders_safely(self):
        """Cancel all orders with best-effort handling."""
        if hasattr(self.exchange_client, "cancel_all_orders"):
            try:
                await self.exchange_client.cancel_all_orders()
                self._invalidate_order_cache()
                self._invalidate_order_info_cache()
                return
            except Exception as exc:
                self.logger.log(f"[WEBHOOK] cancel_all_orders failed: {exc}", "WARNING")
        try:
            active_orders = await self._get_active_orders_cached()
            for order in active_orders:
                try:
                    await self.exchange_client.cancel_order(order.order_id)
                except Exception as exc:
                    self.logger.log(f"[WEBHOOK] Cancel order {order.order_id} failed: {exc}", "WARNING")
            self._invalidate_order_cache()
        except Exception as exc:
            self.logger.log(f"[WEBHOOK] Fetch active orders for cancel failed: {exc}", "WARNING")

    async def _poll_webhook_direction(self, force: bool = False):
        """Poll basic webhook direction file and trigger stop/reverse when needed."""
        if not self.webhook_sl:
            return
        now = time.time()
        if (not force) and (now - self._last_webhook_poll) < self._webhook_poll_interval:
            return
        self._last_webhook_poll = now
        data = self._load_json_file(self.basic_direction_file)
        entry = None
        if isinstance(data, dict):
            entry = data.get(self.symbol_base)
            if entry is None:
                # Fallback: accept keys like ETHUSDT/ETHUSD when symbol_base is ETH
                for k, v in data.items():
                    if isinstance(k, str) and k.upper().startswith(self.symbol_base):
                        entry = v
                        break
        if not entry:
            return
        webhook_dir = str(entry.get("direction", "")).lower()
        if webhook_dir not in ("buy", "sell"):
            return
        # Ignore stale signals older than 20 minutes
        updated_at = entry.get("updated_at")
        if updated_at:
            try:
                ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00")).timestamp()
                if now - ts > 20 * 60:
                    return
            except Exception:
                pass
        self.webhook_latest_direction = webhook_dir
        if webhook_dir == self.config.direction and not self.webhook_stop_mode:
            if self.webhook_block_trading:
                self.webhook_block_trading = False
                self._set_stop_new_orders(False)
            return
        await self._handle_webhook_signal(webhook_dir)

    async def _handle_webhook_signal(self, webhook_dir: str):
        """Handle webhook stop/reverse directive."""
        if self.webhook_stop_mode and self.webhook_target_direction == webhook_dir:
            return
        self.logger.log(
            f"[WEBHOOK] Signal received: {webhook_dir.upper()} (mode={'FAST' if self.webhook_sl_fast else 'SLOW'})",
            "INFO",
        )
        self.webhook_block_trading = True
        self._set_stop_new_orders(True)
        self.webhook_target_direction = webhook_dir
        self.webhook_reverse_pending = self.webhook_reverse
        self.webhook_unwind_last_ts = 0.0
        self.webhook_stop_mode = "fast" if self.webhook_sl_fast else "slow"
        await self._cancel_all_orders_safely()
        if self.webhook_sl_fast and self.enable_notifications:
            await self.send_notification(f"[WEBHOOK] Fast stop toward {webhook_dir.upper()} signal")
        if self.webhook_sl_fast:
            await self._handle_webhook_stop_tasks()
        else:
            if self.enable_notifications:
                await self.send_notification(f"[WEBHOOK] Slow stop toward {webhook_dir.upper()} signal (stop new + cancel orders)")

    async def _handle_webhook_stop_tasks(self) -> bool:
        """Progress webhook stop/unwind; return True if webhook flow handled this tick."""
        if not self.webhook_stop_mode:
            return False
        try:
            pos_signed = await self._get_position_signed_cached()
        except Exception as exc:
            self.logger.log(f"[WEBHOOK] Failed to fetch position during webhook stop: {exc}", "WARNING")
            return True

        pos_abs = abs(pos_signed)
        if pos_abs > 0:
            close_side = "sell" if pos_signed > 0 else "buy"
            if self.webhook_stop_mode == "fast":
                try:
                    await self._close_position_in_chunks(close_side)
                except Exception as exc:
                    self.logger.log(f"[WEBHOOK] Fast stop close failed: {exc}", "ERROR")
                return True
            if time.time() - self.webhook_unwind_last_ts >= max(self.config.wait_time, 1):
                close_qty = min(self.config.quantity, pos_abs)
                try:
                    await self.exchange_client.reduce_only_close_with_retry(close_qty, close_side)
                except Exception as exc:
                    self.logger.log(f"[WEBHOOK] Slow stop close failed: {exc}", "WARNING")
                self.webhook_unwind_last_ts = time.time()
            return True

        # Position flat: finalize stop/reverse
        await self._cancel_all_orders_safely()
        if self.webhook_reverse_pending and self.webhook_target_direction:
            self.config.direction = self.webhook_target_direction
            self.current_direction = self.config.direction
            self.webhook_reverse_pending = False
            self.webhook_block_trading = False
            self.webhook_stop_mode = None
            self.webhook_target_direction = None
            self.dynamic_stop_price = None
            self.dynamic_stop_direction = None
            self._set_stop_new_orders(False)
            if self.enable_notifications:
                await self.send_notification(f"[WEBHOOK] Reversed to {self.config.direction.upper()} after webhook stop")
            await self._place_and_monitor_open_order()
        else:
            if self.enable_notifications and self.webhook_latest_direction:
                await self.send_notification(f"[WEBHOOK] Flattened after {self.webhook_latest_direction.upper()} signal")
            self.webhook_stop_mode = None
            self.webhook_target_direction = None
            # Keep stop_new_orders until direction aligns again
        return True

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = next_close_order["price"]

            best_bid, best_ask = await self._get_bbo_cached(force=True)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True

    async def _get_last_trade_price(self, best_bid: Optional[Decimal] = None, best_ask: Optional[Decimal] = None) -> Optional[Decimal]:
        """Fetch last traded price; fallback to mid price if unavailable."""
        if best_bid is not None and best_ask is not None:
            try:
                return (Decimal(best_bid) + Decimal(best_ask)) / 2
            except Exception:
                pass
        try:
            bid, ask = await self._get_bbo_cached()
            return (bid + ask) / 2
        except Exception as exc:
            self.logger.log(f"[PRICE] Fallback trade price failed: {exc}", "WARNING")
            return None

    async def _check_price_condition(self):
        stop_trading = False
        pause_trading = False
        stop_loss_triggered = False
        best_bid = None
        best_ask = None

        await self._sync_external_pivots()
        best_bid, best_ask = await self._get_bbo_cached(force=True)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        trade_price = await self._get_last_trade_price_cached(best_bid, best_ask, force=True)

        if self.enable_dynamic_sl and self.dynamic_stop_price is not None and (not self.stop_loss_triggered):
            if self.config.direction == "buy" and best_bid <= self.dynamic_stop_price:
                stop_loss_triggered = True
            elif self.config.direction == "sell" and best_ask >= self.dynamic_stop_price:
                stop_loss_triggered = True
        # Immediate reverse on break of confirmed high/low (trade price +/- buffer ticks)
        if self.enable_zigzag and self.enable_auto_reverse and trade_price is not None:
            buffer = self.break_buffer_ticks * self.config.tick_size
            if self.last_confirmed_high is not None and trade_price >= (self.last_confirmed_high + buffer):
                if self.config.direction == "sell":
                    await self._handle_reverse_signal("buy", Decimal(trade_price))
                    return stop_trading, pause_trading, stop_loss_triggered, best_bid, best_ask
            if self.last_confirmed_low is not None and trade_price <= (self.last_confirmed_low - buffer):
                if self.config.direction == "buy":
                    await self._handle_reverse_signal("sell", Decimal(trade_price))
                    return stop_trading, pause_trading, stop_loss_triggered, best_bid, best_ask

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading, stop_loss_triggered, best_bid, best_ask

    async def _execute_stop_loss(self, best_bid: Decimal, best_ask: Decimal):
        """Cancel open orders and close position when fixed stop-loss hits."""
        if self.stop_loss_triggered:
            return

        self.stop_loss_triggered = True
        msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}]\n"
        msg += "Stop-loss triggered. Cancelling open orders and closing position (continue running).\n"
        msg += "触发止损，正在撤单并平掉当前仓位，将继续运行。\n"
        await self.send_notification(msg.lstrip())

        try:
            active_orders = await self._get_active_orders_cached()
            for order in active_orders:
                try:
                    await self.exchange_client.cancel_order(order.order_id)
                except Exception as cancel_err:
                    self.logger.log(f"Failed to cancel order {order.order_id}: {cancel_err}", "WARNING")
            self._invalidate_order_cache()
            self._invalidate_order_info_cache()
        except Exception as e:
            self.logger.log(f"Error fetching active orders during stop-loss: {e}", "ERROR")

        try:
            position_size = abs(await self._get_position_signed_cached())
        except Exception as e:
            self.logger.log(f"Error fetching position during stop-loss: {e}", "ERROR")
            position_size = Decimal(0)

        if position_size > 0:
            close_side = self.config.close_order_side
            market_close_result = None

            if hasattr(self.exchange_client, "place_market_order"):
                try:
                    market_close_result = await self.exchange_client.place_market_order(
                        self.config.contract_id,
                        position_size,
                        close_side
                    )
                except Exception as market_err:
                    self.logger.log(f"Market stop-loss order failed: {market_err}", "ERROR")

            if (not market_close_result) or (not getattr(market_close_result, "success", False)):
                # Fallback: place an aggressive limit order toward best bid/ask
                fallback_price = best_bid if close_side == "sell" else best_ask
                try:
                    await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        position_size,
                        fallback_price,
                        close_side
                    )
                except Exception as fallback_err:
                    self.logger.log(f"Stop-loss fallback order failed: {fallback_err}", "ERROR")

            self._invalidate_position_cache()
            await asyncio.sleep(0.25)

        # Reset stop-loss state to allow continued trading
        self.current_sl_order_id = None
        self.dynamic_stop_price = None
        self.dynamic_stop_direction = None
        self._last_stop_eval_price = None
        self.stop_loss_triggered = False

    async def send_notification(self, message: str):
        if not self.enable_notifications:
            return
        if self.account_name:
            message = f"[{self.account_name}] {message}"
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as lark_bot:
                await lark_bot.send_text(message)

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if telegram_token and telegram_chat_id:
            with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)

    async def _notify_error_once(self, message: str, dedup_seconds: int = 300):
        """Send error notification with simple dedup to avoid spam."""
        now = time.time()
        if self.last_error_notified_msg == message and (now - self.last_error_notified_ts) < dedup_seconds:
            return
        self.last_error_notified_msg = message
        self.last_error_notified_ts = now
        await self.send_notification(message)

    async def _notify_direction(self, prefix: str = "[DIRECTION]"):
        """Notify current direction once (used at startup)."""
        if not self.enable_notifications:
            return
        await self.send_notification(f"{prefix} Current direction {self.config.direction.upper()}")

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            if self.enable_zigzag:
                await self._initialize_pivots_from_store()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            if self.enable_dynamic_sl:
                self.logger.log(f"Dynamic SL enabled", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            if self.enable_notifications:
                await self.send_notification(f"[START] {self.config.exchange.upper()} {self.config.ticker} bot started.")
                await self._notify_direction()
            # Main trading loop
            while not self.shutdown_requested:
                try:
                    await self._maybe_send_daily_pnl()
                    if self.webhook_sl:
                        await self._poll_webhook_direction()
                    # Webhook slow/fast stop handling takes priority
                    if self.webhook_sl and self.webhook_stop_mode:
                        handled_webhook = await self._handle_webhook_stop_tasks()
                        if handled_webhook:
                            await asyncio.sleep(0.5 if self.webhook_stop_mode == "fast" else max(self.config.wait_time, 1))
                            continue
                    # Handle pending slow reverse before normal flow
                    if self.pending_reverse_state == "unwinding":
                        await self._perform_slow_unwind()
                        await asyncio.sleep(max(self.config.wait_time, 1))
                        continue

                    # Update active orders
                    active_orders = await self._get_active_orders_cached()

                    # Filter close orders
                    self.active_close_orders = []
                    for order in active_orders:
                        if order.side == self.config.close_order_side:
                            self.active_close_orders.append({
                                'id': order.order_id,
                                'price': order.price,
                                'size': order.size
                            })

                    # Periodic logging
                    mismatch_detected = await self._log_status_periodically()

                    stop_trading, pause_trading, stop_loss_triggered, best_bid, best_ask = await self._check_price_condition()
                    if stop_loss_triggered:
                        await self._execute_stop_loss(best_bid, best_ask)
                        continue

                    if stop_trading:
                        msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                        msg += "Stopped trading due to stop price triggered\n"
                        msg += "价格已经达到停止交易价格，脚本将停止交易\n"
                        await self.send_notification(msg.lstrip())
                        await self.graceful_shutdown(msg)
                        continue

                    if pause_trading:
                        await asyncio.sleep(5)
                        continue

                    if not mismatch_detected:
                        wait_time = self._calculate_wait_time()

                        if wait_time > 0:
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            meet_grid_step_condition = await self._meet_grid_step_condition()
                            if not meet_grid_step_condition:
                                await asyncio.sleep(1)
                                continue

                            await self._place_and_monitor_open_order()
                            self.last_close_orders += 1

                    # 成功运行一轮，若之前有网络失败计数则发送恢复通知一次
                    if self.net_failure_count > 0:
                        await self._notify_error_once("[NET] 恢复：已重新连接交易所，恢复交易循环", dedup_seconds=0)
                        self.net_failure_count = 0
                except Exception as e:
                    err_msg = str(e)
                    self.logger.log(f"Critical error in main loop: {err_msg}", "ERROR")
                    # 判定是否网络/行情不可用错误
                    net_keywords = ["connection reset", "cannot connect", "timed out", "no bid/ask data", "ssl", "aiohttp"]
                    is_net = any(k in err_msg.lower() for k in net_keywords)
                    if is_net:
                        self.net_failure_count += 1
                        if self.net_failure_count % 5 == 0:
                            await self._notify_error_once(f"[NET] 重试中（次数 {self.net_failure_count}）：{err_msg}", dedup_seconds=0)
                        await asyncio.sleep(min(30, 5 * self.net_failure_count))
                        continue
                    else:
                        # 普通错误：去重后通知一次
                        await self._notify_error_once(f"出现报错：{err_msg}", dedup_seconds=300)
                        await asyncio.sleep(5)
                        continue

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
