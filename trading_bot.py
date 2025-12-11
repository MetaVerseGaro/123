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
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Tuple, Set, Any
from collections import deque, OrderedDict

from exchanges import ExchangeFactory
from exchanges.base import OrderResult
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
    trading_mode: str = "grid"
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
    break_buffer_ticks: Decimal = Decimal("10")
    use_risk_exposure: bool = True
    leverage: Decimal = Decimal("1")
    risk_reward: Decimal = Decimal("2")
    hft_pivot_file: Optional[str] = None

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

        # Mode: grid (default) | zigzag_timing
        self.trading_mode = str(getattr(config, "trading_mode", None) or os.getenv("TRADING_MODE", "grid")).lower()
        self.hft_mode = self.trading_mode == "hft"
        self.zigzag_timing_enabled = self.trading_mode in ("zigzag_timing", "hft")

        # Mode tags
        self.mode_tag = self.trading_mode or "grid"
        self.mode_prefix = f"[MODE:{self.mode_tag.upper()}]"

        # Cache helper
        debug_flag = ("--debug" in sys.argv) or (str(os.getenv("DEBUG_CACHE", "false")).lower() == "true")
        self.cache = _AsyncCache(self.logger, debug=debug_flag)
        # TTL settings (seconds)
        self.ttl_bbo = float(os.getenv("TTL_BBO_SEC", "0.3"))
        self.ttl_bbo_cache = float(os.getenv("BBO_CACHE_TTL_S", "0.5"))
        self.bbo_force_timeout = float(os.getenv("BBO_FORCE_TIMEOUT_MS", "150")) / 1000.0
        self.ttl_last_price = float(os.getenv("TTL_LAST_PRICE_SEC", "0.5"))
        self.ttl_active_orders = float(os.getenv("TTL_ACTIVE_ORDERS_SEC", "2"))
        self.ttl_active_orders_idle = float(os.getenv("TTL_ACTIVE_ORDERS_IDLE_SEC", "6"))
        self.ttl_position = float(os.getenv("TTL_POSITION_SEC", "1.5"))
        self.ttl_position_idle = float(os.getenv("TTL_POSITION_IDLE_SEC", "6"))
        # Balance TTL is short (3-5s) and does not reuse equity TTL
        self.ttl_balance = float(os.getenv("TTL_BALANCE_SEC", "4"))
        self.ttl_equity = float(os.getenv("TTL_EQUITY_SEC", "60"))
        self.ttl_order_info = float(os.getenv("TTL_ORDER_INFO_SEC", "1.5"))
        self.pivot_debounce_ms = int(os.getenv("PIVOT_DEBOUNCE_MS", "150"))
        self.zigzag_bbo_min_interval = float(os.getenv("ZIGZAG_BBO_MIN_INTERVAL_SEC", "1.0"))
        self._last_pivot_reload_ts: float = 0.0
        self._pivot_reload_in_progress: bool = False
        self._zigzag_last_bbo_ts: float = 0.0
        # Shared BBO cache (sidecar) support
        self.shared_bbo_file: Optional[Path] = None
        shared_bbo_path = os.getenv("SHARED_BBO_FILE", "").strip()
        if shared_bbo_path:
            self.shared_bbo_file = Path(shared_bbo_path).expanduser().resolve()
        self.shared_bbo_max_age = float(os.getenv("SHARED_BBO_MAX_AGE_SEC", "1.5"))
        # Sidecar BBO cache cleanup interval (keep file small on t3.micro)
        self.shared_bbo_cleanup_sec = float(os.getenv("SHARED_BBO_CLEANUP_SEC", "30"))
        self._shared_bbo_last_write_ts: float = 0.0
        if self.shared_bbo_file:
            self.logger.log(f"[INIT] Shared BBO file: {self.shared_bbo_file}", "INFO")
        else:
            self.logger.log("[INIT] Shared BBO file not set; using local WS/REST only", "INFO")

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
        self.current_direction: Optional[str] = self.config.direction.lower() if self.config.direction else None
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
        # Residual position handling (tiny < min_order_size)
        self._residual_last_dir: Optional[str] = None
        self._residual_last_qty: Optional[Decimal] = None
        self._residual_last_ts: float = 0.0
        self._residual_last_fail: bool = False
        # 动态 SL 无单独开关：开启止损即开启动态 SL
        self.enable_dynamic_sl = self.stop_loss_enabled
        self.enable_zigzag = str(os.getenv('ENABLE_ZIGZAG', 'true')).lower() == 'true'
        self.auto_reverse_fast = str(os.getenv('AUTO_REVERSE_FAST', 'true')).lower() == 'true'
        # ZigZag timing mode state
        self.direction_lock: Optional[str] = None
        self.pending_entry: Optional[str] = None
        self.pending_break_price: Optional[Decimal] = None
        self.pending_break_trigger_ts: Optional[float] = None
        self.pending_entry_static_mode: bool = False
        self._pending_entry_order_ids: List[str] = []
        self.zigzag_stop_price: Optional[Decimal] = None
        self.zigzag_entry_price: Optional[Decimal] = None
        self.zigzag_tp_order_id: Optional[str] = None
        self.zigzag_tp_qty: Optional[Decimal] = None
        self.enable_notifications = str(os.getenv('ENABLE_NOTIFICATIONS', 'false')).lower() == 'true'
        self.daily_pnl_report = str(os.getenv('DAILY_PNL_REPORT', 'false')).lower() == 'true'
        self.current_sl_order_id: Optional[Decimal] = None
        self.break_buffer_ticks = Decimal(self.config.break_buffer_ticks)
        self.zigzag_timeframe = os.getenv('ZIGZAG_TIMEFRAME', '1m')
        self.zigzag_timeframe_sec = self._parse_timeframe_to_seconds(self.zigzag_timeframe)
        self.zigzag_timeframe_key = self._normalize_timeframe_key(self.zigzag_timeframe)
        self.symbol_base = self._extract_symbol_base(self.config.ticker)
        self.timing_prefix = "[HFT]" if self.hft_mode else "[ZIGZAG-TIMING]"
        self.base_dir = Path(__file__).resolve().parent
        pivot_path_cfg = getattr(config, "zigzag_pivot_file", None)
        hft_pivot_cfg = getattr(config, "hft_pivot_file", None)
        dir_path_cfg = getattr(config, "webhook_basic_direction_file", None)
        default_pivot_env = "HFT_PIVOT_FILE" if self.hft_mode else "ZIGZAG_PIVOT_FILE"
        default_pivot_name = "hft_pivots.json" if self.hft_mode else "zigzag_pivots.json"
        pivot_override = hft_pivot_cfg if self.hft_mode else pivot_path_cfg
        self.zigzag_pivot_file = self._resolve_path(pivot_override or os.getenv(default_pivot_env, default_pivot_name))
        self.basic_direction_file = self._resolve_path(dir_path_cfg or os.getenv('WEBHOOK_BASIC_DIRECTION_FILE', 'webhook_basic_direction.json'))
        self.recent_pivots: deque[PivotPoint] = deque(maxlen=12)
        self._processed_pivot_keys: Set[Tuple[str, str]] = set()
        # Cache built pivot points to avoid repeated OHLC fetches for the same pivot
        self._pivot_point_cache: Dict[Tuple[str, str], PivotPoint] = {}
        self._last_pivot_poll: float = 0.0
        self._min_qty_block_last_ts: float = 0.0
        # Track last quantity calc reason to improve logging on invalid qty
        self._last_qty_reason: Optional[str] = None
        # Throttled log tracker to avoid repeated identical logs
        self._log_throttle: Dict[str, Tuple[str, float]] = {}
        self._pending_log_cache: Dict[str, str] = {}
        self._blocked_direction: Optional[str] = None
        self._hft_block_dir: Optional[str] = None
        self._hft_block_until: float = 0.0
        # Throttled log tracker to avoid repeated identical logs
        self._log_throttle: Dict[str, Tuple[str, float]] = {}
        default_interval = float(self.zigzag_timeframe_sec or 60)
        self._pivot_poll_interval: float = float(os.getenv("PIVOT_POLL_INTERVAL_SEC", str(int(default_interval))))
        self._last_pivot_change_ts: float = time.time()
        self.pending_entry_state: Optional[Dict[str, Any]] = None
        self._webhook_poll_interval: float = float(os.getenv("WEBHOOK_POLL_INTERVAL_SEC", "5"))
        self._last_webhook_poll: float = 0.0
        # REST throttle lanes to comply with lighter_rate_limits_official.md
        self._rest_last_call: Dict[str, float] = {}
        self.webhook_sl = bool(getattr(config, "webhook_sl", False) or str(os.getenv("WEBHOOK_SL", "false")).lower() == "true")
        self.webhook_sl_fast = bool(getattr(config, "webhook_sl_fast", False) or str(os.getenv("WEBHOOK_SL_FAST", "false")).lower() == "true")
        self.webhook_reverse = bool(getattr(config, "webhook_reverse", False) or str(os.getenv("WEBHOOK_REVERSE", "false")).lower() == "true")
        self.webhook_latest_direction: Optional[str] = None
        self.webhook_stop_mode: Optional[str] = None  # None | fast | slow
        self.webhook_target_direction: Optional[str] = None
        self.webhook_reverse_pending: bool = False
        self.webhook_block_trading: bool = False
        self.webhook_unwind_last_ts: float = 0.0
        # Break/flatten retry throttles
        self._last_flatten_attempt_ts: float = 0.0
        self._last_entry_attempt_ts: float = 0.0
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
        cfg_use_risk = getattr(config, "use_risk_exposure", None)
        env_use_risk = os.getenv("USE_RISK_EXPOSURE", None)
        self.use_risk_exposure = bool(str(cfg_use_risk if cfg_use_risk is not None else (env_use_risk if env_use_risk is not None else "true")).lower() == "true")
        try:
            cfg_leverage = getattr(config, "leverage", None)
            env_leverage = os.getenv("LEVERAGE_MULTIPLIER", os.getenv("LEVERAGE", "1"))
            val = cfg_leverage if cfg_leverage not in (None, "") else env_leverage
            self.leverage = Decimal(str(val)) if val not in (None, "") else Decimal("1")
        except Exception:
            self.leverage = Decimal("1")
        try:
            cfg_rr = getattr(config, "risk_reward", None)
            env_rr = os.getenv("RISK_REWARD", None)
            val = cfg_rr if cfg_rr not in (None, "") else env_rr
            self.risk_reward = Decimal(str(val)) if val not in (None, "") else Decimal("2")
        except Exception:
            self.risk_reward = Decimal("2")
        self.release_timeout_minutes = int(os.getenv("RELEASE_TIMEOUT_MINUTES", "10"))
        self.basic_release_timeout_minutes = getattr(config, "basic_release_timeout_minutes", 0) or 0
        self.stop_new_orders_equity_threshold = Decimal(os.getenv("STOP_NEW_ORDERS_EQUITY_THRESHOLD", "50"))
        self.stop_new_orders = False
        self.stop_new_orders_reason: Optional[str] = None
        self.stop_new_orders_since: Optional[float] = None
        self.stop_new_since: Optional[float] = None
        self.basic_full_since: Optional[float] = None
        self.redundancy_insufficient_since: Optional[float] = None
        self.last_new_order_time = time.time()
        self.last_release_attempt = 0  # legacy; kept for backward compatibility
        self.last_release_attempt_advanced = 0.0
        self.last_release_attempt_basic = 0.0
        self.last_stop_new_notify = False
        self.pending_reverse_direction: Optional[str] = None
        self.pending_reverse_state: Optional[str] = None  # None | waiting_next_pivot | unwinding
        self.pending_original_direction: Optional[str] = None
        self.last_daily_pnl_date: Optional[str] = None
        self.daily_pnl_baseline: Optional[Decimal] = None
        self.last_daily_sent_date: Optional[str] = None
        self.dynamic_stop_can_widen: bool = False
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
        # ZigZag timing mode强制开启相关能力（不使用原网格反手逻辑）
        if self.zigzag_timing_enabled:
            self.stop_loss_enabled = True
            self.enable_advanced_risk = True
            self.enable_zigzag = True
            self.enable_dynamic_sl = True
            self.enable_auto_reverse = False
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

    def _cache_key(self, name: str) -> str:
        cid = self.config.contract_id or self.config.ticker
        return f"{name}:{cid}:{self.mode_tag}"

    def _invalidate_order_cache(self):
        self.cache.invalidate(self._cache_key("orders"))

    def _invalidate_position_cache(self):
        self.cache.invalidate(self._cache_key("position"))

    def _order_info_key(self, order_id: str) -> str:
        return f"order_info:{order_id}:{self.mode_tag}"

    def _invalidate_order_info_cache(self, order_id: Optional[str] = None):
        if order_id:
            self.cache.invalidate(self._order_info_key(order_id))
        else:
            # Broad invalidation (used after bulk cancel)
            suffix = f":{self.mode_tag}"
            keys = [k for k in self.cache.keys() if k.startswith("order_info:") and k.endswith(suffix)]
            if keys:
                self.cache.invalidate(*keys)

    def _set_direction_all(self, direction: Optional[str], lock: bool = True, mode: Optional[str] = None) -> bool:
        """
        Set config/current/lock consistently.
        Returns True if any value changed.
        """
        mode = mode or self.mode_tag
        if mode != self.mode_tag:
            if self.cache.debug:
                self.logger.log(f"{self.mode_prefix} skip set_direction_all for mode {mode}", "DEBUG")
            return False
        dir_val = direction.lower() if isinstance(direction, str) else None
        changed = False
        if self.config.direction != dir_val:
            self.config.direction = dir_val
            changed = True
        if self.current_direction != dir_val:
            self.current_direction = dir_val
            changed = True
        if lock and self.direction_lock != dir_val:
            self.direction_lock = dir_val
            changed = True
        return changed

    def _rest_lane(self, lane: str) -> str:
        return f"{self.mode_tag}:{lane}"

    def _price_breaches_stop(self, stop_price: Optional[Decimal], direction: str, price: Decimal) -> bool:
        """Return True if an order price is beyond the allowed stop boundary."""
        if stop_price is None:
            return False
        dir_l = str(direction).lower()
        if dir_l == "buy":
            return price <= stop_price
        if dir_l == "sell":
            return price >= stop_price
        return False

    def _round_quantity(self, qty: Decimal) -> Decimal:
        """Normalize quantity to a reasonable precision."""
        try:
            return Decimal(qty).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        except Exception:
            return Decimal(qty)

    async def _cancel_order_ids(self, order_ids: List[str]):
        """Best-effort cancel for a list of order ids."""
        if not order_ids:
            return
        for oid in order_ids:
            try:
                await self.exchange_client.cancel_order(str(oid))
            except Exception as exc:
                self.logger.log(f"{self.timing_prefix} Cancel order {oid} failed: {exc}", "WARNING")

    def _shared_bbo_key(self) -> str:
        """Build the shared BBO key for the current contract."""
        cid = self.config.contract_id
        contract_key = str(cid) if cid not in (None, "") else self.config.ticker
        return f"{self.config.exchange}:{contract_key}"

    def _save_shared_bbo_data(self, data: Dict[str, Any]):
        """Persist shared BBO data atomically, cleaning up on errors."""
        if not self.shared_bbo_file:
            return
        try:
            self.shared_bbo_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.shared_bbo_file.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            tmp.replace(self.shared_bbo_file)
        except Exception as exc:
            if self.cache.debug:
                self.logger.log(f"[SHARED-BBO] save failed: {exc}", "WARNING")

    def _read_shared_bbo(self) -> Optional[Tuple[Decimal, Decimal]]:
        """Read BBO from shared sidecar file if fresh; prune stale entries."""
        if not self.shared_bbo_file:
            if self.cache.debug:
                self.logger.log("[SHARED-BBO] skipped: SHARED_BBO_FILE not set", "DEBUG")
            return None
        if not self.shared_bbo_file.exists():
            if self.cache.debug:
                self.logger.log(f"[SHARED-BBO] skipped: {self.shared_bbo_file} not found", "DEBUG")
            return None
        now_ts = time.time()
        try:
            with open(self.shared_bbo_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            if self.cache.debug:
                self.logger.log(f"[SHARED-BBO] read failed: {exc}", "WARNING")
            return None

        if not isinstance(payload, dict):
            return None

        changed = False
        cutoff = now_ts - self.shared_bbo_cleanup_sec
        keys_to_drop = []
        for k, v in payload.items():
            ts_val = 0.0
            try:
                ts_val = float(v.get("ts", 0)) if isinstance(v, dict) else 0.0
            except Exception:
                ts_val = 0.0
            too_old = (ts_val <= 0) or (ts_val < cutoff)
            # Extra guard: if a writer keeps a stale value alive forever, still drop it after a long grace window
            if not too_old and ts_val > 0:
                try:
                    too_old = (now_ts - ts_val) > max(self.shared_bbo_cleanup_sec, self.shared_bbo_max_age * 6)
                except Exception:
                    too_old = False
            if too_old:
                keys_to_drop.append(k)
        for k in keys_to_drop:
            payload.pop(k, None)
            changed = True

        key = self._shared_bbo_key()
        entry = payload.get(key)
        result: Optional[Tuple[Decimal, Decimal]] = None
        if isinstance(entry, dict):
            try:
                ts_val = float(entry.get("ts", 0) or 0.0)
                if (now_ts - ts_val) <= self.shared_bbo_max_age:
                    bid = Decimal(str(entry.get("bid")))
                    ask = Decimal(str(entry.get("ask")))
                    if bid > 0 and ask > 0 and bid < ask:
                        result = (bid, ask)
            except Exception:
                result = None
        else:
            if self.cache.debug:
                self.logger.log(f"[SHARED-BBO] miss: key {key} not in cache file", "DEBUG")

        if changed:
            self._save_shared_bbo_data(payload)
        if self.cache.debug and result is None:
            self.logger.log("[SHARED-BBO] miss or stale, will use WS/REST", "DEBUG")
        if self.cache.debug and result is not None:
            self.logger.log(f"[SHARED-BBO] hit @ ts={ts_val}", "DEBUG")
        return result

    def _write_shared_bbo(self, bid: Decimal, ask: Decimal):
        """Persist BBO to shared sidecar file with simple rate limit and cleanup."""
        if not self.shared_bbo_file:
            return
        now_ts = time.time()
        # Simple throttle to avoid excessive disk writes
        if (now_ts - self._shared_bbo_last_write_ts) < max(0.1, self.shared_bbo_max_age / 2):
            return
        self._shared_bbo_last_write_ts = now_ts
        payload: Dict[str, Any] = {}
        try:
            if self.shared_bbo_file.exists():
                with open(self.shared_bbo_file, "r", encoding="utf-8") as f:
                    payload = json.load(f) or {}
        except Exception:
            payload = {}

        if not isinstance(payload, dict):
            payload = {}

        cutoff = now_ts - self.shared_bbo_cleanup_sec
        keys_to_drop = []
        for k, v in payload.items():
            ts_val = 0.0
            try:
                ts_val = float(v.get("ts", 0)) if isinstance(v, dict) else 0.0
            except Exception:
                ts_val = 0.0
            if ts_val <= 0 or ts_val < cutoff:
                keys_to_drop.append(k)
        for k in keys_to_drop:
            payload.pop(k, None)

        # Avoid clobbering fresh data from another process when nothing changed
        existing = payload.get(self._shared_bbo_key())
        if isinstance(existing, dict):
            try:
                exist_ts = float(existing.get("ts", 0) or 0)
                exist_bid = Decimal(str(existing.get("bid")))
                exist_ask = Decimal(str(existing.get("ask")))
                if (
                    exist_bid == bid
                    and exist_ask == ask
                    and (now_ts - exist_ts) < (self.shared_bbo_max_age / 2)
                ):
                    return
            except Exception:
                pass

        payload[self._shared_bbo_key()] = {
            "bid": str(bid),
            "ask": str(ask),
            "ts": now_ts,
        }
        self._save_shared_bbo_data(payload)

    async def _fetch_ws_bbo(self) -> Optional[Tuple[Decimal, Decimal]]:
        """
        Try to fetch BBO from WS-backed helper if exchange client provides one.
        Expected method names: get_ws_bbo / get_cached_bbo / get_orderbook_bbo.
        """
        # Direct read from ws_manager best_bid/best_ask if present
        ws_mgr = getattr(self.exchange_client, "ws_manager", None)
        if ws_mgr:
            try:
                bid_val = getattr(ws_mgr, "best_bid", None)
                ask_val = getattr(ws_mgr, "best_ask", None)
                if bid_val is not None and ask_val is not None:
                    bid_d = Decimal(str(bid_val))
                    ask_d = Decimal(str(ask_val))
                    if bid_d > 0 and ask_d > 0 and bid_d < ask_d:
                        return bid_d, ask_d
            except Exception:
                pass

        candidates = ["get_ws_bbo", "get_cached_bbo", "get_orderbook_bbo"]
        for name in candidates:
            fn = getattr(self.exchange_client, name, None)
            if not callable(fn):
                continue
            try:
                res = fn(self.config.contract_id)
                if asyncio.iscoroutine(res):
                    res = await res
            except TypeError:
                try:
                    res = fn()
                    if asyncio.iscoroutine(res):
                        res = await res
                except Exception as e:
                    self.logger.log(f"[WS] {name} call failed: {e}", "WARNING")
                    continue
            except Exception as e:
                self.logger.log(f"[WS] {name} call failed: {e}", "WARNING")
                continue
            if not res or (isinstance(res, (list, tuple)) and len(res) < 2):
                continue
            try:
                bid, ask = res if isinstance(res, (list, tuple)) else (res[0], res[1])  # type: ignore
                bid_d = Decimal(str(bid))
                ask_d = Decimal(str(ask))
                if bid_d > 0 and ask_d > 0:
                    return bid_d, ask_d
            except Exception:
                continue
        return None

    async def _get_bbo_cached(self, force: bool = False) -> Tuple[Decimal, Decimal]:
        key = self._cache_key("bbo")
        async def fetch_bbo():
            return await self.exchange_client.fetch_bbo_prices(self.config.contract_id)

        # Try shared sidecar first to reuse BBO across processes (keeps REST load lower than double-pulling)
        shared = self._read_shared_bbo()
        if shared:
            return shared

        # Prefer WS snapshot if available
        ws_bbo = await self._fetch_ws_bbo()
        if ws_bbo:
            self._write_shared_bbo(ws_bbo[0], ws_bbo[1])
            return ws_bbo

        if force:
            try:
                val = await asyncio.wait_for(fetch_bbo(), timeout=self.bbo_force_timeout)
                self._write_shared_bbo(val[0], val[1])
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
        prices = await self.cache.get(
            key,
            self.ttl_bbo,
            fetch_bbo,
            force=False
        )
        self._write_shared_bbo(prices[0], prices[1])
        return prices

    async def _get_bbo_for_zigzag(self, force: bool = False) -> Tuple[Decimal, Decimal]:
        """
        Throttled BBO for zigzag mode: honor a min interval when idle; force refresh when in/pending position.
        """
        now = time.time()
        key = self._cache_key("bbo")
        if (not force) and (now - self._zigzag_last_bbo_ts < self.zigzag_bbo_min_interval):
            cached = self.cache.peek(key, self.ttl_bbo_cache)
            if cached:
                return cached
        prices = await self._get_bbo_cached(force=force)
        self._zigzag_last_bbo_ts = time.time()
        return prices

    async def _get_ws_last_price(self) -> Optional[Decimal]:
        """
        Try to fetch last price from WS-backed helpers before REST.
        Expected method names: get_ws_last_price / get_cached_last_price / get_ws_ticker.
        """
        candidates = ["get_ws_last_price", "get_cached_last_price", "get_ws_ticker"]

        def _normalize_price(val: Any) -> Optional[Decimal]:
            if val is None:
                return None
            if isinstance(val, dict):
                for k in ("last", "last_price", "price", "mark_price", "index_price", "close"):
                    if k in val:
                        val = val[k]
                        break
            if isinstance(val, (list, tuple)) and val:
                val = val[0]
            try:
                return Decimal(str(val))
            except Exception:
                return None

        for name in candidates:
            fn = getattr(self.exchange_client, name, None)
            if not callable(fn):
                continue
            try:
                res = fn(self.config.contract_id)
                if asyncio.iscoroutine(res):
                    res = await res
            except TypeError:
                try:
                    res = fn()
                    if asyncio.iscoroutine(res):
                        res = await res
                except Exception as e:
                    self.logger.log(f"[WS] {name} call failed: {e}", "WARNING")
                    continue
            except Exception as e:
                self.logger.log(f"[WS] {name} call failed: {e}", "WARNING")
                continue
            price = _normalize_price(res)
            if price is not None and price > 0:
                return price
        # Fallback: derive mid from ws_manager best bid/ask if available
        ws_mgr = getattr(self.exchange_client, "ws_manager", None)
        if ws_mgr:
            try:
                bid_val = getattr(ws_mgr, "best_bid", None)
                ask_val = getattr(ws_mgr, "best_ask", None)
                if bid_val is not None and ask_val is not None:
                    bid_d = Decimal(str(bid_val))
                    ask_d = Decimal(str(ask_val))
                    if bid_d > 0 and ask_d > 0 and bid_d < ask_d:
                        return (bid_d + ask_d) / 2
            except Exception:
                pass
        return None

    async def _get_last_trade_price_cached(self, best_bid: Optional[Decimal] = None, best_ask: Optional[Decimal] = None, force: bool = False) -> Optional[Decimal]:
        key = self._cache_key("last_price")

        async def _fetch():
            ws_last = await self._get_ws_last_price()
            if ws_last is not None:
                return ws_last
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
        key = self._cache_key("orders")
        ttl = self.ttl_active_orders
        if self.zigzag_timing_enabled:
            needs_fast = bool(
                self.pending_entry
                or self.direction_lock
                or self.redundancy_insufficient_since
                or self.pending_reverse_state
                or self.pending_reverse_direction
                or self.webhook_stop_mode
                or self.stop_new_orders is False  # preparing for opens
            )
            if not needs_fast:
                ttl = max(self.ttl_active_orders_idle, self.ttl_active_orders)
        async def _fetch_active_orders():
            try:
                val = await self.exchange_client.get_active_orders(self.config.contract_id)
            except Exception as exc:
                self.logger.log(f"[ORDERS] get_active_orders failed: {exc}", "WARNING")
                return []
            if val is None:
                return []
            try:
                return list(val)
            except Exception:
                return []
        return await self.cache.get(
            key,
            ttl,
            _fetch_active_orders
        )

    async def _get_directional_position(self, direction: str, force: bool = False) -> Decimal:
        """Return absolute position in the given direction."""
        pos = await self._get_position_signed_cached(force=force)
        if direction == "buy":
            return max(Decimal(0), pos)
        return max(Decimal(0), -pos)

    async def _cancel_orders_by_side(self, side: str):
        """Cancel all active orders of a given side."""
        try:
            active_orders = await self._get_active_orders_cached()
            if not active_orders:
                return
            targets = [o for o in active_orders if getattr(o, "side", "").lower() == side.lower()]
            for order in targets:
                try:
                    await self.exchange_client.cancel_order(order.order_id)
                except Exception as exc:
                    self.logger.log(f"{self.timing_prefix} Cancel {side} order {order.order_id} failed: {exc}", "WARNING")
            if targets:
                self._invalidate_order_cache()
        except Exception as exc:
            self.logger.log(f"{self.timing_prefix} Cancel by side failed: {exc}", "WARNING")

    async def _place_post_only_limit(self, side: str, quantity: Decimal, price: Decimal, reduce_only: bool = False) -> OrderResult:
        """Place a post-only limit order with best-effort fallbacks."""
        quantity = self._round_quantity(quantity)
        if self.min_order_size and quantity < self.min_order_size:
            quantity = self.min_order_size
        if quantity <= 0:
            return OrderResult(success=False, error_message="Invalid quantity")
        try:
            price = self.exchange_client.round_to_tick(price)
        except Exception:
            price = Decimal(price)

        client = self.exchange_client
        # Prefer explicit post-only if available
        if hasattr(client, "place_post_only_order"):
            try:
                return await client.place_post_only_order(self.config.contract_id, quantity, price, side, reduce_only=reduce_only)
            except TypeError:
                try:
                    return await client.place_post_only_order(self.config.contract_id, quantity, price, side)
                except Exception as exc:
                    self.logger.log(f"{self.timing_prefix} place_post_only_order fallback err: {exc}", "WARNING")
            except Exception as exc:
                self.logger.log(f"{self.timing_prefix} place_post_only_order err: {exc}", "WARNING")

        if hasattr(client, "place_limit_order"):
            try:
                return await client.place_limit_order(self.config.contract_id, quantity, price, side, reduce_only=reduce_only)
            except TypeError:
                try:
                    return await client.place_limit_order(self.config.contract_id, quantity, price, side)
                except Exception as exc:
                    self.logger.log(f"{self.timing_prefix} place_limit_order fallback err: {exc}", "WARNING")
            except Exception as exc:
                self.logger.log(f"{self.timing_prefix} place_limit_order err: {exc}", "WARNING")

        # Fallback: use open/close API (may not be post-only)
        try:
            if reduce_only:
                if hasattr(client, "place_close_order"):
                    return await client.place_close_order(self.config.contract_id, quantity, price, side)
            else:
                if hasattr(client, "place_open_order"):
                    return await client.place_open_order(self.config.contract_id, quantity, side)
        except Exception as exc:
            return OrderResult(success=False, error_message=str(exc))

        return OrderResult(success=False, error_message="No supported post-only/limit order method")

    async def _calc_qty_by_risk_zigzag(self, entry_price: Decimal, stop_price: Decimal) -> Optional[Decimal]:
        """Calculate target quantity based on risk_pct and stop distance."""
        self._last_qty_reason = None
        try:
            entry_price = Decimal(entry_price)
            stop_price = Decimal(stop_price)
        except Exception:
            self._last_qty_reason = "invalid entry/stop price"
            return None
        unit_risk = abs(entry_price - stop_price)
        if unit_risk <= 0:
            self._last_qty_reason = "unit_risk <= 0"
            return None
        equity = await self._get_equity_snapshot()
        if equity is None or equity <= 0:
            self._last_qty_reason = "equity unavailable"
            return None
        allowed_risk = equity * (self.risk_pct / Decimal(100))
        if allowed_risk <= 0:
            self._last_qty_reason = "allowed_risk <= 0"
            return None
        qty = allowed_risk / unit_risk
        if self.min_order_size:
            if qty < self.min_order_size:
                self._last_qty_reason = f"calc qty {qty} < min_order_size {self.min_order_size}"
                await self._notify_min_qty_block(qty, entry_price=entry_price, stop_price=stop_price)
                return None
        return self._round_quantity(qty)

    async def _calc_qty_by_leverage(self, entry_price: Decimal) -> Optional[Decimal]:
        """Calculate target quantity using equity * leverage / entry_price (no stop-distance dependency)."""
        self._last_qty_reason = None
        try:
            entry_price = Decimal(entry_price)
        except Exception:
            self._last_qty_reason = "invalid entry price"
            return None
        if entry_price <= 0:
            self._last_qty_reason = "entry_price <= 0"
            return None
        equity = await self._get_equity_snapshot()
        if equity is None or equity <= 0:
            self._last_qty_reason = "equity unavailable"
            return None
        notional = equity * self.leverage
        qty = notional / entry_price
        if self.min_order_size:
            if qty < self.min_order_size:
                self._last_qty_reason = f"calc qty {qty} < min_order_size {self.min_order_size}"
                await self._notify_min_qty_block(qty, entry_price=entry_price, stop_price=None)
                return None
        return self._round_quantity(qty)

    def _calc_tp_price_rr2(self, entry_price: Decimal, stop_price: Decimal, direction: str, rr: Optional[Decimal] = None) -> Optional[Decimal]:
        """Calculate TP price at configurable RR from entry/stop (defaults to 2)."""
        try:
            entry_price = Decimal(entry_price)
            stop_price = Decimal(stop_price)
            rr_val = Decimal(rr) if rr is not None else Decimal("2")
        except Exception:
            return None
        if rr_val <= 0:
            return None
        if direction == "buy":
            risk = entry_price - stop_price
            if risk <= 0:
                return None
            tp_price = entry_price + risk * rr_val
        else:
            risk = stop_price - entry_price
            if risk <= 0:
                return None
            tp_price = entry_price - risk * rr_val
        try:
            return self.exchange_client.round_to_tick(tp_price)
        except Exception:
            return tp_price

    async def _throttle_rest(self, lane: str, min_interval: float = 1.0):
        """
        Best-effort REST throttling per lane (non-blocking for WS paths).
        Keeps LF endpoints (e.g., historical candles) below ~60 req/min default cap.
        """
        now_ts = time.time()
        lane_key = self._rest_lane(lane)
        last_ts = self._rest_last_call.get(lane_key, 0.0)
        wait = min_interval - (now_ts - last_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        self._rest_last_call[lane_key] = time.time()

    async def _notify_zigzag_trade(self, action: str, direction: str, qty: Decimal, price: Decimal, note: str = ""):
        """Notify entry/exit in zigzag_timing with size and notional exposure."""
        if not self.enable_notifications:
            return
        try:
            qty_val = Decimal(qty)
            price_val = Decimal(price)
            exposure = qty_val * price_val
        except Exception:
            qty_val = qty
            price_val = price
            exposure = None
        msg_parts = [
            self.timing_prefix,
            action.upper(),
            direction.upper(),
            f"qty={qty_val}",
            f"px={price_val}",
        ]
        if exposure is not None:
            msg_parts.append(f"notional={exposure}")
        if note:
            msg_parts.append(note)
        await self.send_notification(" ".join(str(x) for x in msg_parts))

    async def _cancel_zigzag_tp(self):
        """Cancel existing fixed TP order if tracked."""
        if not self.zigzag_tp_order_id:
            return
        try:
            await self.exchange_client.cancel_order(self.zigzag_tp_order_id)
            self._invalidate_order_cache()
        except Exception as exc:
            self.logger.log(f"{self.timing_prefix} Cancel TP failed: {exc}", "WARNING")
        finally:
            self.zigzag_tp_order_id = None
            self.zigzag_tp_qty = None

    async def _place_zigzag_tp(self, direction: str, filled_qty: Decimal, entry_price: Decimal, stop_price: Decimal):
        """Place fixed TP at RR 1:2 for 50% position, reduce-only post-only."""
        await self._cancel_zigzag_tp()
        if self.risk_reward <= 0:
            self.logger.log(f"{self.timing_prefix} Skip TP: risk_reward {self.risk_reward} <= 0 (entry={entry_price}, stop={stop_price})", "INFO")
            return
        tp_price = self._calc_tp_price_rr2(entry_price, stop_price, direction, rr=self.risk_reward)
        if tp_price is None:
            self.logger.log(f"{self.timing_prefix} Skip TP: tp_price None (entry={entry_price}, stop={stop_price}, dir={direction})", "INFO")
            return
        qty = self._round_quantity(filled_qty / Decimal(2))
        if self.min_order_size and qty < self.min_order_size:
            self.logger.log(
                f"{self.timing_prefix} Skip TP: qty {qty} < min_order_size {self.min_order_size} (filled={filled_qty})",
                "INFO",
            )
            return
        side = "sell" if direction == "buy" else "buy"
        res = await self._place_post_only_limit(side, qty, tp_price, reduce_only=True)
        if res and res.success:
            self.zigzag_tp_order_id = res.order_id
            self.zigzag_tp_qty = qty
            self._invalidate_order_cache()
            self.logger.log(f"{self.timing_prefix} TP placed {side.upper()} qty={qty} @ {tp_price} reduce-only", "INFO")
        else:
            self.logger.log(f"{self.timing_prefix} Place TP failed: {getattr(res, 'error_message', '')}", "WARNING")

    def _build_price_ladder(self, side: str, base_price: Decimal, max_orders: int) -> List[Decimal]:
        """Build a simple 1-tick spaced price ladder."""
        prices: List[Decimal] = []
        tick = self.config.tick_size
        for i in range(max_orders):
            if side == "buy":
                px = base_price - tick * i
                if px <= 0:
                    px = tick
            else:
                px = base_price + tick * i
            try:
                px = self.exchange_client.round_to_tick(px)
            except Exception:
                pass
            prices.append(px)
        return prices

    async def _post_only_entry_batch(self, direction: str, target_qty: Decimal, best_bid: Decimal, best_ask: Decimal, wait_sec: float = 3.0) -> Tuple[Decimal, Decimal]:
        """Post-only batch entry; returns filled_qty, final_dir_qty."""
        if target_qty <= 0:
            return Decimal(0), await self._get_directional_position(direction, force=True)
        base_price = best_bid if direction == "buy" else best_ask
        if base_price is None or base_price <= 0:
            return Decimal(0), await self._get_directional_position(direction, force=True)
        max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else target_qty
        num_orders = int((target_qty / max_per).to_integral_value(rounding=ROUND_HALF_UP))
        if num_orders * max_per < target_qty:
            num_orders += 1
        prices = self._build_price_ladder(direction, base_price, max(num_orders, 1))
        start_qty = await self._get_directional_position(direction, force=True)
        remaining = target_qty
        order_ids: List[str] = []
        for px in prices:
            if remaining <= 0:
                break
            qty = min(max_per, remaining)
            res = await self._place_post_only_limit(direction, qty, px, reduce_only=False)
            if res and res.success:
                order_ids.append(res.order_id)
                remaining -= qty
        start_ts = time.time()
        while time.time() - start_ts < wait_sec:
            await asyncio.sleep(1)
        await self._cancel_order_ids(order_ids)
        await asyncio.sleep(0.1)
        end_qty = await self._get_directional_position(direction, force=True)
        filled = max(Decimal(0), end_qty - start_qty)
        return filled, end_qty

    async def _post_only_exit_batch(self, close_side: str, close_qty: Decimal, best_bid: Decimal, best_ask: Decimal, wait_sec: float = 3.0) -> Tuple[Decimal, Decimal]:
        """Post-only batch exit; returns closed_qty, remaining_abs."""
        if close_qty <= 0:
            pos_abs = abs(await self._get_position_signed_cached(force=True))
            return Decimal(0), pos_abs
        base_price = best_ask if close_side.lower() == "sell" else best_bid
        if base_price is None or base_price <= 0:
            pos_abs = abs(await self._get_position_signed_cached(force=True))
            return Decimal(0), pos_abs
        max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else close_qty
        num_orders = int((close_qty / max_per).to_integral_value(rounding=ROUND_HALF_UP))
        if num_orders * max_per < close_qty:
            num_orders += 1
        prices = self._build_price_ladder(close_side, base_price, max(num_orders, 1))
        start_abs = abs(await self._get_position_signed_cached(force=True))
        remaining = close_qty
        order_ids: List[str] = []
        for px in prices:
            if remaining <= 0:
                break
            qty = min(max_per, remaining)
            res = await self._place_post_only_limit(close_side, qty, px, reduce_only=True)
            if res and res.success:
                order_ids.append(res.order_id)
                remaining -= qty
        start_ts = time.time()
        while time.time() - start_ts < wait_sec:
            await asyncio.sleep(0.5)
        await self._cancel_order_ids(order_ids)
        await asyncio.sleep(0.1)
        end_abs = abs(await self._get_position_signed_cached(force=True))
        closed = max(Decimal(0), start_abs - end_abs)
        return closed, end_abs

    async def _flatten_opposite(self, new_direction: str, best_bid: Decimal, best_ask: Decimal) -> bool:
        """Ensure opposite side positions/orders are cleared before entering new direction."""
        # Cancel orders that would increase exposure against the new direction
        opposite_side = "sell" if new_direction == "buy" else "buy"
        await self._cancel_orders_by_side(opposite_side)

        pos_signed = await self._get_position_signed_cached(force=True)
        if (pos_signed > 0 and new_direction == "buy") or (pos_signed < 0 and new_direction == "sell"):
            self._last_flatten_attempt_ts = 0.0
            return True  # already aligned

        pos_abs = abs(pos_signed)
        if pos_abs <= (self.min_order_size or Decimal("0")):
            self._last_flatten_attempt_ts = 0.0
            return True

        # Use the correct close side based on current position sign to avoid opening on the wrong side
        close_side = "sell" if pos_signed > 0 else "buy"
        now_ts = time.time()
        if (now_ts - self._last_flatten_attempt_ts) < 5:
            wait_left = 5 - (now_ts - self._last_flatten_attempt_ts)
            self.logger.log(
                f"{self.timing_prefix} Flatten blocked: cooldown {wait_left:.2f}s remaining (pos={pos_signed}, side={close_side})",
                "INFO",
            )
            return False
        self._last_flatten_attempt_ts = now_ts
        _, remaining = await self._post_only_exit_batch(close_side, pos_abs, best_bid, best_ask, wait_sec=5.0)
        if remaining <= (self.min_order_size or Decimal("0")):
            self._last_flatten_attempt_ts = 0.0
            return True
        self.logger.log(
            f"{self.timing_prefix} Flatten incomplete: remaining {remaining} {close_side} after post-only exit batch",
            "INFO",
        )
        return False

    async def _execute_zigzag_stop(self, close_side: str, best_bid: Decimal, best_ask: Decimal):
        """Handle stop trigger: cancel TP, close position, reset state."""
        await self._cancel_zigzag_tp()
        pos_abs = abs(await self._get_position_signed_cached(force=True))
        ref_price = best_bid if close_side.lower() == "sell" else best_ask
        if pos_abs > 0:
            await self._post_only_exit_batch(close_side, pos_abs, best_bid, best_ask, wait_sec=5.0)
            await self._notify_zigzag_trade("exit", close_side, pos_abs, ref_price, note="stop-hit")
        prev_dir = self.direction_lock
        self._set_direction_all(None, mode="zigzag_timing")
        self.pending_entry = None
        self.pending_entry_state = None
        self.pending_break_price = None
        self.pending_break_trigger_ts = None
        self.pending_entry_static_mode = False
        await self._cancel_pending_entry_orders()
        self.zigzag_stop_price = None
        self.zigzag_entry_price = None
        self.dynamic_stop_price = None
        self.dynamic_stop_can_widen = False
        # HFT: block same direction reopen for 2 minutes after full close
        if self.hft_mode and prev_dir:
            self._hft_block_dir = prev_dir
            self._hft_block_until = time.time() + 120

    async def _cancel_pending_entry_orders(self):
        """Cancel tracked pending entry orders."""
        if not self._pending_entry_order_ids:
            return
        await self._cancel_order_ids(self._pending_entry_order_ids)
        self._pending_entry_order_ids = []

    async def _clear_pending_entry_state(self, clear_direction: bool = False):
        """Clear pending entry flags and optionally reset direction/stop state."""
        # Cancel any tracked static/chase open/close orders from the new breakout flow
        if self.pending_entry_state:
            close_ids = self.pending_entry_state.get("static_close_order_ids") or []
            open_ids = self.pending_entry_state.get("static_open_order_ids") or []
            chase_open_ids = self.pending_entry_state.get("chase_open_order_ids") or []
            to_cancel = [oid for oid in (close_ids + open_ids + chase_open_ids) if oid]
            if to_cancel:
                await self._cancel_order_ids(to_cancel)
        self.pending_entry_state = None
        await self._cancel_pending_entry_orders()
        if clear_direction:
            self._set_direction_all(None, lock=True, mode="zigzag_timing")
            self.zigzag_stop_price = None
            self.zigzag_entry_price = None
            self.dynamic_stop_price = None
            self.dynamic_stop_can_widen = False
            self.dynamic_stop_can_widen = False
        self.pending_entry = None
        self.pending_break_price = None
        self.pending_break_trigger_ts = None
        self.pending_entry_static_mode = False
        self._set_stop_new_orders(False, mode="zigzag_timing")
        self._last_entry_attempt_ts = 0.0

    async def _place_static_entry_orders(self, direction: str, break_price: Decimal, quantity: Decimal) -> List[str]:
        """Place static post-only entry orders anchored at break_price without chasing."""
        await self._cancel_pending_entry_orders()
        if quantity <= 0:
            return []
        max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else quantity
        num_orders = int((quantity / max_per).to_integral_value(rounding=ROUND_HALF_UP))
        if num_orders * max_per < quantity:
            num_orders += 1
        prices = self._build_price_ladder(direction, break_price, max(num_orders, 1))
        remaining = quantity
        placed: List[str] = []
        for px in prices:
            if remaining <= 0:
                break
            qty = min(max_per, remaining)
            res = await self._place_post_only_limit(direction, qty, px, reduce_only=False)
            if res and res.success:
                placed.append(res.order_id)
                remaining -= qty
        self._pending_entry_order_ids = placed
        return placed

    async def _finalize_pending_entry(self, direction: str, filled_qty: Decimal, entry_price: Decimal, stop_price: Decimal):
        """Finalize pending entry: set direction, stop, TP, clear state."""
        await self._cancel_pending_entry_orders()
        self._set_direction_all(direction, mode="zigzag_timing")
        self.pending_entry = None
        self.pending_break_price = None
        self.pending_break_trigger_ts = None
        self.pending_entry_static_mode = False
        self.zigzag_stop_price = stop_price
        self.zigzag_entry_price = entry_price
        self.dynamic_stop_price = stop_price
        self.dynamic_stop_can_widen = True
        await self._place_zigzag_tp(direction, filled_qty, entry_price, stop_price)
        await self._notify_zigzag_trade("entry", direction, filled_qty, entry_price)
        self._last_entry_attempt_ts = 0.0

    async def _process_pending_zigzag_entry(self, best_bid: Decimal, best_ask: Decimal):
        """Process pending zigzag/hft entry with breakout-driven close+reverse flow."""
        if not self.pending_entry:
            return
        direction = self.pending_entry
        desired_sign = 1 if direction == "buy" else -1
        buffer = self.break_buffer_ticks * self.config.tick_size
        min_qty = self.min_order_size or Decimal("0")

        if self.stop_new_orders:
            ok = await self._flatten_opposite(direction, best_bid, best_ask)
            if not ok:
                try:
                    pos_signed = await self._get_position_signed_cached(force=True)
                except Exception:
                    pos_signed = None
                self.logger.log(
                    f"{self.timing_prefix} Pending entry blocked: stop_new_orders active (reason={self.stop_new_orders_reason}) flatten failed pos={pos_signed}",
                    "INFO",
                )
            else:
                self.logger.log(
                    f"{self.timing_prefix} Pending entry blocked: stop_new_orders active (reason={self.stop_new_orders_reason}) but opposite flattened",
                    "INFO",
                )
            return

        # Build state on first entry attempt
        if not self.pending_entry_state:
            if direction == "buy":
                if self.last_confirmed_low is None or self.last_confirmed_high is None:
                    self.logger.log(f"{self.timing_prefix} Pending entry buy blocked: missing pivots", "INFO")
                    return
                stop_price = self.last_confirmed_low - buffer
                break_price = self.pending_break_price or (self.last_confirmed_high + buffer)
                anchor_price = best_ask
            else:
                if self.last_confirmed_high is None or self.last_confirmed_low is None:
                    self.logger.log(f"{self.timing_prefix} Pending entry sell blocked: missing pivots", "INFO")
                    return
                stop_price = self.last_confirmed_high + buffer
                break_price = self.pending_break_price or (self.last_confirmed_low - buffer)
                anchor_price = best_bid

            if anchor_price is None or anchor_price <= 0:
                self.logger.log(f"{self.timing_prefix} Pending entry blocked: invalid anchor price", "INFO")
                return
            if self.zigzag_tp_order_id:
                await self._cancel_zigzag_tp()

            target_entry_price = break_price
            if self.use_risk_exposure:
                target_qty = await self._calc_qty_by_risk_zigzag(target_entry_price, stop_price)
            else:
                target_qty = await self._calc_qty_by_leverage(target_entry_price)
            if target_qty is None or target_qty <= 0:
                reason = self._last_qty_reason
                if reason and "min_order_size" in reason:
                    return  # 已在 _notify_min_qty_block 记录/通知，避免重复日志
                self.logger.log(
                    f"{self.timing_prefix} Pending entry blocked: target_qty invalid (target_qty={target_qty}, entry={target_entry_price}, stop={stop_price})",
                    "INFO",
                )
                return

            # Cancel opposite-side orders before active close
            await self._cancel_orders_by_side("sell" if direction == "buy" else "buy")
            self.pending_entry_state = {
                "direction": direction,
                "stop_price": stop_price,
                "break_price": break_price,
                "target_qty": target_qty,
                "entry_ref_price": target_entry_price,
                "pivot_marker": self._last_pivot_change_ts,
                "last_close_attempt": 0.0,
                "last_open_attempt": 0.0,
                "static_close_order_ids": [],
                "static_open_order_ids": [],
                "chase_open_order_ids": [],
                "force_close_only": False,
                "skip_open": False,
                "stage": "close",
            }

        state = self.pending_entry_state
        pivot_updated = self._last_pivot_change_ts > state.get("pivot_marker", 0.0)
        stop_price = state["stop_price"]
        break_price = state["break_price"]
        target_qty = state["target_qty"]

        # Position snapshot
        pos_signed = await self._get_position_signed_cached(force=True)
        pos_abs = abs(pos_signed)
        opposite = (pos_signed * desired_sign) < 0

        self.logger.log(
            f"{self.timing_prefix} Pending entry state stage={state.get('stage')} dir={direction} desired_sign={desired_sign} pos={pos_signed} break={break_price} stop={stop_price} opposite={opposite}",
            "INFO",
        )

        # Determine chunk sizing
        def _chunk_amount(qty: Decimal) -> Decimal:
            max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else qty
            return min(qty, max_per)

        def _latest_pivot_label() -> Optional[str]:
            if self.recent_pivots:
                try:
                    return self.recent_pivots[-1].label
                except Exception:
                    return None
            return None

        def _pivot_adverse_for_direction(dir_val: str) -> bool:
            """Only treat new pivots as adverse if they oppose the intended direction."""
            if not pivot_updated:
                return False
            label = _latest_pivot_label()
            if not label:
                return False
            if dir_val == "buy":
                return label in ("LL", "HL", "L")
            return label in ("HH", "LH", "H")

        # Close opposite side first
        if state.get("stage") == "close":
            self.logger.log(
                f"{self.timing_prefix} Close stage pos={pos_signed} abs={pos_abs} desired={direction} break={break_price} stop={stop_price}",
                "INFO",
            )
            if (not opposite) or pos_abs <= min_qty:
                if state.get("static_close_order_ids"):
                    await self._cancel_order_ids(state["static_close_order_ids"])
                state["static_close_order_ids"] = []
                state["stage"] = "open"
            else:
                close_side = "buy" if pos_signed < 0 else "sell"
                # Maker pricing: buy against bid, sell against ask to avoid post-only rejection when inside breakout range
                book_price = best_bid if close_side == "buy" else best_ask
                now_ts = time.time()
                adverse_pivot = _pivot_adverse_for_direction(direction)
                # Pivot update while static close pending -> force close-only if adverse
                if adverse_pivot and state.get("static_close_order_ids"):
                    await self._cancel_order_ids(state["static_close_order_ids"])
                    state["static_close_order_ids"] = []
                    state["force_close_only"] = True
                    state["skip_open"] = True
                    # Block same-direction re-entry until opposite breakout to avoid immediate reopen after adverse pivot
                    self._blocked_direction = direction
                    state["last_close_attempt"] = 0.0
                    self.logger.log(
                        f"{self.timing_prefix} Adverse pivot during static close; force close-only and skip open for this signal",
                        "INFO",
                    )
                favourable = False
                # 判定用中间价，避免仅看买一/卖一因点差误判
                mid_price = None
                try:
                    mid_price = (best_bid + best_ask) / 2
                except Exception:
                    pass
                ref_price = mid_price if mid_price is not None else book_price
                if ref_price is not None and ref_price > 0:
                    # 回到破位价内为有利侧：
                    # 平空开多（close_side=buy）有利：价格 < 破位价；平多开空（close_side=sell）有利：价格 > 破位价
                    tolerance = self.config.tick_size / Decimal(2)
                    if close_side == "buy":
                        favourable = ref_price <= (break_price - tolerance)
                    else:
                        favourable = ref_price >= (break_price + tolerance)
                # Unthrottled log to diagnose unfavourable chasing
                self.logger.log(
                    f"{self.timing_prefix} Close eval side={close_side} bid={best_bid} ask={best_ask} ref={ref_price} break={break_price} fav={favourable} pos={pos_signed}",
                    "INFO",
                )
                # 被强制 close-only：若已有挂单则等待，否则按盘口价挂一次
                if state.get("force_close_only"):
                    if not state.get("static_close_order_ids") and book_price is not None and book_price > 0:
                        qty = _chunk_amount(pos_abs)
                        res = await self._place_post_only_limit(close_side, qty, book_price, reduce_only=True)
                        state["last_close_attempt"] = now_ts
                        if res and res.success:
                            self._invalidate_position_cache()
                    return
                if favourable:
                    # 价格更有利：按 3s 节奏追价，取消静态挂单
                    self._log_once(
                        f"close_mode:{direction}",
                        f"{self.timing_prefix} Close favourable: book={book_price} break={break_price} -> chase 3s",
                        "INFO",
                        interval=5.0,
                    )
                    if state.get("static_close_order_ids"):
                        await self._cancel_order_ids(state["static_close_order_ids"])
                        state["static_close_order_ids"] = []
                    if book_price is not None and book_price > 0 and (now_ts - state.get("last_close_attempt", 0.0)) >= 3.0:
                        qty = _chunk_amount(pos_abs)
                        res = await self._place_post_only_limit(close_side, qty, book_price, reduce_only=True)
                        state["last_close_attempt"] = now_ts
                        if res and res.success:
                            self._invalidate_position_cache()
                    return
                # 不利时挂破位价静态单，取消追价
                self._log_once(
                    f"close_mode_static:{direction}",
                    f"{self.timing_prefix} Close static at break {break_price} (book={book_price})",
                    "INFO",
                    interval=5.0,
                )
                if book_price is not None and book_price > 0:
                    if state.get("static_close_order_ids"):
                        pass
                    else:
                        remaining_close = pos_abs
                        placed_ids: List[str] = []
                        while remaining_close > 0:
                            qty = _chunk_amount(remaining_close)
                            res = await self._place_post_only_limit(close_side, qty, break_price, reduce_only=True)
                            if res and res.success:
                                placed_ids.append(res.order_id)
                                remaining_close -= qty
                            else:
                                break
                        if placed_ids:
                            state["static_close_order_ids"] = placed_ids
                            self.logger.log(
                                f"{self.timing_prefix} Close static placed @ {break_price} ids={placed_ids} qty={pos_abs}",
                                "INFO",
                            )
                        else:
                            self.logger.log(
                                f"{self.timing_prefix} Close static placement failed side={close_side} qty={pos_abs} break={break_price}",
                                "WARNING",
                            )
                return

        # If we get here, closing is done or not needed
        if state.get("skip_open"):
            self._log_once("skip_open", f"{self.timing_prefix} Skip open due to prior pivot update during close", "INFO", interval=10.0)
            # Block same-direction re-entry until opposite breakout
            if self._blocked_direction != direction:
                self._blocked_direction = direction
            await self._clear_pending_entry_state(clear_direction=True)
            return

        open_adverse = _pivot_adverse_for_direction(direction)

        # Opening new direction
        current_dir_qty = await self._get_directional_position(direction, force=True)
        remaining = max(Decimal(0), target_qty - current_dir_qty)
        if remaining <= min_qty:
            entry_px = state.get("entry_ref_price", break_price)
            await self._cancel_order_ids(state.get("static_open_order_ids", []))
            await self._finalize_pending_entry(direction, current_dir_qty, entry_px, stop_price)
            self.pending_entry_state = None
            return

        if open_adverse and remaining > 0:
            # abandon entry only when adverse pivot appears before entry filled, and block this direction until opposite breakout
            await self._cancel_order_ids(state.get("static_open_order_ids", []))
            self._blocked_direction = direction
            self.logger.log(f"{self.timing_prefix} Abandon open: adverse pivot arrived before entry filled; block {direction} until opposite breakout", "INFO")
            # 如果仍有反向持仓，重新转为 close-only 追价
            try:
                pos_abs = abs(await self._get_position_signed_cached(force=True))
            except Exception:
                pos_abs = Decimal(0)
            if pos_abs > min_qty:
                state["stage"] = "close"
                state["force_close_only"] = True
                state["skip_open"] = True
                state["static_open_order_ids"] = []
                state["static_close_order_ids"] = []
                state["last_close_attempt"] = 0.0
                self._log_once(
                    f"re_close:{direction}",
                    f"{self.timing_prefix} Adverse pivot -> re-enter close-only chasing for remaining pos={pos_abs}",
                    "INFO",
                    interval=5.0,
                )
            else:
                await self._clear_pending_entry_state(clear_direction=True)
            return

        # Maker pricing: buy using bid, sell using ask to keep orders resting when price re-enters breakout band
        book_price = best_bid if direction == "buy" else best_ask
        favourable_open = False
        mid_price = None
        try:
            mid_price = (best_bid + best_ask) / 2
        except Exception:
            pass
        ref_price = mid_price if mid_price is not None else book_price
        if ref_price is not None and ref_price > 0:
            # 开仓与平仓同判定：
            # 开多（平空开多）有利：价格 < 破位价；开空（平多开空）有利：价格 > 破位价
            if direction == "buy":
                favourable_open = ref_price < break_price
            else:
                favourable_open = ref_price > break_price

        self._log_once(
            f"open_eval:{direction}",
            f"{self.timing_prefix} Open eval dir={direction} ref={ref_price} break={break_price} favourable={favourable_open} remaining={remaining}",
            "INFO",
            interval=2.0,
        )

        now_ts = time.time()
        if favourable_open and (now_ts - state.get("last_open_attempt", 0.0)) >= 3.0:
            # Cancel previous chase orders to avoid piling up
            if state.get("chase_open_order_ids"):
                await self._cancel_order_ids(state["chase_open_order_ids"])
                state["chase_open_order_ids"] = []
            if state.get("static_open_order_ids"):
                await self._cancel_order_ids(state["static_open_order_ids"])
                state["static_open_order_ids"] = []
            qty = _chunk_amount(remaining)
            res = await self._place_post_only_limit(direction, qty, book_price, reduce_only=False)
            state["last_open_attempt"] = now_ts
            if res and res.success:
                state["entry_ref_price"] = book_price
                state["chase_open_order_ids"] = [res.order_id]
                self._invalidate_position_cache()
            else:
                self.logger.log(
                    f"{self.timing_prefix} Open attempt failed post-only {direction} qty={qty} @ {book_price}, will retry in 3s",
                    "INFO",
                )
        elif not favourable_open and book_price is not None and book_price > 0:
            # Cancel any chase orders before parking static entries
            if state.get("chase_open_order_ids"):
                await self._cancel_order_ids(state["chase_open_order_ids"])
                state["chase_open_order_ids"] = []
            if state.get("static_open_order_ids"):
                pass
            else:
                remaining_open = remaining
                placed_ids: List[str] = []
                while remaining_open > 0:
                    qty = _chunk_amount(remaining_open)
                    res = await self._place_post_only_limit(direction, qty, break_price, reduce_only=False)
                    if res and res.success:
                        placed_ids.append(res.order_id)
                        remaining_open -= qty
                    else:
                        break
                if placed_ids:
                    state["static_open_order_ids"] = placed_ids

        # Check fill progress
        current_dir_qty = await self._get_directional_position(direction, force=True)
        if current_dir_qty >= max(min_qty, target_qty - min_qty):
            entry_px = state.get("entry_ref_price", break_price)
            await self._cancel_order_ids(state.get("chase_open_order_ids", []))
            await self._cancel_order_ids(state.get("static_open_order_ids", []))
            await self._finalize_pending_entry(direction, current_dir_qty, entry_px, stop_price)
            self.pending_entry_state = None
        else:
            self._log_pending_entry_progress(direction, current_dir_qty, target_qty)

    async def _sync_direction_lock(self, force: bool = False) -> Decimal:
        """Keep direction_lock in sync with actual position."""
        # Slow down REST hits when idle/stable; still allow callers to force refresh
        need_fast = force or bool(
            self.pending_entry
            or self.pending_reverse_state
            or self.pending_reverse_direction
            or self.webhook_stop_mode
        )
        pos_signed = await self._get_position_signed_cached(force=need_fast)
        if pos_signed > 0:
            self.direction_lock = "buy"
        elif pos_signed < 0:
            self.direction_lock = "sell"
        else:
            if not self.pending_entry:
                self.direction_lock = None
                self.zigzag_stop_price = None
                self.dynamic_stop_price = None
                self.dynamic_stop_direction = None
                await self._cancel_zigzag_tp()
        return pos_signed

    async def _realign_direction_state(self, trade_price: Optional[Decimal], pos_signed: Optional[Decimal] = None, reason: Optional[str] = None):
        """
        Align direction/config with actual position and breakout context.
        - When holding a position, trust the signed position to set direction.
        - When flat, follow breakout of confirmed pivots to reset direction.
        """
        if pos_signed is None:
            pos_signed = await self._get_position_signed_cached(force=True)
        pos_abs = abs(pos_signed)
        tolerance = self.min_order_size or Decimal("0")
        buffer = self.break_buffer_ticks * self.config.tick_size

        # Tiny residual position: try to flatten via reduce-only market, without locking direction
        if tolerance > 0 and pos_abs > 0 and pos_abs < tolerance:
            dir_hint = "buy" if pos_signed > 0 else "sell"
            now = time.time()
            same_residual = (
                self._residual_last_dir == dir_hint
                and self._residual_last_qty is not None
                and abs(self._residual_last_qty - pos_abs) < (tolerance / Decimal(1000))
                and (now - self._residual_last_ts) < 30
                and self._residual_last_fail
            )
            if same_residual:
                self.logger.log(
                    f"{self.timing_prefix} Residual pos {pos_abs} {dir_hint} < min {tolerance}; skip repeat close attempt (cooldown)",
                    "DEBUG",
                )
                return
            self.logger.log(
                f"{self.timing_prefix} Residual pos {pos_abs} {dir_hint} < min {tolerance}; sending reduce-only market close",
                "INFO",
            )
            try:
                success = await self._close_residual_position_market(pos_signed)
            except Exception as exc:
                self._residual_last_fail = True
                self.logger.log(f"{self.timing_prefix} Residual close failed: {exc}", "WARNING")
            else:
                self._residual_last_fail = not success
            self._residual_last_dir = dir_hint
            self._residual_last_qty = pos_abs
            self._residual_last_ts = now
            return

        if pos_abs > tolerance:
            actual_dir = "buy" if pos_signed > 0 else "sell"
            changed = self._set_direction_all(actual_dir, mode="zigzag_timing")
            if self.pending_entry and self.pending_entry == actual_dir:
                self.pending_entry = None
                self.pending_break_trigger_ts = None
                self.pending_entry_state = None
                changed = True
            if changed:
                self.logger.log(f"{self.timing_prefix} Direction realigned to {actual_dir.upper()} based on position {pos_signed}", "INFO")
            needs_stop_refresh = changed or (self.dynamic_stop_price is None)
            if self.dynamic_stop_direction and self.dynamic_stop_direction != actual_dir:
                self.dynamic_stop_price = None
                self.dynamic_stop_direction = None
                needs_stop_refresh = True
            # Ensure stop tracking matches the real direction
            if self.enable_dynamic_sl and needs_stop_refresh:
                await self._refresh_stop_loss(force=True)
            return

        # Flat: allow breakout to reset intended direction (without entering yet)
        self._set_direction_all(None, mode="zigzag_timing")
        self.dynamic_stop_price = None
        self.dynamic_stop_direction = None
        self.dynamic_stop_can_widen = False
        if self.zigzag_timing_enabled and not self.webhook_stop_mode:
            self._set_stop_new_orders(False, mode="zigzag_timing")
            self.redundancy_insufficient_since = None
        if (not self.pending_entry) and trade_price is not None:
            new_dir = None
            if self.last_confirmed_high is not None and trade_price >= (self.last_confirmed_high + buffer):
                new_dir = "buy"
            elif self.last_confirmed_low is not None and trade_price <= (self.last_confirmed_low - buffer):
                new_dir = "sell"
            if new_dir and new_dir != self.config.direction:
                self._set_direction_all(new_dir, lock=False, mode="zigzag_timing")
                self.logger.log(f"{self.timing_prefix} Flat breakout sets direction to {new_dir.upper()}", "INFO")

    async def _close_residual_position_market(self, pos_signed: Decimal) -> bool:
        """Close tiny residual position with reduce-only market path."""
        pos_abs = abs(pos_signed)
        if pos_abs <= 0:
            return True
        close_side = "sell" if pos_signed > 0 else "buy"
        try:
            await self.exchange_client.reduce_only_close_with_retry(pos_abs, close_side)
            return True
        except Exception:
            # Fallback to market close if reduce-only helper fails
            if hasattr(self.exchange_client, "place_market_order"):
                await self.exchange_client.place_market_order(self.config.contract_id, pos_abs, close_side)
                return True
            raise

    async def _handle_zigzag_timing_cycle(self):
        """Main loop for zigzag_timing mode."""
        await self._sync_external_pivots()
        need_force_bbo = bool(self.pending_entry or self.direction_lock)
        best_bid, best_ask = await self._get_bbo_for_zigzag(force=need_force_bbo)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")
        trade_price = await self._get_last_trade_price_cached(best_bid, best_ask, force=False)
        pos_signed = await self._sync_direction_lock()
        await self._realign_direction_state(trade_price, pos_signed, reason="loop")

        # Stop/structural checks: in timing/HFT, convert to breakout signal so close/open flow stays unified
        signal = None
        buffer = self.break_buffer_ticks * self.config.tick_size
        active_stop = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price is not None) else self.zigzag_stop_price
        if self.direction_lock and active_stop is not None:
            if self.direction_lock == "buy" and best_bid <= active_stop:
                signal = "sell"
            if self.direction_lock == "sell" and best_ask >= active_stop:
                signal = "buy"

        if self.direction_lock:
            if self.direction_lock == "buy" and self.last_confirmed_low is not None and best_bid <= (self.last_confirmed_low - buffer):
                self.logger.log(f"{self.timing_prefix} Structural close (converted to signal): long breaks last confirmed low", "INFO")
                signal = "sell"
            if self.direction_lock == "sell" and self.last_confirmed_high is not None and best_ask >= (self.last_confirmed_high + buffer):
                self.logger.log(f"{self.timing_prefix} Structural close (converted to signal): short breaks last confirmed high", "INFO")
                signal = "buy"

        # Detect breakout signals
        if trade_price is not None:
            if self.last_confirmed_high is not None and trade_price >= (self.last_confirmed_high + buffer):
                signal = "buy"
            if self.last_confirmed_low is not None and trade_price <= (self.last_confirmed_low - buffer):
                signal = "sell" if signal is None else signal
        if signal:
            if self.direction_lock and self.direction_lock == signal:
                signal = None
            else:
                # unblock if opposite breakout
                if self._blocked_direction and signal != self._blocked_direction:
                    self._blocked_direction = None
                # HFT 同向冷却 2 分钟
                if self.hft_mode and self._hft_block_dir == signal and time.time() < self._hft_block_until:
                    self._log_once("hft_block_dir", f"{self.timing_prefix} Block {signal} until {int(self._hft_block_until - time.time())}s later (cooldown)", "INFO", interval=5.0)
                    signal = None
                # block same-direction signal when flagged
                if self._blocked_direction and signal == self._blocked_direction:
                    self._log_once("blocked_direction", f"{self.timing_prefix} Pending entry blocked for {signal} until opposite breakout", "INFO", interval=10.0)
                    signal = None
            if signal and self.pending_entry != signal:
                self.pending_entry = signal
                self.pending_entry_state = None
                self.pending_break_trigger_ts = time.time()
                self._last_entry_attempt_ts = 0.0
                self._last_flatten_attempt_ts = 0.0
                self.pending_entry_static_mode = False
                await self._cancel_pending_entry_orders()
                try:
                    if signal == "buy" and self.last_confirmed_high is not None:
                        self.pending_break_price = self.last_confirmed_high + buffer
                    elif signal == "sell" and self.last_confirmed_low is not None:
                        self.pending_break_price = self.last_confirmed_low - buffer
                except Exception:
                    self.pending_break_price = None

        if self.pending_entry:
            await self._process_pending_zigzag_entry(best_bid, best_ask)

    async def _get_position_signed_cached(self, force: bool = False) -> Decimal:
        key = self._cache_key("position")
        ttl = self.ttl_position
        if self.zigzag_timing_enabled:
            needs_fast = bool(
                self.pending_entry
                or self.redundancy_insufficient_since
                or self.pending_reverse_state
                or self.pending_reverse_direction
                or self.webhook_stop_mode
            )
            if not needs_fast:
                ttl = max(self.ttl_position_idle, self.ttl_position)
        return await self.cache.get(
            key,
            ttl,
            lambda: self.exchange_client.get_account_positions(),
            force=force
        )

    async def _get_position_detail_prefer_ws(self) -> Optional[Tuple[Decimal, Decimal]]:
        """
        Prefer WS-sourced position detail if available; REST get_position_detail only when WS is unavailable.
        Returns (signed_position, avg_price) or None if unavailable.
        """
        ws_candidates = [
            "get_position_detail_ws",
            "get_ws_position_detail",
            "get_position_detail_from_ws",
        ]
        for name in ws_candidates:
            fn = getattr(self.exchange_client, name, None)
            if callable(fn):
                try:
                    res = await fn()
                    if res and isinstance(res, (list, tuple)) and len(res) == 2:
                        pos_val, avg_val = res
                        return Decimal(pos_val), Decimal(avg_val)
                except Exception as e:
                    self.logger.log(f"[RISK] WS position detail {name} failed: {e}", "WARNING")
                    break  # WS path failed; allow REST fallback

        if hasattr(self.exchange_client, "get_position_detail"):
            try:
                pos_signed_detail, avg_price_detail = await self.exchange_client.get_position_detail()
                return Decimal(pos_signed_detail), Decimal(avg_price_detail)
            except Exception as e:
                self.logger.log(f"[RISK] get_position_detail failed: {e}", "WARNING")
        return None

    def _order_ws_available(self) -> bool:
        return bool(getattr(self.exchange_client, "ws_manager", None)) or bool(getattr(self.exchange_client, "order_ws_enabled", False))

    async def _get_order_info_cached(self, order_id: str, force: bool = False):
        key = self._order_info_key(order_id)
        return await self.cache.get(
            key,
            self.ttl_order_info,
            lambda: self.exchange_client.get_order_info(order_id),
            force=force
        )


    def _set_stop_new_orders(self, enable: bool, mode: Optional[str] = None, reason: Optional[str] = None):
        """Set stop_new_orders flag and track duration with reason logging."""
        mode = mode or self.mode_tag
        if mode != self.mode_tag:
            if self.cache.debug:
                self.logger.log(f"{self.mode_prefix} skip stop_new_orders update from mode {mode}", "DEBUG")
            return
        if enable:
            if not self.stop_new_orders:
                self.stop_new_since = time.time()
                self.stop_new_orders_since = self.stop_new_since
                self.stop_new_orders_reason = reason or "unspecified"
                self.logger.log(
                    f"{self.mode_prefix} stop_new_orders enabled: reason={self.stop_new_orders_reason}",
                    "INFO",
                )
            self.stop_new_orders = True
            if reason:
                self.stop_new_orders_reason = reason
        else:
            if self.stop_new_orders:
                self.logger.log(f"{self.mode_prefix} stop_new_orders disabled", "INFO")
            self.stop_new_orders = False
            self.stop_new_since = None
            self.stop_new_orders_since = None
            self.stop_new_orders_reason = None
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
                        exchange_key = self.config.exchange or entry.get("raw_ticker")
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

    async def _backfill_recent_pivot_prices(self, max_each: int = 2):
        """Backfill latest high/low pivot prices for current exchange if missing."""
        if not self.enable_zigzag:
            return
        try:
            store = self._load_json_file(self.zigzag_pivot_file) or {}
            base_bucket = store.get(self.symbol_base, {}) if isinstance(store, dict) else {}
            tf_bucket = base_bucket.get(self.zigzag_timeframe_key) or base_bucket.get(str(self.zigzag_timeframe_key))
            if not tf_bucket or not isinstance(tf_bucket, list):
                return
            highs: List[Dict[str, Any]] = []
            lows: List[Dict[str, Any]] = []
            for item in reversed(tf_bucket):
                try:
                    label = str(item.get("label", "")).upper()
                except Exception:
                    continue
                if label in ("HH", "LH") and len(highs) < max_each:
                    highs.append(item)
                elif label in ("LL", "HL") and len(lows) < max_each:
                    lows.append(item)
                if len(highs) >= max_each and len(lows) >= max_each:
                    break
            targets = highs + lows
            if not targets:
                return
            updated = False
            exch_key = self.config.exchange
            for item in targets:
                label = str(item.get("label", "")).upper()
                if item.get(f"price_{exch_key}") is not None:
                    continue
                close_raw = item.get("close_time_utc") or item.get("pivot_bar_close") or item.get("close_time")
                close_dt = self._parse_pivot_time(str(close_raw)) if close_raw else None
                if close_dt is None:
                    continue
                candle = await self._fetch_candle_for_time(close_dt)
                if not candle:
                    continue
                high_val, low_val = candle
                price = high_val if label in ("HH", "LH") else low_val
                item[f"price_{exch_key}"] = str(price)
                updated = True
            if updated:
                try:
                    self.zigzag_pivot_file.parent.mkdir(parents=True, exist_ok=True)
                    tmp = self.zigzag_pivot_file.with_suffix(".tmp")
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(store, f, ensure_ascii=False, indent=2, default=str)
                    tmp.replace(self.zigzag_pivot_file)
                    self._pivot_file_mtime = self.zigzag_pivot_file.stat().st_mtime
                    if self.cache.debug:
                        self.logger.log("[ZIGZAG] Backfilled recent pivot prices for exchange", "DEBUG")
                except Exception as exc:
                    self.logger.log(f"[ZIGZAG] Backfill pivot prices persist failed: {exc}", "WARNING")
        except Exception as exc:
            self.logger.log(f"[ZIGZAG] Backfill recent pivot prices failed: {exc}", "WARNING")

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
        equity_key = f"equity:{self.mode_tag}"
        if self._equity_cache is not None and (now_ts - self._equity_cache_ts) < self.ttl_equity:
            return self._equity_cache

        async def _fetch_equity():
            equity_val = None
            if hasattr(self.exchange_client, "get_account_equity"):
                try:
                    equity_val = await self.exchange_client.get_account_equity()
                except Exception as e:
                    self.logger.log(f"[RISK] get_account_equity failed: {e}", "WARNING")
            if equity_val is None and hasattr(self.exchange_client, "get_available_balance"):
                try:
                    equity_val = await self.exchange_client.get_available_balance()
                except Exception as e:
                    self.logger.log(f"[RISK] get_available_balance as equity fallback failed: {e}", "WARNING")
            if equity_val is None:
                # If we still have nothing, fall back to last known good equity
                return self._equity_last_nonzero
            return equity_val

        equity = await self.cache.get(equity_key, self.ttl_equity, _fetch_equity, force=False)
        if equity is None or equity <= 0:
            self.cache.invalidate(equity_key)
            return self._equity_last_nonzero
        self._equity_cache = equity
        self._equity_cache_ts = now_ts
        self._equity_last_nonzero = equity
        return equity

    async def _get_balance_snapshot(self) -> Optional[Decimal]:
        """Fetch balance with short TTL; reuse equity failure semantics."""
        key = f"balance:{self.mode_tag}"

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
                    labels = [lbl for lbl in (self.account_name, self.mode_tag) if lbl]
                    prefix = f"[{'|'.join(labels)}] " if labels else ""
                    tg_bot.send_text(f"{prefix}[PNL] Daily PnL unavailable (missing equity data)")
            else:
                pnl = equity - self.daily_pnl_baseline
                labels = [lbl for lbl in (self.account_name, self.mode_tag) if lbl]
                prefix = f"[{'|'.join(labels)}] " if labels else ""
                msg = f"{prefix}[PNL] {current_date} Equity: {equity} | PnL: {pnl}"
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
        if pivot.label in ("HH", "LH", "H"):
            self.last_confirmed_high = pivot.price
        if pivot.label in ("LL", "HL", "L"):
            self.last_confirmed_low = pivot.price
        self.recent_pivots.append(pivot)
        self._last_pivot_change_ts = time.time()

    def _compute_dynamic_stop(self, direction: str) -> Optional[Decimal]:
        """Compute dynamic stop based on confirmed pivots and configurable tick buffer."""
        tick = self.config.tick_size
        buffer_ticks = self.break_buffer_ticks * tick
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
                await self._throttle_rest("candle", min_interval=1.0)
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
                await self._throttle_rest("candle", min_interval=1.0)
                try:
                    candles = await self.exchange_client.fetch_history_candles(limit=300, timeframe=tf)
                    if not candles:
                        return None
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
                exch_key = self.config.exchange
                if exch_key:
                    price_val = item.get(f"price_{exch_key}")
                if price_val is None:
                    # fallback to raw_ticker-based key for backward compatibility
                    if raw_ticker:
                        price_val = item.get(f"price_{raw_ticker}")
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

    def _log_pivot_price_delay(self, pivot: PivotPoint, price_type: str, fetch_ts: float):
        if not pivot or not pivot.close_time:
            return
        try:
            pivot_ts = pivot.close_time.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            return
        delay_sec = int(fetch_ts - pivot_ts)
        pivot_dt = pivot.close_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        fetch_dt = datetime.fromtimestamp(fetch_ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.log(
            f"[ZIGZAG] Received {pivot.label} at {pivot_dt}, {price_type} price fetched at {fetch_dt}, Δ+{delay_sec}s",
            "INFO",
        )

    async def _notify_min_qty_block(self, qty: Decimal, entry_price: Optional[Decimal] = None, stop_price: Optional[Decimal] = None):
        """Log + (throttled) notify when calculated qty is below min_order_size."""
        msg_parts = [
            f"{self.timing_prefix} calc qty {qty} < min_order_size {self.min_order_size}; skip entry"
        ]
        if entry_price is not None:
            msg_parts.append(f"entry={entry_price}")
        if stop_price is not None:
            msg_parts.append(f"stop={stop_price}")
        msg = " ".join(str(x) for x in msg_parts)
        self.logger.log(msg, "INFO")
        now_ts = time.time()
        if self.enable_notifications and (now_ts - self._min_qty_block_last_ts > 5):
            await self.send_notification(msg)
        self._min_qty_block_last_ts = now_ts

    def _log_once(self, key: str, msg: str, level: str = "INFO", interval: float = 3.0):
        """
        Log only when message changes or interval elapses to reduce noisy repeats.
        key: logical bucket (include direction/signal as needed)
        """
        now_ts = time.time()
        last_msg, last_ts = self._log_throttle.get(key, ("", 0.0))
        if msg == last_msg and (now_ts - last_ts) < interval:
            return
        self._log_throttle[key] = (msg, now_ts)
        self.logger.log(msg, level)

    def _log_pending_entry_progress(self, direction: str, filled: Decimal, target: Decimal):
        key = f"pending_entry:{direction}"
        msg = f"{self.timing_prefix} Entry pending ({direction}) filled {filled}/{target}"
        last = self._pending_log_cache.get(key)
        if last == msg:
            return
        self._pending_log_cache[key] = msg
        self.logger.log(msg, "INFO")

    async def _build_pivot_point(self, entry: Dict[str, Any]) -> Optional[PivotPoint]:
        """Build PivotPoint with price derived from exchange OHLC data."""
        price = entry.get("price")
        if price is None:
            candle = await self._fetch_candle_for_time(entry["close_time"])
            if not candle:
                self.logger.log(f"[ZIGZAG] Missing OHLC for pivot at {entry['close_time']}", "WARNING")
                return None
            high, low = candle
            is_high_label = entry["label"] in ("HH", "LH", "H")
            price = high if is_high_label else low
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
            self._set_direction_all(desired_dir, lock=False, mode=self.mode_tag)
            self.logger.log(f"[INIT] Direction set to {desired_dir.upper()} via pivot {latest.label} at {latest.close_time}", "INFO")

    async def _initialize_pivots_from_store(self):
        """Load existing pivots on startup to seed state without notifications."""
        if not self.enable_zigzag:
            return
        entries = self._load_pivots_for_config()
        if not entries:
            return
        # 先补全缺失价格，再标记 processed，避免覆盖已有价格
        for entry in entries:
            try:
                pivot_point = await self._build_pivot_point(entry)
            except Exception as exc:
                self.logger.log(f"[INIT] Build pivot failed: {exc}", "WARNING")
                pivot_point = None
            if pivot_point:
                key = (pivot_point.close_time.isoformat(), pivot_point.label)
                self._pivot_point_cache[key] = pivot_point
                self._processed_pivot_keys.add(key)
                # Seed confirmed high/low so timing mode has break reference on startup
                self._update_confirmed_pivots(pivot_point)
        # 仅根据最近 pivot 判向并获取匹配方向的最近止损位，减少 OHLC 查询
        direction, stop_entry = self._determine_initial_direction_and_stop(entries)
        if direction and self.enable_advanced_risk and self.stop_loss_enabled and self.enable_zigzag:
            if direction != self.config.direction:
                self._set_direction_all(direction, lock=False, mode=self.mode_tag)
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
        if self.hft_mode:
            latest_high_entry = None
            latest_low_entry = None
            for entry in entries:
                try:
                    label = str(entry.get("label", "")).upper()
                    close_time = entry.get("close_time")
                except Exception:
                    continue
                if not close_time:
                    continue
                if label in ("H", "HH", "LH"):
                    if (latest_high_entry is None) or (close_time > latest_high_entry.get("close_time")):
                        latest_high_entry = entry
                if label in ("L", "LL", "HL"):
                    if (latest_low_entry is None) or (close_time > latest_low_entry.get("close_time")):
                        latest_low_entry = entry
            for candidate in (latest_high_entry, latest_low_entry):
                if not candidate:
                    continue
                key = (candidate["close_time"].isoformat(), candidate["label"])
                pivot_point = self._pivot_point_cache.get(key)
                if not pivot_point:
                    try:
                        pivot_point = await self._build_pivot_point(candidate)
                    except Exception as exc:
                        self.logger.log(f"[INIT] Build HFT pivot failed: {exc}", "WARNING")
                        pivot_point = None
                if pivot_point:
                    self._pivot_point_cache[key] = pivot_point
                    self._processed_pivot_keys.add(key)
                    self._update_confirmed_pivots(pivot_point)

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

    async def _refresh_stop_loss(self, force: bool = False, mode: Optional[str] = None):
        """Refresh stop-loss tracking without placing native SL orders."""
        mode = mode or self.mode_tag
        if mode != self.mode_tag:
            if self.cache.debug:
                self.logger.log(f"{self.mode_prefix} skip refresh_stop_loss from mode {mode}", "DEBUG")
            return
        if not self.enable_dynamic_sl:
            return

        # Cancel any existing native SL once, then operate in internal-only mode
        if self.current_sl_order_id:
            try:
                await self.exchange_client.cancel_order(self.current_sl_order_id)
            except Exception:
                pass
            self.current_sl_order_id = None

        try:
            pos_signed = await self._get_position_signed_cached(force=force)
        except Exception as e:
            self.logger.log(f"[SL] Failed to fetch position for refresh: {e}", "WARNING")
            return

        if pos_signed is None:
            self.logger.log(f"[SL] Position is None, skipping refresh", "WARNING")
            return

        pos_abs = abs(pos_signed)
        if pos_abs == 0:
            self.dynamic_stop_price = None
            self.dynamic_stop_direction = None
            self.dynamic_stop_can_widen = False
            return

        direction = "buy" if pos_signed > 0 else "sell"
        # Prefer actual position sign; only fallback to current_direction when flat (handled above)
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
            widen = False
            if direction == "buy" and struct_stop < self.dynamic_stop_price:
                widen = True
            if direction == "sell" and struct_stop > self.dynamic_stop_price:
                widen = True
            if widen and self.dynamic_stop_can_widen:
                self.dynamic_stop_can_widen = False
                dyn_stop = struct_stop
            else:
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
        self.dynamic_stop_can_widen = False
        if self.enable_notifications and prev_stop != self.dynamic_stop_price:
            try:
                await self.send_notification(f"[SL] Dynamic stop updated from {prev_stop} to {self.dynamic_stop_price} for {direction.upper()}")
            except Exception:
                pass
        # After SL update, re-evaluate redundancy and optionally stop new orders
        await self._run_redundancy_check(direction, pos_signed, mode=mode)

    async def _run_redundancy_check(self, direction: str, pos_signed: Decimal, mode: Optional[str] = None):
        """Re-evaluate redundancy and update stop_new_orders state."""
        mode = mode or self.mode_tag
        if mode != self.mode_tag:
            if self.cache.debug:
                self.logger.log(f"{self.mode_prefix} skip redundancy_check from mode {mode}", "DEBUG")
            return
        # Timing 模式不依赖冗余检查来阻止新单，改由方向锁 + min_order_size 控制
        if self.zigzag_timing_enabled:
            return
        # 风险敞口关闭时不计算冗余，也不因冗余停新单
        if not self.use_risk_exposure:
            self._set_stop_new_orders(False, mode=self.mode_tag)
            self.redundancy_insufficient_since = None
            return
        if not (self.stop_loss_enabled and self.enable_advanced_risk):
            return
        try:
            position_amt = abs(pos_signed)
            if position_amt == 0:
                self._set_stop_new_orders(False, mode=self.mode_tag)
                self.redundancy_insufficient_since = None
                return
            best_bid, best_ask = await self._get_bbo_cached()
            mid_price = (best_bid + best_ask) / 2
            # Need avg entry price; fallback: mid_price
            avg_price = mid_price
            stop_price = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price) else None
            pos_detail = await self._get_position_detail_prefer_ws()
            if pos_detail:
                pos_signed_detail, avg_price_detail = pos_detail
                if pos_signed_detail != 0:
                    avg_price = avg_price_detail

            if stop_price and position_amt > 0:
                if pos_signed > 0:
                    per_base_loss = (avg_price - stop_price)
                else:
                    per_base_loss = (stop_price - avg_price)
                if per_base_loss <= 0:
                    return
                equity = await self._get_equity_snapshot()
                if equity is None:
                    return
                max_loss = equity * (self.risk_pct / Decimal(100))
                allowed_position = max_loss / per_base_loss
                if allowed_position < 0:
                    allowed_position = Decimal(0)
                allowed_position = self._round_quantity(allowed_position)
                excess = max(Decimal(0), position_amt - allowed_position)
                tolerance = self.min_order_size or Decimal("0")
                if excess >= tolerance and position_amt > 0:
                    try:
                        close_qty = excess
                        close_side = 'sell' if pos_signed > 0 else 'buy'
                        await self.exchange_client.reduce_only_close_with_retry(close_qty, close_side)
                        self.logger.log(f"[RISK] Reduced excess position {close_qty} to keep stop-loss exposure within limit", "WARNING")
                        self._invalidate_position_cache()
                    except Exception as e:
                        self.logger.log(f"[RISK] Failed to trim excess position: {e}", "ERROR")
                headroom = allowed_position - position_amt
                needed_headroom = self.config.quantity or Decimal(0)
                if headroom < needed_headroom:
                    if not self.stop_new_orders and self.enable_notifications:
                        await self.send_notification(f"[RISK] Stop new orders after SL update: headroom {headroom} < qty {self.config.quantity}")
                    self._set_stop_new_orders(True, mode=self.mode_tag, reason="redundancy_insufficient")
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
                    self._set_stop_new_orders(False, mode=self.mode_tag)
                    self.redundancy_insufficient_since = None
        except Exception as e:
            self.logger.log(f"[RISK] redundancy check after SL update failed: {e}", "WARNING")

    async def _handle_pending_entry_pivot(self, pivot: PivotPoint):
        """Handle new HL/LH during pending entry: cancel or finalize partial fills."""
        if not self.pending_entry:
            return
        need_cancel = (
            (self.pending_entry == "buy" and pivot.label == "HL")
            or (self.pending_entry == "sell" and pivot.label == "LH")
        )
        if need_cancel and pivot.close_time and self.pending_break_trigger_ts:
            try:
                pivot_ts = pivot.close_time.replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                pivot_ts = None
            if pivot_ts is not None and pivot_ts <= self.pending_break_trigger_ts:
                need_cancel = False
        if not need_cancel:
            return
        current_dir_qty = await self._get_directional_position(self.pending_entry, force=True)
        await self._cancel_pending_entry_orders()
        # 如果已持有该方向仓位（无论是否打满），视为入场；否则放弃本次信号
        if current_dir_qty <= Decimal("0"):
            await self._clear_pending_entry_state(clear_direction=True)
            return
        stop_price = self._compute_dynamic_stop(self.pending_entry) or self.dynamic_stop_price or pivot.price
        entry_price = self.pending_break_price or pivot.price
        await self._finalize_pending_entry(self.pending_entry, current_dir_qty, entry_price, stop_price)
        if self.enable_dynamic_sl:
            await self._refresh_stop_loss(force=True)

    async def _handle_pivot_event(self, pivot: PivotPoint, notify: bool = True):
        """Handle external ZigZag pivot: update pivots, dynamic SL, and slow reverse follow-up."""
        self._update_confirmed_pivots(pivot)
        await self._handle_pending_entry_pivot(pivot)

        if self.pending_reverse_state:
            handled = await self._process_slow_reverse_followup(pivot)
            if handled:
                return

        if self.enable_dynamic_sl:
            await self._refresh_stop_loss(force=True)

        if notify and self.enable_notifications:
            await self._notify_recent_pivots()

    async def _maybe_resume_pending_reverse_by_price(self, trade_price: Optional[Decimal]):
        """If pending slow reverse is invalidated by price reclaim/break, resume original direction early."""
        if self.pending_reverse_state != "waiting_next_pivot" or trade_price is None:
            return
        buffer = self.break_buffer_ticks * self.config.tick_size
        if self.pending_reverse_direction == "sell" and self.last_confirmed_high is not None:
            if trade_price >= (self.last_confirmed_high + buffer):
                await self._resume_after_invalid_reverse()
                if self.enable_dynamic_sl:
                    await self._refresh_stop_loss(force=True)
                if self.enable_notifications:
                    await self.send_notification("[REV-SLOW] Pending reverse canceled: price reclaimed last high")
                return
        if self.pending_reverse_direction == "buy" and self.last_confirmed_low is not None:
            if trade_price <= (self.last_confirmed_low - buffer):
                await self._resume_after_invalid_reverse()
                if self.enable_dynamic_sl:
                    await self._refresh_stop_loss(force=True)
                if self.enable_notifications:
                    await self.send_notification("[REV-SLOW] Pending reverse canceled: price broke last low")
                return

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
        self._set_direction_all(new_direction, mode=self.mode_tag)
        self.dynamic_stop_price = None
        self.dynamic_stop_direction = None
        self.last_open_order_time = 0
        self._invalidate_position_cache()
        self._invalidate_order_cache()
        # Ensure new direction can place orders
        self._set_stop_new_orders(False, mode=self.mode_tag)

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
        if self.pending_reverse_state == "waiting_next_pivot" and self.pending_reverse_direction == new_direction:
            return  # already waiting; avoid duplicate log/spam
        self.logger.log(f"[REV-SLOW] Observe next pivot for potential reverse to {new_direction.upper()} via ZigZag at {pivot_price}", "WARNING")
        self.pending_reverse_direction = new_direction
        self.pending_reverse_state = "waiting_next_pivot"
        self.pending_original_direction = self.config.direction
        self._set_stop_new_orders(True, mode=self.mode_tag)  # 暂停新开仓，保留现有 TP

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
        self._set_stop_new_orders(False, mode=self.mode_tag)

    async def _process_slow_reverse_followup(self, pivot: PivotPoint) -> bool:
        """Process next confirmed pivot for slow reverse logic. Returns True if handled."""
        if self.pending_reverse_state != "waiting_next_pivot":
            return False

        if self.pending_reverse_direction == "sell":
            if pivot.label == "LH":
                await self._cancel_all_orders_safely()
                self.pending_reverse_state = "unwinding"
                return True
            if pivot.label == "HH":
                await self._resume_after_invalid_reverse()
                return True

        if self.pending_reverse_direction == "buy":
            if pivot.label == "HL":
                await self._cancel_all_orders_safely()
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
                self._set_direction_all(self.pending_reverse_direction, lock=False, mode=self.mode_tag)
                self.direction_lock = None
                self.pending_reverse_direction = None
                self.pending_reverse_state = None
                self.pending_original_direction = None
                self._set_stop_new_orders(False, mode=self.mode_tag)
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
        try:
            result = await self.exchange_client.reduce_only_close_with_retry(pos_abs, close_side)
            self.last_release_attempt = time.time()
            # If exchange returns partial, log and let next loop continue closing
            try:
                remaining = abs(await self._get_position_signed_cached(force=True))
                if remaining >= self.min_order_size:
                    self.logger.log(f"[REV-SLOW] Partial close during reverse, remaining {remaining}", "WARNING")
            except Exception:
                pass
            if hasattr(result, "success") and not getattr(result, "success", True):
                self.logger.log(f"[REV-SLOW] Close during reverse reported failure: {getattr(result, 'error_message', '')}", "WARNING")
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
            # Prevent placing new opens beyond stop boundary
            dir_l = str(self.config.direction).lower()
            stop_threshold = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price is not None) else None
            if stop_threshold is not None:
                candidate_price = await self.exchange_client.get_order_price(self.config.direction)
                if self._price_breaches_stop(stop_threshold, dir_l, Decimal(candidate_price)):
                    self.logger.log(f"[OPEN] Skip new order: price {candidate_price} breaches stop {stop_threshold}", "WARNING")
                    return False
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
        if self.config.direction:
            self._set_direction_all(self.config.direction, lock=False, mode=self.mode_tag)

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

                self.logger.log(f"Current Position: {self._round_quantity(position_amt)} | Active closing amount: {self._round_quantity(active_close_amount)} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                # Check for position mismatch
                mismatch_detected = False
                # 基础风险：最大持仓限制（启用时）
                if self.enable_basic_risk and self.max_position_limit is not None and position_amt >= self.max_position_limit:
                    self._set_stop_new_orders(True, mode=self.mode_tag, reason="max_position_limit")
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

                if (not self.zigzag_timing_enabled) and self.enable_advanced_risk and self.stop_loss_enabled and equity is not None:
                    # Redundancy calculation for stop-new-orders gating
                    avg_price = mid_price
                    pos_detail = await self._get_position_detail_prefer_ws()
                    if pos_detail:
                        pos_signed_detail, avg_price_detail = pos_detail
                        if pos_signed_detail != 0:
                            avg_price = avg_price_detail

                stop_price = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price) else None
                if (
                    (not self.zigzag_timing_enabled)
                    and self.enable_advanced_risk
                    and self.stop_loss_enabled
                    and self.use_risk_exposure
                    and stop_price
                    and position_amt > 0
                ):
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
                        self._set_stop_new_orders(True, mode=self.mode_tag, reason="redundancy_insufficient")
                    if self.redundancy_insufficient_since is None:
                        self.redundancy_insufficient_since = time.time()
                    self.last_stop_new_notify = True
                else:
                    if self.stop_new_orders and self.enable_notifications and self.last_stop_new_notify:
                        await self.send_notification("[RISK] Resume new orders: redundancy restored")
                    self._set_stop_new_orders(False, mode=self.mode_tag)
                    self.redundancy_insufficient_since = None

                # Release liquidity: advanced uses release_timeout_minutes；基础风险也可用 basic_release_timeout_minutes
                should_release_advanced = (
                    (not self.zigzag_timing_enabled)
                    and self.enable_advanced_risk
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
                    now_ts = time.time()
                    if should_release_advanced:
                        interval = self.release_timeout_minutes
                        if now_ts - self.last_release_attempt_advanced > interval * 60:
                            self.last_release_attempt_advanced = now_ts
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
                                            )
                            except Exception as e:
                                self.logger.log(f"[RISK] Release attempt failed: {e}", "WARNING")
                    elif should_release_basic:
                        interval = self.basic_release_timeout_minutes
                        if now_ts - self.last_release_attempt_basic > interval * 60:
                            self.last_release_attempt_basic = now_ts
                            try:
                                release_qty = min(self.config.quantity, position_amt)
                                if release_qty > 0:
                                    close_side = self.config.close_order_side
                                    release_result = await self.exchange_client.reduce_only_close_with_retry(
                                        release_qty, close_side, timeout_sec=5.0, max_attempts=5
                                    )
                                    if release_result.success:
                                        self._invalidate_position_cache()
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
                                                f"[RISK-BASIC] Released {release_qty} after sustained full position"
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
            if not active_orders:
                return
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
                self._set_stop_new_orders(False, mode=self.mode_tag)
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
        self._set_stop_new_orders(True, mode=self.mode_tag, reason="webhook_stop")
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
            self._set_direction_all(self.webhook_target_direction, mode=self.mode_tag)
            self.webhook_reverse_pending = False
            self.webhook_block_trading = False
            self.webhook_stop_mode = None
            self.webhook_target_direction = None
            self.dynamic_stop_price = None
            self.dynamic_stop_direction = None
            self._set_stop_new_orders(False, mode=self.mode_tag)
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

        # If slow reverse is pending, allow price reclaim/break to invalidate without waiting next pivot
        if trade_price is not None:
            await self._maybe_resume_pending_reverse_by_price(Decimal(trade_price))

        if self.enable_dynamic_sl and self.dynamic_stop_price is not None and (not self.stop_loss_triggered):
            if self.config.direction == "buy" and best_bid <= self.dynamic_stop_price:
                stop_loss_triggered = True
            elif self.config.direction == "sell" and best_ask >= self.dynamic_stop_price:
                stop_loss_triggered = True
        # Immediate reverse on break of confirmed high/low (trade price +/- buffer ticks)
        if self.enable_zigzag and self.enable_auto_reverse and trade_price is not None:
            buffer = self.break_buffer_ticks * self.config.tick_size
            can_reverse = not (self.pending_reverse_state or self.pending_reverse_direction or self.reversing)
            if can_reverse and self.last_confirmed_high is not None and trade_price >= (self.last_confirmed_high + buffer):
                if self.config.direction == "sell":
                    await self._handle_reverse_signal("buy", Decimal(trade_price))
                    return stop_trading, pause_trading, stop_loss_triggered, best_bid, best_ask
            if can_reverse and self.last_confirmed_low is not None and trade_price <= (self.last_confirmed_low - buffer):
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
        prefix_parts = []
        if self.account_name:
            prefix_parts.append(self.account_name)
        if self.mode_tag:
            prefix_parts.append(self.mode_tag)
        if prefix_parts:
            message = f"[{'|'.join(prefix_parts)}] {message}"
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
        dir_val = self.config.direction.upper() if self.config.direction else "NONE"
        await self.send_notification(f"{prefix} Current direction {dir_val}")

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
                await self._backfill_recent_pivot_prices()
            if self.zigzag_timing_enabled:
                pos_signed = await self._sync_direction_lock(force=True)
                await self._realign_direction_state(trade_price=None, pos_signed=pos_signed, reason="startup_sync")

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self._round_quantity(self.config.quantity)}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Trading Mode: {self.trading_mode}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price if self.config.stop_price == -1 else self.exchange_client.round_to_tick(self.config.stop_price)}", "INFO")
            if self.enable_dynamic_sl:
                self.logger.log(f"Dynamic SL enabled", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price if self.config.pause_price == -1 else self.exchange_client.round_to_tick(self.config.pause_price)}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            if self.enable_notifications:
                await self.send_notification(f"[START] {self.config.exchange.upper()} {self.config.ticker} bot started.")
                await self._notify_direction()
            # Main trading loop
            while not self.shutdown_requested:
                try:
                    await self._maybe_send_daily_pnl()
                    if self.zigzag_timing_enabled:
                        await self._handle_zigzag_timing_cycle()
                        await asyncio.sleep(2)
                        continue
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
