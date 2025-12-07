"""Data feed services and cache helpers extracted from the monolith."""

import os
import time
import json
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Tuple, Any, Callable, Set

from helpers.logger import TradingLogger
from core.notifications import NotificationManager


class AsyncCache:
    """Lightweight async cache with TTL and optional debug logging."""

    def __init__(self, logger: TradingLogger, debug: bool = False):
        self._store: Dict[str, Tuple[float, float, Any]] = {}
        self.logger = logger
        self.debug = debug

    def invalidate(self, *keys: str):
        for key in keys:
            self._store.pop(key, None)
            if self.debug:
                self.logger.log(f"[CACHE] invalidate {key}", "DEBUG")

    async def get(self, key: str, ttl: float, fetcher: Callable[[], Any], force: bool = False):
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
        now = time.time()
        if key not in self._store:
            return None
        ts, ttl, val = self._store[key]
        if now - ts < max_age:
            return val
        return None

    def keys(self) -> List[str]:
        return list(self._store.keys())


class SharedBBOStore:
    """Shared BBO sidecar I/O with cleanup and throttling."""

    def __init__(
        self,
        logger: TradingLogger,
        cache: AsyncCache,
        shared_bbo_file: Optional[Path],
        shared_bbo_max_age: float,
        shared_bbo_cleanup_sec: float,
    ):
        self.logger = logger
        self.cache = cache
        self.shared_bbo_file = shared_bbo_file
        self.shared_bbo_max_age = shared_bbo_max_age
        self.shared_bbo_cleanup_sec = shared_bbo_cleanup_sec
        self._shared_bbo_last_write_ts: float = 0.0

    def _shared_bbo_key(self, exchange: str, contract_id: Optional[str], ticker: str) -> str:
        cid = contract_id
        contract_key = str(cid) if cid not in (None, "") else ticker
        return f"{exchange}:{contract_key}"

    def _save_shared_bbo_data(self, data: Dict[str, Any]):
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

    def read(self, exchange: str, contract_id: Optional[str], ticker: str) -> Optional[Tuple[Decimal, Decimal]]:
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

        key = self._shared_bbo_key(exchange, contract_id, ticker)
        entry = payload.get(key)
        result: Optional[Tuple[Decimal, Decimal]] = None
        ts_val = 0.0
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

    def write(self, exchange: str, contract_id: Optional[str], ticker: str, bid: Decimal, ask: Decimal):
        if not self.shared_bbo_file:
            return
        now_ts = time.time()
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

        key = self._shared_bbo_key(exchange, contract_id, ticker)
        existing = payload.get(key)
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

        payload[key] = {
            "bid": str(bid),
            "ask": str(ask),
            "ts": now_ts,
        }
        self._save_shared_bbo_data(payload)


@dataclass
class PivotEntry:
    label: str
    price: Optional[Decimal]
    close_time: datetime
    timeframe: str
    raw_ticker: str


class PivotFileWatcher:
    """Poll ZigZag pivot files with debounce and dedup."""

    def __init__(
        self,
        logger: TradingLogger,
        pivot_file: Path,
        exchange: str,
        ticker: str,
        timeframe: str,
        poll_interval: float = 60.0,
        debounce_ms: int = 150,
        cache_debug: bool = False,
    ):
        self.logger = logger
        self.pivot_file = self._resolve_path(pivot_file)
        self.exchange = exchange
        self.ticker = ticker
        self.timeframe = timeframe
        self.timeframe_key = self._normalize_timeframe_key(timeframe)
        self.symbol_base = self._extract_symbol_base(ticker)
        self.poll_interval = poll_interval
        self.debounce_ms = debounce_ms
        self.cache_debug = cache_debug
        self._pivot_file_mtime: float = 0.0
        self._last_pivot_poll: float = 0.0
        self._processed_keys: Set[Tuple[str, str]] = set()

    @staticmethod
    def _resolve_path(path: Path) -> Path:
        if path.is_absolute():
            return path
        return (Path(__file__).resolve().parent / path).resolve()

    @staticmethod
    def _parse_timeframe_to_seconds(tf: str) -> int:
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

    @staticmethod
    def _extract_symbol_base(ticker: str) -> str:
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

    @staticmethod
    def _normalize_timeframe_key(tf: str) -> str:
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

    @staticmethod
    def _parse_pivot_time(value: str) -> Optional[datetime]:
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

    def _load_json_file(self) -> Optional[Dict]:
        try:
            if not self.pivot_file.exists():
                return None
            try:
                self._pivot_file_mtime = self.pivot_file.stat().st_mtime
            except Exception:
                pass
            with open(self.pivot_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            self.logger.log(f"[WEBHOOK] Failed to load {self.pivot_file}: {exc}", "WARNING")
            return None

    def _load_entries(self) -> List[PivotEntry]:
        store = self._load_json_file() or {}
        base_bucket = store.get(self.symbol_base, {}) if isinstance(store, dict) else {}
        tf_bucket = base_bucket.get(self.timeframe_key) or base_bucket.get(str(self.timeframe_key))
        if not tf_bucket:
            return []
        entries: List[PivotEntry] = []
        for item in tf_bucket[-10:]:
            try:
                label = str(item.get("label")).upper()
                raw_ticker = item.get("raw_ticker") or item.get("ticker") or self.ticker
                tf_val = item.get("tf") or item.get("timeframe") or self.timeframe_key
                if self._normalize_timeframe_key(tf_val) != self.timeframe_key:
                    continue
                close_raw = item.get("close_time_utc") or item.get("pivot_bar_close") or item.get("close_time")
                close_dt = self._parse_pivot_time(str(close_raw)) if close_raw else None
                if not label or close_dt is None:
                    continue
                price_val = None
                exch_key = self.exchange
                if exch_key:
                    price_val = item.get(f"price_{exch_key}")
                if price_val is None and raw_ticker:
                    price_val = item.get(f"price_{raw_ticker}")
                if price_val is None:
                    price_val = item.get("price") or item.get("high") or item.get("low")
                price_dec = Decimal(str(price_val)) if price_val is not None else None
                entries.append(PivotEntry(label=label, price=price_dec, close_time=close_dt, timeframe=self.timeframe_key, raw_ticker=raw_ticker))
            except Exception:
                continue
        entries.sort(key=lambda x: x.close_time)
        return entries

    async def poll(self, force: bool = False) -> List[PivotEntry]:
        now = time.time()
        current_mtime = 0.0
        try:
            if self.pivot_file.exists():
                current_mtime = self.pivot_file.stat().st_mtime
        except Exception:
            current_mtime = 0.0
        if (not force) and (now - self._last_pivot_poll < self.poll_interval) and (current_mtime == self._pivot_file_mtime):
            return []
        self._last_pivot_poll = now
        if current_mtime and current_mtime != self._pivot_file_mtime:
            await asyncio.sleep(self.debounce_ms / 1000)
            try:
                current_mtime = self.pivot_file.stat().st_mtime
            except Exception:
                current_mtime = self._pivot_file_mtime
        entries = self._load_entries()
        new_entries: List[PivotEntry] = []
        for entry in entries:
            key = (entry.close_time.isoformat(), entry.label)
            if key in self._processed_keys:
                continue
            self._processed_keys.add(key)
            new_entries.append(entry)
        return new_entries


class DataFeedManager:
    def __init__(
        self,
        logger: TradingLogger,
        cache: AsyncCache,
        exchange_client,
        config,
        shared_bbo: SharedBBOStore,
        *,
        mode_tag: str = "default",
        ttl_bbo: float = 0.3,
        ttl_bbo_cache: float = 0.5,
        bbo_force_timeout: float = 0.15,
        ttl_last_price: float = 0.5,
        zigzag_bbo_min_interval: float = 1.0,
    ):
        self.logger = logger
        self.cache = cache
        self.exchange_client = exchange_client
        self.config = config
        self.mode_tag = mode_tag or "default"
        self.shared_bbo = shared_bbo
        self.ttl_bbo = ttl_bbo
        self.ttl_bbo_cache = ttl_bbo_cache
        self.bbo_force_timeout = bbo_force_timeout
        self.ttl_last_price = ttl_last_price
        self.zigzag_bbo_min_interval = zigzag_bbo_min_interval
        self._zigzag_last_bbo_ts: float = 0.0

    def _cache_key(self, suffix: str) -> str:
        contract_id = getattr(self.config, "contract_id", None) or getattr(self.config, "ticker", "")
        exchange = getattr(self.config, "exchange", "")
        return f"{exchange}:{contract_id}:{self.mode_tag}:{suffix}"

    async def _fetch_ws_bbo(self) -> Optional[Tuple[Decimal, Decimal]]:
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

    async def get_bbo_cached(self, force: bool = False) -> Tuple[Decimal, Decimal]:
        key = self._cache_key("bbo")

        async def fetch_bbo():
            return await self.exchange_client.fetch_bbo_prices(self.config.contract_id)

        shared = self.shared_bbo.read(self.config.exchange, self.config.contract_id, getattr(self.config, "ticker", ""))
        if shared:
            return shared

        ws_bbo = await self._fetch_ws_bbo()
        if ws_bbo:
            self.shared_bbo.write(self.config.exchange, self.config.contract_id, getattr(self.config, "ticker", ""), ws_bbo[0], ws_bbo[1])
            return ws_bbo

        if force:
            try:
                val = await asyncio.wait_for(fetch_bbo(), timeout=self.bbo_force_timeout)
                self.shared_bbo.write(self.config.exchange, self.config.contract_id, getattr(self.config, "ticker", ""), val[0], val[1])
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
        prices = await self.cache.get(key, self.ttl_bbo, fetch_bbo, force=False)
        self.shared_bbo.write(self.config.exchange, self.config.contract_id, getattr(self.config, "ticker", ""), prices[0], prices[1])
        return prices

    async def get_bbo_for_zigzag(self, force: bool = False) -> Tuple[Decimal, Decimal]:
        now = time.time()
        key = self._cache_key("bbo")
        if (not force) and (now - self._zigzag_last_bbo_ts < self.zigzag_bbo_min_interval):
            cached = self.cache.peek(key, self.ttl_bbo_cache)
            if cached:
                return cached
        prices = await self.get_bbo_cached(force=force)
        self._zigzag_last_bbo_ts = time.time()
        return prices

    async def _get_ws_last_price(self) -> Optional[Decimal]:
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

    async def get_last_trade_price_cached(self, best_bid: Optional[Decimal] = None, best_ask: Optional[Decimal] = None, force: bool = False) -> Optional[Decimal]:
        key = self._cache_key("last_price")

        async def _fetch():
            ws_last = await self._get_ws_last_price()
            if ws_last is not None:
                return ws_last
            if best_bid is not None and best_ask is not None:
                try:
                    return (Decimal(best_bid) + Decimal(best_ask)) / 2
                except Exception:
                    pass
            try:
                bid, ask = await self.get_bbo_cached(force=force)
                return (bid + ask) / 2
            except Exception as exc:
                self.logger.log(f"[PRICE] Fallback trade price failed: {exc}", "WARNING")
                return None

        return await self.cache.get(key, self.ttl_last_price, _fetch, force=force)


class CoreServices:
    """Container for core services used by strategies and orchestrator."""

    def __init__(
        self,
        logger: TradingLogger,
        config,
        exchange_client,
        *,
        mode_tag: str = "default",
        cache_debug: bool = False,
        shared_bbo_file: Optional[Path] = None,
        shared_bbo_max_age: float = 1.5,
        shared_bbo_cleanup_sec: float = 30.0,
    ):
        self.logger = logger
        self.config = config
        self.exchange_client = exchange_client
        self.mode_tag = mode_tag or "default"
        self.cache = AsyncCache(logger, debug=cache_debug)

        shared_file = shared_bbo_file
        env_path = os.getenv("SHARED_BBO_FILE", "").strip()
        if not shared_file and env_path:
            shared_file = Path(env_path).expanduser().resolve()

        self.shared_bbo = SharedBBOStore(
            logger,
            self.cache,
            shared_file,
            shared_bbo_max_age=shared_bbo_max_age,
            shared_bbo_cleanup_sec=shared_bbo_cleanup_sec,
        )

        self.data_feeds = DataFeedManager(
            logger=logger,
            cache=self.cache,
            exchange_client=exchange_client,
            config=config,
            shared_bbo=self.shared_bbo,
            mode_tag=self.mode_tag,
            ttl_bbo=float(os.getenv("TTL_BBO_SEC", "0.3")),
            ttl_bbo_cache=float(os.getenv("BBO_CACHE_TTL_S", "0.5")),
            bbo_force_timeout=float(os.getenv("BBO_FORCE_TIMEOUT_MS", "150")) / 1000.0,
            ttl_last_price=float(os.getenv("TTL_LAST_PRICE_SEC", "0.5")),
            zigzag_bbo_min_interval=float(os.getenv("ZIGZAG_BBO_MIN_INTERVAL_SEC", "1.0")),
        )

        pivot_file_str = getattr(config, "zigzag_pivot_file", None) or os.getenv("ZIGZAG_PIVOT_FILE", "zigzag_pivots.json")
        pivot_file = Path(str(pivot_file_str)) if pivot_file_str else Path("zigzag_pivots.json")
        tf = getattr(config, "zigzag_timeframe", None) or os.getenv("ZIGZAG_TIMEFRAME", "1m")
        default_interval = float(PivotFileWatcher._parse_timeframe_to_seconds(tf))
        poll_interval = float(os.getenv("PIVOT_POLL_INTERVAL_SEC", str(int(default_interval))))
        debounce_ms = int(os.getenv("PIVOT_DEBOUNCE_MS", "150"))

        self.pivot_watcher = PivotFileWatcher(
            logger=logger,
            pivot_file=pivot_file,
            exchange=getattr(config, "exchange", ""),
            ticker=getattr(config, "ticker", ""),
            timeframe=tf,
            poll_interval=poll_interval,
            debounce_ms=debounce_ms,
            cache_debug=cache_debug,
        )

        enable_notifications_cfg = getattr(config, "enable_notifications", None)
        enable_notifications = (
            str(enable_notifications_cfg).lower() == "true"
            if enable_notifications_cfg is not None
            else str(os.getenv("ENABLE_NOTIFICATIONS", "false")).lower() == "true"
        )
        daily_pnl_cfg = getattr(config, "daily_pnl_report", None)
        daily_pnl_report = (
            str(daily_pnl_cfg).lower() == "true"
            if daily_pnl_cfg is not None
            else str(os.getenv("DAILY_PNL_REPORT", "false")).lower() == "true"
        )
        account_name = os.getenv("ACCOUNT_NAME", "").strip()
        self.notifications = NotificationManager(
            logger=logger,
            account_name=account_name,
            mode_tag=self.mode_tag,
            enable_notifications=enable_notifications,
            daily_pnl_report=daily_pnl_report,
        )