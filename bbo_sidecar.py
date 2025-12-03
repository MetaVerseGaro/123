"""
Lightweight BBO sidecar.

Launch this as a separate process to share WS-driven BBO with trading_bot instances
via the SHARED_BBO_FILE JSON cache. Minimal REST is used as a fallback when WS lacks data.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any
import dotenv

from exchanges import ExchangeFactory
from trading_bot import TradingConfig


def _log(msg: str):
    print(f"[BBO-SIDECAR] {msg}")


class SharedBBO:
    def __init__(self, path: Path, key: str, max_age: float = 1.5, cleanup_sec: float = 30.0):
        self.path = path
        self.key = key
        self.max_age = max_age
        self.cleanup_sec = cleanup_sec
        self._last_write = 0.0

    def _save(self, data: Dict[str, Any]):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            tmp.replace(self.path)
        except Exception as exc:
            try:
                tmp.unlink(missing_ok=True)  # best-effort cleanup
            except Exception:
                pass
            raise RuntimeError(f"failed to write shared BBO file: {exc}") from exc

    def write(self, bid: Decimal, ask: Decimal):
        now_ts = time.time()
        # Simple throttle: avoid excessive disk writes
        if (now_ts - self._last_write) < max(0.1, self.max_age / 2):
            return
        self._last_write = now_ts

        payload: Dict[str, Any] = {}
        try:
            if self.path.exists():
                with open(self.path, "r", encoding="utf-8") as f:
                    payload = json.load(f) or {}
        except Exception as exc:
            _log(f"read shared BBO failed, recreating file: {exc}")
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        cutoff = now_ts - self.cleanup_sec
        stale = [k for k, v in payload.items() if not isinstance(v, dict) or float(v.get("ts", 0) or 0) < cutoff]
        for k in stale:
            payload.pop(k, None)

        existing = payload.get(self.key)
        try:
            if isinstance(existing, dict):
                prev_ts = float(existing.get("ts", 0) or 0)
                prev_bid = Decimal(str(existing.get("bid")))
                prev_ask = Decimal(str(existing.get("ask")))
                # Skip redundant rewrites when the book is unchanged and still fresh to keep disk I/O low on t3.micro
                if (
                    prev_bid == bid
                    and prev_ask == ask
                    and (now_ts - prev_ts) < (self.max_age / 2)
                ):
                    return
        except Exception:
            pass

        payload[self.key] = {"bid": str(bid), "ask": str(ask), "ts": now_ts}
        try:
            self._save(payload)
        except Exception as exc:
            _log(str(exc))


def _load_config_file(path: Path) -> dict:
    """Load JSON with inline # comments stripped (compatible with botA.json style)."""
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except Exception as exc:
        raise RuntimeError(f"failed to read config {path}: {exc}") from exc
    cleaned = []
    for line in raw_lines:
        in_str = False
        new_line = ""
        for ch in line:
            if ch == '"' and (not new_line or new_line[-1] != '\\'):
                in_str = not in_str
            if ch == '#' and not in_str:
                break
            new_line += ch
        cleaned.append(new_line)
    try:
        return json.loads("\n".join(cleaned))
    except Exception as exc:
        raise RuntimeError(f"failed to parse config {path}: {exc}") from exc


def _load_sidecar_settings() -> Dict[str, Any]:
    """
    Resolve sidecar settings with the following priority:
    1) bot JSON (default botA.json or SIDECAR_CONFIG env) + its env_file for API 密钥.
    2) Fallback to direct env vars (EXCHANGE/TICKER/SHARED_BBO_FILE/...).
    """
    cfg_path = Path(os.getenv("SIDECAR_CONFIG", "botA.json")).expanduser().resolve()
    cfg: Dict[str, Any] = {}
    if cfg_path.exists():
        try:
            cfg = _load_config_file(cfg_path)
            env_file = cfg.get("env_file")
            if env_file:
                dotenv.load_dotenv(Path(env_file).expanduser())
            for k, v in cfg.get("env", {}).items():
                os.environ[str(k)] = str(v)
        except Exception as exc:
            _log(f"config load failed, fall back to env: {exc}")
            cfg = {}

    sidecar_cfg = cfg.get("sidecar", {}) if isinstance(cfg, dict) else {}
    trading_cfg = cfg.get("trading", {}) if isinstance(cfg, dict) else {}

    def pick(*candidates, default=None):
        for c in candidates:
            if c is not None and c != "":
                return c
        return default

    exchange = pick(sidecar_cfg.get("exchange"), cfg.get("exchange"), os.getenv("EXCHANGE"))
    ticker = pick(sidecar_cfg.get("ticker"), trading_cfg.get("ticker"), os.getenv("TICKER"))
    shared_file = pick(sidecar_cfg.get("shared_bbo_file"), os.getenv("SHARED_BBO_FILE"))
    max_age = float(pick(sidecar_cfg.get("shared_bbo_max_age_sec"), os.getenv("SHARED_BBO_MAX_AGE_SEC"), 1.5))
    cleanup = float(pick(sidecar_cfg.get("shared_bbo_cleanup_sec"), os.getenv("SHARED_BBO_CLEANUP_SEC"), 30))
    publish_interval = float(pick(sidecar_cfg.get("publish_interval_sec"), os.getenv("BBO_PUBLISH_INTERVAL_SEC"), 0.25))
    rest_interval = float(pick(sidecar_cfg.get("rest_interval_sec"), os.getenv("BBO_SIDECAR_REST_INTERVAL_SEC"), 5))

    return {
        "exchange": exchange,
        "ticker": ticker,
        "shared_file": shared_file,
        "max_age": max_age,
        "cleanup": cleanup,
        "publish_interval": publish_interval,
        "rest_interval": rest_interval,
    }


async def run_sidecar():
    settings = _load_sidecar_settings()
    exchange = settings.get("exchange")
    ticker = settings.get("ticker")
    shared_file = settings.get("shared_file")
    if not (exchange and ticker and shared_file):
        print("Usage: set EXCHANGE, TICKER, SHARED_BBO_FILE (or provide botA.json with sidecar section)")
        return

    # Minimal config just to spin up the exchange client
    cfg = TradingConfig(
        ticker=ticker,
        contract_id="",
        quantity=Decimal("0"),
        take_profit=Decimal("0"),
        tick_size=Decimal("0.01"),
        direction="buy",
        max_orders=1,
        wait_time=1,
        exchange=exchange,
        grid_step=Decimal("0"),
        stop_price=Decimal("-1"),
        pause_price=Decimal("-1"),
        boost_mode=False,
    )

    client = ExchangeFactory.create_exchange(exchange, cfg)
    try:
        cfg.contract_id, cfg.tick_size = await client.get_contract_attributes()
    except Exception as exc:
        _log(f"failed to load contract attributes: {exc}")
        return

    try:
        await client.connect()
    except Exception as exc:
        _log(f"failed to connect to exchange WS: {exc}")
        return
    await asyncio.sleep(2)  # let WS warm up

    contract_key = cfg.contract_id if cfg.contract_id not in ("", None) else cfg.ticker
    shared = SharedBBO(
        Path(shared_file).expanduser().resolve(),
        key=f"{exchange}:{contract_key}",
        max_age=settings["max_age"],
        cleanup_sec=settings["cleanup"],
    )

    # Defaults tuned for AWS t3.micro: moderate publish cadence, REST fallback only when WS is stale/missing
    publish_interval = settings["publish_interval"]
    rest_interval = settings["rest_interval"]
    last_rest = 0.0
    last_ws_change = 0.0
    last_ws_bid = None
    last_ws_ask = None

    _log(f"started for {exchange} {ticker} (contract_id={cfg.contract_id}), writing to {shared_file}")
    try:
        while True:
            bid_val = None
            ask_val = None
            ws_mgr = getattr(client, "ws_manager", None)
            if ws_mgr:
                bid_val = getattr(ws_mgr, "best_bid", None)
                ask_val = getattr(ws_mgr, "best_ask", None)
                # Prefer a WS timestamp hint when available; otherwise fall back to change detection
                ts_hint = None
                for attr in ("last_bbo_ts", "last_book_ts", "last_update_ts"):
                    ts_hint = getattr(ws_mgr, attr, None)
                    if ts_hint:
                        break
                if ts_hint:
                    try:
                        ts_val = float(ts_hint)
                        if ts_val > 1e12:  # ms -> s
                            ts_val = ts_val / 1000.0
                        last_ws_change = ts_val
                    except Exception:
                        pass

            now_ts = time.time()

            ws_valid = False
            try:
                bid = Decimal(str(bid_val)) if bid_val is not None else None
                ask = Decimal(str(ask_val)) if ask_val is not None else None
                if bid is not None and ask is not None and bid > 0 and ask > 0 and bid < ask:
                    ws_valid = True
            except Exception:
                bid = ask = None
                ws_valid = False

            if ws_valid:
                if bid != last_ws_bid or ask != last_ws_ask:
                    last_ws_change = now_ts
                    last_ws_bid, last_ws_ask = bid, ask
            ws_stale = False
            if last_ws_change > 0:
                ws_stale = (now_ts - last_ws_change) > max(shared.max_age * 4, rest_interval * 2)
            else:
                ws_stale = True

            # REST fallback only when WS is missing/stale
            need_rest = (not ws_valid) or ws_stale
            if need_rest and (now_ts - last_rest) >= rest_interval:
                try:
                    bid_rest, ask_rest = await client.fetch_bbo_prices(cfg.contract_id)
                    bid = Decimal(str(bid_rest))
                    ask = Decimal(str(ask_rest))
                    ws_valid = bid > 0 and ask > 0 and bid < ask
                    if ws_valid:
                        last_rest = now_ts
                        last_ws_change = now_ts  # reset staleness so we do not hammer REST
                        last_ws_bid, last_ws_ask = bid, ask
                    else:
                        need_rest = True
                except Exception as exc:
                    _log(f"REST fallback failed: {exc}")
                    bid = ask = None
                    last_rest = now_ts

            if ws_valid:
                shared.write(bid, ask)
            await asyncio.sleep(publish_interval)
    except KeyboardInterrupt:
        _log("stopped by user")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(run_sidecar())
    except KeyboardInterrupt:
        sys.exit(0)
