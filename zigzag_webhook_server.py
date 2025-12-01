"""
Webhook server for ingesting TradingView ZigZag pivots and basic risk directions.

- Accepts ZigZag pivot messages (label/ticker/timeframe/close_time_utc) and keeps
  the latest 10 per (base, timeframe) both in memory and on disk.
- Accepts basic risk direction messages (buy/sell) per base symbol and persists
  the latest entry.
- Logs to logs/zigzag_webhook_YYYY-MM-DD.log (configurable) and removes logs
  older than 7 days on startup.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from aiohttp import web


BASE_DIR = Path(__file__).resolve().parent


def resolve_path(path_str: str) -> Path:
    """Resolve relative paths against project root."""
    path = Path(path_str)
    if not path.is_absolute():
        return (BASE_DIR / path).resolve()
    return path


def setup_logging() -> None:
    """Configure file logging with daily files and 7-day cleanup."""
    log_dir = resolve_path(os.getenv("WEBHOOK_LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    retention_days = int(os.getenv("WEBHOOK_LOG_RETENTION_DAYS", "7"))

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    for file in log_dir.glob("zigzag_webhook_*.log"):
        try:
            mtime = datetime.fromtimestamp(file.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                file.unlink()
        except Exception:
            pass

    log_file = log_dir / f"zigzag_webhook_{datetime.utcnow():%Y-%m-%d}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def extract_base_symbol(ticker: str) -> str:
    """Extract base symbol: strip common quote suffixes like USDT/USD/USDC after letter run."""
    letters = []
    for ch in ticker:
        if ch.isalpha():
            letters.append(ch)
        else:
            break
    candidate = "".join(letters) or ticker
    for quote in ("USDT", "USD", "USDC", "PERP"):
        if candidate.upper().endswith(quote) and len(candidate) > len(quote):
            return candidate.upper()[:-len(quote)]
    return candidate.upper()


def parse_utc_time(value: str) -> datetime:
    """Parse various UTC time formats and return an aware datetime."""
    value = value.strip()
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as exc:
        raise ValueError(f"Invalid time format: {value}") from exc


def normalize_timeframe(tf: Any) -> str:
    """Normalize timeframe to string (minutes)."""
    if tf is None:
        raise ValueError("Missing timeframe")
    if isinstance(tf, (int, float)):
        return str(int(tf))
    tf_str = str(tf).strip().lower()
    if tf_str.endswith("m"):
        return tf_str[:-1] or "0"
    if tf_str.endswith("h"):
        try:
            return str(int(tf_str[:-1]) * 60)
        except Exception as exc:
            raise ValueError(f"Invalid timeframe: {tf}") from exc
    return tf_str


def ensure_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class PivotStore:
    """In-memory + file persistence for ZigZag pivots."""

    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Dict[str, list]] = {}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {}

    async def add_pivot(
        self,
        base: str,
        timeframe: str,
        label: str,
        raw_ticker: str,
        close_time_utc: datetime,
    ) -> None:
        entry = {
            "label": label.upper(),
            "raw_ticker": raw_ticker,
            "tf": timeframe,
            "close_time_utc": ensure_iso(close_time_utc),
        }
        async with self._lock:
            if base not in self.data:
                self.data[base] = {}
            if timeframe not in self.data[base]:
                self.data[base][timeframe] = []

            bucket = self.data[base][timeframe]
            dedup_key = (entry["label"], entry["close_time_utc"])
            bucket = [
                x
                for x in bucket
                if (x.get("label"), x.get("close_time_utc")) != dedup_key
            ]
            bucket.append(entry)
            if len(bucket) > 10:
                bucket = bucket[-10:]
            self.data[base][timeframe] = bucket
            await self._persist()

    async def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        tmp.replace(self.path)


class DirectionStore:
    """Persist last basic direction per base symbol."""

    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Dict[str, str]] = {}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {}

    async def set_direction(self, base: str, direction: str) -> None:
        entry = {
            "direction": direction.lower(),
            "updated_at": ensure_iso(datetime.utcnow().replace(tzinfo=timezone.utc)),
        }
        async with self._lock:
            self.data[base] = entry
            await self._persist()

    async def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        tmp.replace(self.path)


def parse_pivot_from_text(text: str) -> Optional[Tuple[str, str, str, datetime]]:
    """Parse pivot payload from plain text like 'ZigZag HL | ETHUSDT.P | TF=1 | pivot bar close (UTC)=2025-11-28 18:05'."""
    label_match = re.search(r"ZigZag\s+(HH|HL|LH|LL)", text, re.IGNORECASE)
    tf_match = re.search(r"TF\s*=\s*([0-9]+)", text, re.IGNORECASE)
    time_match = re.search(r"pivot\s+bar\s+close\s*\(UTC\)\s*=\s*([^|]+)", text, re.IGNORECASE)
    parts = [p.strip() for p in text.split("|") if p.strip()]
    raw_ticker = None
    for part in parts:
        if "." in part or part.isalpha():
            raw_ticker = part
            break
    if not (label_match and tf_match and time_match and raw_ticker):
        return None
    label = label_match.group(1).upper()
    timeframe = normalize_timeframe(tf_match.group(1))
    close_time = parse_utc_time(time_match.group(1))
    return label, raw_ticker, timeframe, close_time


def parse_direction_from_text(text: str) -> Optional[Tuple[str, str]]:
    """Parse direction payload from plain text like 'buy | ETHUSDT.P'."""
    tokens = [t.strip() for t in text.split("|") if t.strip()]
    if not tokens:
        return None
    direction = tokens[0].lower()
    if direction not in ("buy", "sell"):
        return None
    raw_ticker = tokens[1] if len(tokens) > 1 else None
    return direction, raw_ticker


async def handle_webhook(request: web.Request) -> web.Response:
    """Handle incoming webhook requests."""
    app = request.app
    pivot_store: PivotStore = app["pivot_store"]
    direction_store: DirectionStore = app["direction_store"]

    raw_text = ""
    payload: Dict[str, Any] = {}
    try:
        payload = await request.json()
    except Exception:
        try:
            raw_text = await request.text()
        except Exception:
            raw_text = ""

    event_type = payload.get("type") if isinstance(payload, dict) else None
    logger = logging.getLogger(__name__)

    # Pivot handling
    if (isinstance(payload, dict) and payload.get("label")) or (
        isinstance(payload, dict) and event_type in ("pivot", "zigzag")
    ):
        try:
            label = str(payload.get("label")).upper()
            raw_ticker = payload.get("ticker") or payload.get("raw_ticker")
            if not raw_ticker:
                raise ValueError("Missing ticker/raw_ticker")
            timeframe = normalize_timeframe(
                payload.get("tf")
                or payload.get("timeframe")
                or payload.get("resolution")
                or payload.get("tf_minutes")
            )
            close_time_raw = payload.get("close_time_utc") or payload.get(
                "pivot_bar_close"
            )
            if not close_time_raw:
                raise ValueError("Missing close_time_utc")
            close_time = parse_utc_time(str(close_time_raw))
            base = extract_base_symbol(raw_ticker)
            await pivot_store.add_pivot(base, timeframe, label, raw_ticker, close_time)
            logger.info(
                "pivot stored | base=%s | tf=%s | label=%s | close=%s | raw=%s",
                base,
                timeframe,
                label,
                ensure_iso(close_time),
                raw_ticker,
            )
            return web.json_response(
                {"status": "ok", "type": "pivot", "base": base, "timeframe": timeframe}
            )
        except Exception as exc:
            logger.error("pivot parse failed: %s", exc)
            return web.json_response({"error": str(exc)}, status=400)

    if raw_text:
        pivot = parse_pivot_from_text(raw_text)
        if pivot:
            label, raw_ticker, timeframe, close_time = pivot
            base = extract_base_symbol(raw_ticker)
            await pivot_store.add_pivot(base, timeframe, label, raw_ticker, close_time)
            logger.info(
                "pivot stored | base=%s | tf=%s | label=%s | close=%s | raw=%s",
                base,
                timeframe,
                label,
                ensure_iso(close_time),
                raw_ticker,
            )
            return web.json_response(
                {"status": "ok", "type": "pivot", "base": base, "timeframe": timeframe}
            )

    # Direction handling
    if isinstance(payload, dict) and payload.get("direction"):
        try:
            direction = str(payload.get("direction")).lower()
            if direction not in ("buy", "sell"):
                raise ValueError("direction must be buy/sell")
            raw_ticker = payload.get("ticker") or payload.get("raw_ticker")
            if not raw_ticker:
                raise ValueError("Missing ticker/raw_ticker")
            base = extract_base_symbol(raw_ticker)
            await direction_store.set_direction(base, direction)
            logger.info(
                "direction stored | base=%s | direction=%s", base, direction.upper()
            )
            return web.json_response(
                {"status": "ok", "type": "direction", "base": base, "direction": direction}
            )
        except Exception as exc:
            logger.error("direction parse failed: %s", exc)
            return web.json_response({"error": str(exc)}, status=400)

    if raw_text:
        direction = parse_direction_from_text(raw_text)
        if direction:
            direction_value, raw_ticker = direction
            if not raw_ticker:
                return web.json_response(
                    {"error": "Missing ticker in direction payload"}, status=400
                )
            base = extract_base_symbol(raw_ticker)
            await direction_store.set_direction(base, direction_value)
            logger.info(
                "direction stored | base=%s | direction=%s",
                base,
                direction_value.upper(),
            )
            return web.json_response(
                {
                    "status": "ok",
                    "type": "direction",
                    "base": base,
                    "direction": direction_value,
                }
            )

    logger.warning("unrecognized payload: %s | %s", payload, raw_text)
    return web.json_response({"error": "Unrecognized payload"}, status=400)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


def create_app() -> web.Application:
    setup_logging()
    pivot_path = resolve_path(os.getenv("ZIGZAG_PIVOT_FILE", "zigzag_pivots.json"))
    direction_path = resolve_path(
        os.getenv("WEBHOOK_BASIC_DIRECTION_FILE", "webhook_basic_direction.json")
    )
    app = web.Application()
    app["pivot_store"] = PivotStore(pivot_path)
    app["direction_store"] = DirectionStore(direction_path)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/health", handle_health)
    return app


def main() -> None:
    app = create_app()
    host = os.getenv("WEBHOOK_HOST", "0.0.0.0")
    # Default 8080 for non-privileged runs; override with WEBHOOK_PORT (e.g., 80/443 behind sudo或反代)
    port = int(os.getenv("WEBHOOK_PORT", "8080"))
    logging.getLogger(__name__).info("Starting webhook server on %s:%s", host, port)
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()
