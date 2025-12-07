import asyncio
import json
import time
from decimal import Decimal
from pathlib import Path

import pytest

from core.data_feeds import AsyncCache, SharedBBOStore, PivotFileWatcher
from core.notifications import NotificationManager


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message: str, level: str = "INFO"):
        self.messages.append((level, message))


def test_async_cache_respects_ttl():
    async def _run():
        logger = DummyLogger()
        cache = AsyncCache(logger, debug=True)

        call_count = 0

        async def fetch():
            nonlocal call_count
            call_count += 1
            return call_count

        first = await cache.get("k", 0.05, fetch)
        second = await cache.get("k", 0.05, fetch)
        assert first == second == 1

        await asyncio.sleep(0.06)

        third = await cache.get("k", 0.05, fetch)
        assert third == 2
        assert call_count == 2

    asyncio.run(_run())


def test_shared_bbo_store_read_write_and_cleanup(tmp_path: Path):
    async def _run():
        logger = DummyLogger()
        cache = AsyncCache(logger, debug=True)
        store = SharedBBOStore(
            logger,
            cache,
            tmp_path / "bbo.json",
            shared_bbo_max_age=0.2,
            shared_bbo_cleanup_sec=0.1,
        )

        store.write("ex", "cid", "TICK", Decimal("1.0"), Decimal("1.1"))
        fresh = store.read("ex", "cid", "TICK")
        assert fresh == (Decimal("1.0"), Decimal("1.1"))

        stale_payload = json.loads((tmp_path / "bbo.json").read_text())
        for key in list(stale_payload.keys()):
            stale_payload[key]["ts"] = time.time() - 1.0
        (tmp_path / "bbo.json").write_text(json.dumps(stale_payload))

        stale = store.read("ex", "cid", "TICK")
        assert stale is None

    asyncio.run(_run())


def test_pivot_file_watcher_detects_new_entries(tmp_path: Path):
    async def _run():
        logger = DummyLogger()
        pivot_file = tmp_path / "pivots.json"
        pivot_store = {
            "BTC": {
                "1": [
                    {"label": "HH", "close_time_utc": "2024-01-01T00:00:00Z", "price": "100"}
                ]
            }
        }
        pivot_file.write_text(json.dumps(pivot_store))

        watcher = PivotFileWatcher(
            logger=logger,
            pivot_file=pivot_file,
            exchange="binance",
            ticker="BTCUSDT",
            timeframe="1m",
            poll_interval=0,
            debounce_ms=0,
            cache_debug=True,
        )

        entries = await watcher.poll(force=True)
        assert len(entries) == 1
        assert entries[0].label == "HH"

        again = await watcher.poll(force=True)
        assert again == []

        pivot_store["BTC"]["1"].append({"label": "LL", "close_time_utc": "2024-01-01T00:01:00Z", "price": "90"})
        pivot_file.write_text(json.dumps(pivot_store))

        new_entries = await watcher.poll(force=True)
        assert [e.label for e in new_entries] == ["LL"]

    asyncio.run(_run())


def test_notification_error_dedup(monkeypatch):
    async def _run():
        monkeypatch.delenv("LARK_TOKEN", raising=False)
        monkeypatch.delenv("LARK_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        logger = DummyLogger()
        manager = NotificationManager(logger, "acct", "mode", True, False)

        await manager.notify_error_once("boom", dedup_seconds=300)
        first_ts = manager.last_error_notified_ts

        await manager.notify_error_once("boom", dedup_seconds=300)
        second_ts = manager.last_error_notified_ts
        assert second_ts == first_ts

        await manager.notify_error_once("boom", dedup_seconds=0)
        third_ts = manager.last_error_notified_ts
        assert third_ts >= second_ts

    asyncio.run(_run())
