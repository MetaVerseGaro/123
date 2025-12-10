"""Parity harness for ZigZagTimingStrategy using deterministic fixture."""

import asyncio
import json
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import List

# Ensure project root on sys.path so imports like `core` resolve when running pytest from anywhere
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import AsyncCache
from core.data_feeds import PivotEntry
from exchanges.base import OrderResult
from strategies.zigzag_timing import ZigZagTimingStrategy


class DummyLogger:
    def __init__(self):
        self.messages: List[str] = []

    def log(self, message: str, level: str = "INFO"):
        self.messages.append(f"[{level}] {message}")


class DummyNotifications:
    def __init__(self):
        self.errors: List[str] = []
        self.sent: List[str] = []

    async def send_notification(self, message: str, level: str = "INFO"):
        self.sent.append(message)

    async def notify_error_once(self, message: str, dedup_seconds: int = 300):
        self.errors.append(message)


class DummyDataFeeds:
    def __init__(self, bbo_pairs):
        self._pairs = list(bbo_pairs)
        self._iter = iter(self._pairs)

    async def get_bbo_for_zigzag(self, force: bool = False):
        try:
            bid, ask = next(self._iter)
        except StopIteration:
            bid, ask = self._pairs[-1]
        return Decimal(str(bid)), Decimal(str(ask))

    async def get_last_trade_price_cached(self, best_bid=None, best_ask=None, force: bool = False):
        if best_bid is None or best_ask is None:
            bid, ask = await self.get_bbo_for_zigzag(force=force)
            return (bid + ask) / 2
        return (Decimal(best_bid) + Decimal(best_ask)) / 2


class DummyPivotWatcher:
    def __init__(self, entries: List[PivotEntry]):
        self.entries = entries
        self._sent = False

    async def poll(self, force: bool = False):
        if self._sent:
            return []
        self._sent = True
        return self.entries


class DummyExchange:
    def __init__(self):
        self.positions = Decimal("0")
        self.orders: List[OrderResult] = []

    async def get_account_positions(self):
        return self.positions

    async def get_account_equity(self):
        return Decimal("1000")

    async def get_available_balance(self):
        return Decimal("1000")

    def round_to_tick(self, price):
        return Decimal(price).quantize(Decimal("0.0001"))

    async def place_post_only_order(self, contract_id, quantity, price, side, reduce_only: bool = False):
        qty = Decimal(quantity)
        px = Decimal(price)
        if side == "buy":
            self.positions += qty
        else:
            self.positions -= qty
        order = OrderResult(success=True, order_id=f"po-{len(self.orders)+1}", side=side, size=qty, price=px, status="FILLED", filled_size=qty)
        self.orders.append(order)
        return order

    async def place_limit_order(self, contract_id, quantity, price, side, reduce_only: bool = False):
        return await self.place_post_only_order(contract_id, quantity, price, side, reduce_only=reduce_only)

    async def place_open_order(self, contract_id, quantity, side):
        return await self.place_post_only_order(contract_id, quantity, Decimal("0"), side, reduce_only=False)

    async def place_close_order(self, contract_id, quantity, price, side):
        return await self.place_post_only_order(contract_id, quantity, price, side, reduce_only=True)

    async def cancel_order(self, order_id):
        self.orders = [o for o in self.orders if o.order_id != order_id]
        return OrderResult(success=True, order_id=order_id)

    async def reduce_only_close_with_retry(self, quantity, side):
        await self.place_post_only_order("", quantity, Decimal("0"), side, reduce_only=True)
        return OrderResult(success=True)

    async def place_market_order(self, contract_id, quantity, side):
        return await self.place_post_only_order(contract_id, quantity, Decimal("0"), side, reduce_only=True)


@dataclass
class DummyConfig:
    contract_id: str = "MOCK"
    ticker: str = "TEST"
    exchange: str = "mock"
    tick_size: Decimal = Decimal("0.1")
    min_order_size: Decimal = Decimal("1")
    break_buffer_ticks: Decimal = Decimal("1")
    breakout_static_pct: Decimal = Decimal("0")
    risk_pct: Decimal = Decimal("1")
    enable_dynamic_sl: bool = True
    mode_tag: str = "zigzag_timing"
    direction: str = "neutral"


class DummyServices:
    def __init__(self, exchange, logger, cache, notifications, data_feeds, pivot_watcher, mode_tag="zigzag_timing"):
        self.exchange_client = exchange
        self.logger = logger
        self.cache = cache
        self.notifications = notifications
        self.data_feeds = data_feeds
        self.pivot_watcher = pivot_watcher
        self.mode_tag = mode_tag


def _parse_pivot(raw: dict) -> PivotEntry:
    price = Decimal(str(raw["price"]))
    close_raw = raw.get("close_time")
    close_dt = datetime.fromisoformat(close_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    return PivotEntry(label=str(raw["label"]).upper(), price=price, close_time=close_dt, timeframe="1", raw_ticker="TEST")


async def run_fixture_once(fixture_path: Path):
    payload = json.loads(fixture_path.read_text())
    cfg_raw = payload["config"]
    cfg = DummyConfig(
        contract_id=cfg_raw.get("contract_id", "MOCK"),
        ticker=cfg_raw.get("ticker", "TEST"),
        exchange=cfg_raw.get("exchange", "mock"),
        tick_size=Decimal(cfg_raw.get("tick_size", "0.1")),
        min_order_size=Decimal(cfg_raw.get("min_order_size", "1")),
        break_buffer_ticks=Decimal(cfg_raw.get("break_buffer_ticks", "1")),
        breakout_static_pct=Decimal(cfg_raw.get("breakout_static_pct", "0")),
        risk_pct=Decimal(cfg_raw.get("risk_pct", "1")),
        enable_dynamic_sl=bool(cfg_raw.get("enable_dynamic_sl", True)),
        mode_tag=cfg_raw.get("mode_tag", "zigzag_timing"),
    )
    pivots = [_parse_pivot(p) for p in payload.get("pivots", [])]
    bbo_pairs = [(Decimal(b), Decimal(a)) for b, a in payload["bbo"]]

    logger = DummyLogger()
    cache = AsyncCache(logger)
    notifications = DummyNotifications()
    data_feeds = DummyDataFeeds(bbo_pairs)
    pivot_watcher = DummyPivotWatcher(pivots)
    exchange = DummyExchange()
    services = DummyServices(exchange, logger, cache, notifications, data_feeds, pivot_watcher)
    strategy = ZigZagTimingStrategy(services, cfg)

    await strategy.on_tick()

    return {
        "direction_lock": strategy.direction_lock,
        "stop_price": strategy.zigzag_stop_price,
        "tp_order": strategy.zigzag_tp_order_id,
        "errors": notifications.errors,
        "positions": exchange.positions,
    }


class ZigZagParityTestCase(unittest.TestCase):
    def test_parity_simple_fixture(self):
        fixture = Path(__file__).parent / "fixtures" / "zigzag_timing_parity_simple.json"
        result = asyncio.run(run_fixture_once(fixture))
        self.assertEqual(result["direction_lock"], "buy")
        self.assertIsNotNone(result["stop_price"])
        self.assertGreater(result["positions"], Decimal("0"))
        self.assertFalse(result["errors"], f"Unexpected errors: {result['errors']}")


if __name__ == "__main__":
    unittest.main()
