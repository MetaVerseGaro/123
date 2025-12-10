"""Parity-oriented harness for GridStrategy using fixtures."""

import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import List

from core import AsyncCache
from exchanges.base import OrderInfo, OrderResult
from strategies.grid import GridStrategy


class DummyLogger:
    def __init__(self):
        self.messages: List[str] = []

    def log(self, message: str, level: str = "INFO"):
        self.messages.append(f"[{level}] {message}")


class DummyNotifications:
    def __init__(self):
        self.errors: List[str] = []

    async def send_notification(self, message: str, level: str = "INFO"):
        self.errors.append(message) if level == "ERROR" else None

    async def notify_error_once(self, message: str, dedup_seconds: int = 300):
        self.errors.append(message)

    async def maybe_send_daily_pnl(self, exchange_client, get_equity_snapshot):
        return None


class DummyDataFeeds:
    def __init__(self, bbo_pairs):
        self._iter = iter(bbo_pairs)

    async def get_bbo_cached(self, force: bool = False):
        try:
            bid, ask = next(self._iter)
        except StopIteration:
            bid, ask = bbo_pairs[-1]
        return Decimal(str(bid)), Decimal(str(ask))


class DummyExchange:
    def __init__(self):
        self.open_orders: List[OrderInfo] = []
        self.close_orders: List[OrderResult] = []
        self.positions = Decimal(0)

    async def get_active_orders(self, contract_id=None):
        return list(self.open_orders)

    async def get_order_info(self, order_id):
        for order in self.open_orders:
            if order.order_id == order_id:
                return order
        return OrderInfo(order_id=order_id, side="buy", size=Decimal("0"), price=Decimal("0"), status="OPEN")

    async def get_account_positions(self):
        return self.positions

    async def get_account_equity(self):
        return Decimal("1000")

    async def get_available_balance(self):
        return Decimal("1000")

    async def place_open_order(self, contract_id, quantity, direction):
        order = OrderInfo(
            order_id=f"open-{len(self.open_orders)+1}",
            side=direction,
            size=Decimal(quantity),
            price=Decimal("100.1"),
            status="FILLED",
            filled_size=Decimal(quantity),
        )
        self.open_orders.append(order)
        return OrderResult(success=True, order_id=order.order_id, side=direction, size=Decimal(quantity), price=order.price, status="FILLED", filled_size=Decimal(quantity))

    async def place_close_order(self, contract_id, quantity, price, side):
        res = OrderResult(success=True, order_id=f"close-{len(self.close_orders)+1}", side=side, size=Decimal(quantity), price=price, status="FILLED", filled_size=Decimal(quantity))
        self.close_orders.append(res)
        return res

    async def cancel_order(self, order_id):
        self.open_orders = [o for o in self.open_orders if o.order_id != order_id]
        return OrderResult(success=True, order_id=order_id)

    async def place_market_order(self, contract_id, quantity, side):
        return OrderResult(success=True, order_id=f"mkt-{side}", side=side, size=quantity, price=Decimal("100"), status="FILLED", filled_size=quantity)

    def round_to_tick(self, price):
        return Decimal(price)

    async def reduce_only_close_with_retry(self, quantity, side):
        return OrderResult(success=True, order_id=f"reduce-{side}", side=side, size=quantity, price=Decimal("100"), status="FILLED", filled_size=quantity)


@dataclass
class DummyConfig:
    contract_id: str = "MOCK"
    tick_size: Decimal = Decimal("0.01")
    quantity: Decimal = Decimal("1")
    take_profit: Decimal = Decimal("0.2")
    direction: str = "buy"
    max_orders: int = 5
    wait_time: int = 0
    exchange: str = "mock"
    grid_step: Decimal = Decimal("0.5")
    stop_price: Decimal = Decimal("-1")
    pause_price: Decimal = Decimal("-1")
    close_order_side: str = "sell"
    mode_tag: str = "grid"
    min_order_size: Decimal = Decimal("0")
    max_position_limit: Decimal = Decimal("0")
    basic_release_timeout_minutes: int = 0


class DummyServices:
    def __init__(self, exchange, logger, cache, notifications, data_feeds, mode_tag="grid"):
        self.exchange_client = exchange
        self.logger = logger
        self.cache = cache
        self.notifications = notifications
        self.data_feeds = data_feeds
        self.mode_tag = mode_tag


async def run_fixture_once(fixture_path: Path):
    payload = json.loads(fixture_path.read_text())
    cfg_raw = payload["config"]
    cfg = DummyConfig(
        direction=cfg_raw.get("direction", "buy"),
        take_profit=Decimal(cfg_raw.get("take_profit", "0.2")),
        grid_step=Decimal(cfg_raw.get("grid_step", "0.5")),
        wait_time=int(cfg_raw.get("wait_time", 0)),
        max_orders=int(cfg_raw.get("max_orders", 5)),
        close_order_side=cfg_raw.get("close_order_side", "sell"),
        min_order_size=Decimal(cfg_raw.get("min_order_size", "0")),
        ticker=cfg_raw.get("ticker", "TEST"),
        exchange=cfg_raw.get("exchange", "mock"),
    )
    bbo_pairs = [(Decimal(b), Decimal(a)) for b, a in payload["bbo"]]
    expected = payload.get("expected", {})

    logger = DummyLogger()
    cache = AsyncCache(logger)
    notifications = DummyNotifications()
    data_feeds = DummyDataFeeds(bbo_pairs)
    exchange = DummyExchange()
    services = DummyServices(exchange, logger, cache, notifications, data_feeds)
    strategy = GridStrategy(services, cfg)

    await strategy.on_tick()

    return {
        "opens": len(exchange.open_orders),
        "closes": len(exchange.close_orders),
        "errors": notifications.errors,
        "expected": expected,
        "logger": logger.messages,
    }


def test_parity_simple_fixture():
    fixture = Path(__file__).parent / "fixtures" / "grid_parity_simple.json"
    result = asyncio.run(run_fixture_once(fixture))
    assert result["opens"] >= result["expected"].get("opens", 1)
    assert result["closes"] >= result["expected"].get("closes", 1)
    assert not result["errors"], f"Unexpected errors: {result['errors']}"


if __name__ == "__main__":
    test_parity_simple_fixture()
