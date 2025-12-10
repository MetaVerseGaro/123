"""Offline harness to smoke-test the CoreServices-based GridStrategy."""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from itertools import cycle
from typing import List
import unittest

from core import AsyncCache
from exchanges.base import OrderInfo, OrderResult
from strategies.grid import GridStrategy


class DummyLogger:
    def log(self, message: str, level: str = "INFO"):
        print(f"[{level}] {message}")


class DummyNotifications:
    def __init__(self):
        self.errors: List[str] = []

    async def send_notification(self, message: str, level: str = "INFO"):
        print(f"[NOTIFY:{level}] {message}")

    async def notify_error_once(self, message: str, dedup_seconds: int = 300):
        self.errors.append(message)
        await self.send_notification(message, level="ERROR")

    async def maybe_send_daily_pnl(self, exchange_client, get_equity_snapshot):
        return None


class DummyDataFeeds:
    def __init__(self):
        self._bbo_iter = cycle([(Decimal("100"), Decimal("100.5")), (Decimal("100.2"), Decimal("100.7"))])

    async def get_bbo_cached(self, force: bool = False):
        return next(self._bbo_iter)

    async def get_last_trade_price_cached(self, best_bid=None, best_ask=None, force=False):
        if best_bid is None or best_ask is None:
            best_bid, best_ask = await self.get_bbo_cached(force=force)
        return (Decimal(best_bid) + Decimal(best_ask)) / 2


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
        self.close_orders.append(OrderResult(success=True, order_id=f"close-{len(self.close_orders)+1}", side=side, size=Decimal(quantity), price=price, status="FILLED", filled_size=Decimal(quantity)))
        return self.close_orders[-1]

    async def cancel_order(self, order_id):
        self.open_orders = [o for o in self.open_orders if o.order_id != order_id]
        return OrderResult(success=True, order_id=order_id)

    async def place_market_order(self, contract_id, quantity, side):
        return OrderResult(success=True, order_id=f"mkt-{side}", side=side, size=quantity, price=Decimal("100"), status="FILLED", filled_size=quantity)

    def round_to_tick(self, price):
        return Decimal(price)


@dataclass
class DummyConfig:
    contract_id: str = "MOCK"
    tick_size: Decimal = Decimal("0.01")
    quantity: Decimal = Decimal("1")
    take_profit: Decimal = Decimal("0.2")
    direction: str = "buy"
    max_orders: int = 5
    wait_time: int = 1
    exchange: str = "mock"
    grid_step: Decimal = Decimal("0.5")
    stop_price: Decimal = Decimal("-1")
    pause_price: Decimal = Decimal("-1")
    close_order_side: str = "sell"
    mode_tag: str = "grid"
    min_order_size: Decimal = Decimal("0")


class DummyServices:
    def __init__(self, exchange, logger, cache, notifications, data_feeds, mode_tag="grid"):
        self.exchange_client = exchange
        self.logger = logger
        self.cache = cache
        self.notifications = notifications
        self.data_feeds = data_feeds
        self.mode_tag = mode_tag


async def run_harness_ticks(ticks: int = 2):
    logger = DummyLogger()
    cache = AsyncCache(logger)
    notifications = DummyNotifications()
    data_feeds = DummyDataFeeds()
    exchange = DummyExchange()
    services = DummyServices(exchange, logger, cache, notifications, data_feeds)
    config = DummyConfig()
    strategy = GridStrategy(services, config)

    for _ in range(ticks):
        await strategy.on_tick()
        await asyncio.sleep(0)

    return exchange, notifications



class GridStrategyHarnessTest(unittest.TestCase):
    def test_grid_strategy_smoke(self):
        exchange, notifications = asyncio.get_event_loop().run_until_complete(run_harness_ticks())
        self.assertTrue(exchange.close_orders, "GridStrategy should place at least one close order")
        self.assertFalse(notifications.errors, "No errors should be emitted in smoke harness")


if __name__ == "__main__":
    unittest.main()
