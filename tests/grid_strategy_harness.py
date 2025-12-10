"""
Offline harness to smoke-test GridStrategy parity against the legacy grid loop.

This runs GridStrategy.on_tick() a few times with mocked exchange + data feeds,
so no network/API calls are made. Logs are printed to stdout for quick inspection.
"""

import asyncio
import itertools
import os
import sys
import json
from decimal import Decimal
from pathlib import Path

import dotenv

# Ensure repository root is on sys.path for local imports
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exchanges import ExchangeFactory
from trading_bot import TradingBot, TradingConfig
from strategies.grid import GridStrategy


class DummyExchange:
    """Minimal exchange stub to satisfy TradingBot grid path."""

    def __init__(self):
        self.current_order = None

    async def get_contract_attributes(self):
        return "MOCK_CONTRACT", Decimal("0.01")

    async def connect(self):
        return None

    async def disconnect(self):
        return None

    # WebSocket handler hooks (no-op for offline)
    def setup_order_update_handler(self, handler):
        return None


def build_bot_from_config(config_path: str = "botA.json") -> TradingBot:
    cfg_path = Path(config_path)

    def _load_json_with_comments(path: Path):
        lines = path.read_text(encoding="utf-8").splitlines()
        cleaned = []
        for line in lines:
            in_str = False
            new_line = ""
            for ch in line:
                if ch == '"' and (not new_line or new_line[-1] != "\\"):
                    in_str = not in_str
                if ch == "#" and not in_str:
                    break
                new_line += ch
            cleaned.append(new_line)
        return json.loads("\n".join(cleaned))

    raw = _load_json_with_comments(cfg_path)
    # Apply env overrides from config and env_file (best-effort)
    for k, v in raw.get("env", {}).items():
        os.environ[str(k)] = str(v)
    env_path = Path(raw.get("env_file", ".env"))
    if env_path.exists():
        dotenv.load_dotenv(env_path)
    trading = raw["trading"]
    risk = raw.get("risk", {})
    adv = risk.get("advanced", {})
    basic = risk.get("basic", {})
    zig_cfg = adv.get("zigzag", {}) if isinstance(adv, dict) else {}

    # Patch ExchangeFactory to avoid real exchange clients
    ExchangeFactory.create_exchange = staticmethod(lambda name, config: DummyExchange())

    tc = TradingConfig(
        ticker=trading["ticker"].upper(),
        contract_id="",
        tick_size=Decimal(0),
        quantity=Decimal(trading["quantity"]),
        take_profit=Decimal(trading["take_profit"]),
        direction=trading["direction"].lower(),
        max_orders=int(trading["max_orders"]),
        wait_time=int(trading["wait_time"]),
        exchange=raw["exchange"].lower(),
        grid_step=Decimal(trading["grid_step"]),
        stop_price=Decimal(trading["stop_price"]),
        pause_price=Decimal(trading["pause_price"]),
        boost_mode=bool(trading.get("boost", False)),
        min_order_size=Decimal(trading["min_order_size"]) if trading.get("min_order_size") else None,
        max_position_count=int(trading.get("max_position_count", 0)),
        basic_release_timeout_minutes=int(trading.get("basic_release_timeout_minutes", 0)),
        trading_mode=trading.get("trading_mode", "grid"),
        enable_advanced_risk=adv.get("enable", True),
        enable_basic_risk=basic.get("enable", True),
        webhook_sl=basic.get("webhook_sl", False),
        webhook_sl_fast=basic.get("webhook_sl_fast", False),
        webhook_reverse=basic.get("webhook_reverse", False),
        zigzag_pivot_file=os.getenv("ZIGZAG_PIVOT_FILE"),
        webhook_basic_direction_file=os.getenv("WEBHOOK_BASIC_DIRECTION_FILE"),
        break_buffer_ticks=Decimal(str(zig_cfg.get("break_buffer_ticks", "10"))),
    )
    return TradingBot(tc)


def patch_bot_for_offline(bot: TradingBot):
    """Monkeypatch bot methods to avoid network and make decisions deterministic."""
    # Deterministic BBO sequence (rotates)
    bbo_sequence = [(Decimal("100"), Decimal("100.5")), (Decimal("100.2"), Decimal("100.7"))]
    bbo_iter = itertools.cycle(bbo_sequence)

    async def fake_bbo_cached(force: bool = False):
        return next(bbo_iter)

    async def fake_last_trade_price_cached(best_bid=None, best_ask=None, force=False):
        if best_bid is None or best_ask is None:
            best_bid, best_ask = await fake_bbo_cached(force=force)
        return (Decimal(best_bid) + Decimal(best_ask)) / 2

    async def fake_active_orders_cached(force: bool = False):
        return []

    async def fake_log_status_periodically():
        return False

    def fake_calculate_wait_time():
        return 0

    async def fake_poll_webhook_direction():
        return None

    async def fake_handle_webhook_stop_tasks():
        return False

    async def fake_perform_slow_unwind():
        return None

    async def fake_maybe_send_daily_pnl():
        return None

    async def fake_sync_external_pivots():
        return None

    async def fake_place_and_monitor_open_order():
        bot.logger.log("[GRID] mock place_and_monitor_open_order()", "INFO")
        return None

    async def fake_execute_stop_loss(best_bid, best_ask):
        bot.logger.log("[GRID] mock execute_stop_loss()", "INFO")
        return None

    async def fake_send_notification(message: str):
        bot.logger.log(f"[NOTIFY] {message}", "INFO")
        return None

    # Apply patches
    bot._get_bbo_cached = fake_bbo_cached
    bot._get_last_trade_price_cached = fake_last_trade_price_cached
    bot._get_active_orders_cached = fake_active_orders_cached
    bot._log_status_periodically = fake_log_status_periodically
    bot._calculate_wait_time = fake_calculate_wait_time
    bot._poll_webhook_direction = fake_poll_webhook_direction
    bot._handle_webhook_stop_tasks = fake_handle_webhook_stop_tasks
    bot._perform_slow_unwind = fake_perform_slow_unwind
    bot._maybe_send_daily_pnl = fake_maybe_send_daily_pnl
    bot._sync_external_pivots = fake_sync_external_pivots
    bot._place_and_monitor_open_order = fake_place_and_monitor_open_order
    bot._execute_stop_loss = fake_execute_stop_loss
    bot.send_notification = fake_send_notification


async def main():
    bot = build_bot_from_config("botA.json")
    patch_bot_for_offline(bot)
    strategy = GridStrategy(bot)

    await bot.exchange_client.connect()
    # Initialize contract attributes (mock)
    bot.config.contract_id, bot.config.tick_size = await bot.exchange_client.get_contract_attributes()

    bot.logger.log("[GRID] Starting offline grid strategy harness (3 ticks)", "INFO")
    for idx in range(3):
        bot.logger.log(f"[GRID] Tick {idx+1}", "INFO")
        await strategy.on_tick()
        await asyncio.sleep(0)  # yield control

    await bot.exchange_client.disconnect()
    bot.logger.log("[GRID] Harness completed", "INFO")


if __name__ == "__main__":
    asyncio.run(main())
