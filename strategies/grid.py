"""Grid strategy wrapping existing TradingBot grid loop logic."""

import asyncio
from typing import Any

from strategies.base import BaseStrategy


class GridStrategy(BaseStrategy):
    def __init__(self, bot: Any):
        super().__init__(services=None)
        self.bot = bot

    async def on_tick(self):
        """Execute one grid-cycle tick mirroring the legacy grid loop."""
        try:
            await self.bot._maybe_send_daily_pnl()

            # Safety guard: if zigzag timing is enabled, delegate to timing loop for parity.
            if self.bot.zigzag_timing_enabled:
                await self.bot._handle_zigzag_timing_cycle()
                await asyncio.sleep(2)
                return

            if self.bot.webhook_sl:
                await self.bot._poll_webhook_direction()
            if self.bot.webhook_sl and self.bot.webhook_stop_mode:
                handled_webhook = await self.bot._handle_webhook_stop_tasks()
                if handled_webhook:
                    await asyncio.sleep(0.5 if self.bot.webhook_stop_mode == "fast" else max(self.bot.config.wait_time, 1))
                    return

            if self.bot.pending_reverse_state == "unwinding":
                await self.bot._perform_slow_unwind()
                await asyncio.sleep(max(self.bot.config.wait_time, 1))
                return

            active_orders = await self.bot._get_active_orders_cached()

            # Refresh close-order snapshot (used by grid step calculation)
            self.bot.active_close_orders = []
            for order in active_orders:
                if order.side == self.bot.config.close_order_side:
                    self.bot.active_close_orders.append(
                        {"id": order.order_id, "price": order.price, "size": order.size}
                    )

            mismatch_detected = await self.bot._log_status_periodically()

            stop_trading, pause_trading, stop_loss_triggered, best_bid, best_ask = await self.bot._check_price_condition()
            if stop_loss_triggered:
                await self.bot._execute_stop_loss(best_bid, best_ask)
                return

            if stop_trading:
                msg = f"\n\nWARNING: [{self.bot.config.exchange.upper()}_{self.bot.config.ticker.upper()}] \n"
                msg += "Stopped trading due to stop price triggered\n"
                msg += "价格已经达到停止交易价格，脚本将停止交易\n"
                await self.bot.send_notification(msg.lstrip())
                await self.bot.graceful_shutdown(msg)
                return

            if pause_trading:
                await asyncio.sleep(5)
                return

            if not mismatch_detected:
                wait_time = self.bot._calculate_wait_time()

                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    return

                meet_grid_step_condition = await self.bot._meet_grid_step_condition()
                if not meet_grid_step_condition:
                    await asyncio.sleep(1)
                    return

                await self.bot._place_and_monitor_open_order()
                self.bot.last_close_orders += 1

            if self.bot.net_failure_count > 0:
                await self.bot._notify_error_once("[NET] 恢复：已重新连接交易所，恢复交易循环", dedup_seconds=0)
                self.bot.net_failure_count = 0

        except Exception as e:
            err_msg = str(e)
            self.bot.logger.log(f"Critical error in main loop: {err_msg}", "ERROR")
            net_keywords = ["connection reset", "cannot connect", "timed out", "no bid/ask data", "ssl", "aiohttp"]
            is_net = any(k in err_msg.lower() for k in net_keywords)
            if is_net:
                self.bot.net_failure_count += 1
                if self.bot.net_failure_count % 5 == 0:
                    await self.bot._notify_error_once(f"[NET] 重试中（次数 {self.bot.net_failure_count}）：{err_msg}", dedup_seconds=0)
                await asyncio.sleep(min(30, 5 * self.bot.net_failure_count))
                return
            await self.bot._notify_error_once(f"出现报错：{err_msg}", dedup_seconds=300)
            await asyncio.sleep(5)
            return


__all__ = ["GridStrategy"]
