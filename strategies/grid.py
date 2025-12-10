"""Grid strategy implemented against CoreServices (decoupled from TradingBot)."""

import asyncio
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List, Optional, Sequence

from core import CoreServices
from exchanges.base import OrderInfo, OrderResult
from strategies.base import BaseStrategy


class GridStrategy(BaseStrategy):
    """Grid strategy that mirrors the legacy grid loop using injected services."""

    def __init__(self, services: CoreServices, config: Any):
        if services is None:
            raise ValueError("services is required")
        super().__init__(services=services)
        self.config = config
        self.exchange = services.exchange_client
        self.logger = services.logger
        self.cache = services.cache
        self.notifications = services.notifications
        self.data_feeds = services.data_feeds
        self.mode_tag = getattr(config, "mode_tag", "grid") or "grid"
        self.stop_new_orders = False
        self.min_order_size: Optional[Decimal] = getattr(config, "min_order_size", None)
        self.max_position_limit: Optional[Decimal] = getattr(config, "max_position_limit", None)
        self.basic_release_timeout_minutes: int = int(getattr(config, "basic_release_timeout_minutes", 0) or 0)
        self.active_close_orders: List[dict] = []
        self.last_close_orders = 0
        self.last_open_order_time = 0.0
        self.last_log_time = 0.0
        self.stop_loss_triggered = False
        self.net_failure_count = 0
        self.basic_full_since: Optional[float] = None
        self.last_release_attempt: float = 0.0
        self._equity_cache: Optional[Decimal] = None
        self._equity_cache_ts: float = 0.0
        self._equity_last_nonzero: Optional[Decimal] = None
        self.ttl_equity = float(getattr(config, "ttl_equity", 30.0))

    async def on_tick(self):
        """Execute one grid-cycle tick decoupled from TradingBot internals."""
        try:
            await self.notifications.maybe_send_daily_pnl(self.exchange, self._get_equity_snapshot)

            best_bid, best_ask = await self.data_feeds.get_bbo_cached(force=True)
            stop_trading, pause_trading, stop_loss_triggered = await self._check_price_condition(best_bid, best_ask)
            if stop_loss_triggered:
                await self._execute_stop_loss(best_bid, best_ask)
                return
            if stop_trading:
                await self._handle_stop_trading()
                return
            if pause_trading:
                await asyncio.sleep(5)
                return

            await self._refresh_close_orders()
            mismatch_detected = await self._log_status_periodically(best_bid, best_ask)

            if mismatch_detected:
                await asyncio.sleep(1)
                return

            wait_time = self._calculate_wait_time()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                return

            meet_grid_step_condition = await self._meet_grid_step_condition(best_bid, best_ask)
            if not meet_grid_step_condition:
                await asyncio.sleep(1)
                return

            placed = await self._place_and_monitor_open_order(best_bid, best_ask)
            if placed:
                self.last_close_orders += 1

            if self.net_failure_count > 0:
                await self.notifications.notify_error_once("[NET] 恢复：已重新连接交易所，恢复交易循环", dedup_seconds=0)
                self.net_failure_count = 0

        except Exception as e:
            err_msg = str(e)
            self.logger.log(f"Critical error in grid loop: {err_msg}", "ERROR")
            net_keywords = ["connection reset", "cannot connect", "timed out", "no bid/ask data", "ssl", "aiohttp"]
            is_net = any(k in err_msg.lower() for k in net_keywords)
            if is_net:
                self.net_failure_count += 1
                if self.net_failure_count % 5 == 0:
                    await self.notifications.notify_error_once(
                        f"[NET] 重试中（次数 {self.net_failure_count}）：{err_msg}", dedup_seconds=0
                    )
                await asyncio.sleep(min(30, 5 * self.net_failure_count))
                return
            await self.notifications.notify_error_once(f"出现报错：{err_msg}", dedup_seconds=300)
            await asyncio.sleep(5)
            return

    def _round_quantity(self, qty: Decimal) -> Decimal:
        try:
            return Decimal(qty).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        except Exception:
            return Decimal(qty)

    async def _get_equity_snapshot(self) -> Optional[Decimal]:
        now_ts = time.time()
        if self._equity_cache is not None and (now_ts - self._equity_cache_ts) < self.ttl_equity:
            return self._equity_cache

        async def _fetch_equity():
            equity_val = None
            if hasattr(self.exchange, "get_account_equity"):
                try:
                    equity_val = await self.exchange.get_account_equity()
                except Exception as exc:
                    self.logger.log(f"[RISK] get_account_equity failed: {exc}", "WARNING")
            if equity_val is None and hasattr(self.exchange, "get_available_balance"):
                try:
                    equity_val = await self.exchange.get_available_balance()
                except Exception as exc:
                    self.logger.log(f"[RISK] get_available_balance fallback failed: {exc}", "WARNING")
            return equity_val

        equity = await self.cache.get(f"equity:{self.mode_tag}", self.ttl_equity, _fetch_equity, force=False)
        if equity is None or equity <= 0:
            self.cache.invalidate(f"equity:{self.mode_tag}")
            return self._equity_last_nonzero
        self._equity_cache = equity
        self._equity_cache_ts = now_ts
        self._equity_last_nonzero = equity
        return equity

    async def _get_position_signed(self) -> Decimal:
        pos = await self.exchange.get_account_positions()
        return Decimal(pos) if pos is not None else Decimal(0)

    async def _get_active_orders(self) -> Sequence[OrderInfo]:
        return await self.exchange.get_active_orders(self.config.contract_id)

    async def _refresh_close_orders(self):
        orders = await self._get_active_orders()
        self.active_close_orders = []
        for order in orders:
            if order.side == self.config.close_order_side:
                self.active_close_orders.append({"id": order.order_id, "price": order.price, "size": order.size})

    def _calculate_wait_time(self) -> Decimal:
        cool_down_time = self.config.wait_time
        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return Decimal(0)
        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return Decimal(1)

        util = Decimal(len(self.active_close_orders)) / Decimal(self.config.max_orders)
        if util >= Decimal(2) / Decimal(3):
            cool_down_time = 2 * self.config.wait_time
        elif util >= Decimal(1) / Decimal(3):
            cool_down_time = self.config.wait_time
        elif util >= Decimal(1) / Decimal(6):
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return Decimal(0)
        return Decimal(1)

    async def _place_take_profit_order(self, quantity: Decimal, filled_price: Decimal) -> bool:
        close_side = self.config.close_order_side
        try:
            target_price = self.exchange.round_to_tick(
                filled_price * (Decimal(1) + (self.config.take_profit / Decimal(100)))
            ) if self.config.direction == "buy" else self.exchange.round_to_tick(
                filled_price * (Decimal(1) - (self.config.take_profit / Decimal(100)))
            )
        except Exception:
            target_price = filled_price
        res = await self.exchange.place_close_order(
            self.config.contract_id,
            self._round_quantity(quantity),
            target_price,
            close_side,
        )
        return bool(res and res.success)

    async def _place_and_monitor_open_order(self, best_bid: Decimal, best_ask: Decimal) -> bool:
        if self.stop_new_orders:
            return False
        qty = self._round_quantity(self.config.quantity)
        if self.min_order_size and qty < self.min_order_size:
            qty = self._round_quantity(self.min_order_size)
        if qty <= 0:
            return False
        order_result: OrderResult = await self.exchange.place_open_order(
            self.config.contract_id,
            qty,
            self.config.direction,
        )
        if not order_result or not order_result.success:
            return False

        order_id = order_result.order_id
        status = order_result.status or "OPEN"
        filled_price = order_result.price
        filled_size = Decimal(order_result.filled_size or 0)

        if status == "FILLED" or filled_size >= self._round_quantity(self.config.quantity):
            if filled_price is None:
                filled_price = best_ask if self.config.direction == "buy" else best_bid
            await self._place_take_profit_order(self.config.quantity, Decimal(filled_price))
            self.last_open_order_time = time.time()
            return True

            # Poll for a short window to see if order fills; otherwise cancel
        try:
            await asyncio.wait_for(self._wait_for_fill(order_id), timeout=10)
        except asyncio.TimeoutError:
            pass

        order_info = None
        try:
            order_info = await self.exchange.get_order_info(order_id)
        except Exception:
            order_info = None
        status = getattr(order_info, "status", status)
        filled_size = Decimal(getattr(order_info, "filled_size", filled_size) or 0)
        filled_price = getattr(order_info, "price", filled_price)

        if status != "FILLED":
            try:
                await self.exchange.cancel_order(order_id)
            except Exception:
                pass

        if filled_size > 0:
            if filled_price is None:
                filled_price = best_ask if self.config.direction == "buy" else best_bid
            await self._place_take_profit_order(filled_size, Decimal(filled_price))
            self.last_open_order_time = time.time()
            return True

        return False

    async def _wait_for_fill(self, order_id: str):
        # Simple polling helper for partial fills
        for _ in range(5):
            await asyncio.sleep(0.5)
            info = await self.exchange.get_order_info(order_id)
            if info and getattr(info, "status", "").upper() == "FILLED":
                return

    async def _log_status_periodically(self, best_bid: Decimal, best_ask: Decimal) -> bool:
        if time.time() - self.last_log_time <= 120 and self.last_log_time != 0:
            return False
        try:
            position_signed = await self._get_position_signed()
            position_amt = abs(position_signed)
            mid_price = (best_bid + best_ask) / 2
            equity = await self._get_equity_snapshot()
            if equity is None and position_amt > 0:
                equity = position_amt * mid_price
            active_close_amount = sum(Decimal(order.get("size", 0)) for order in self.active_close_orders)
            self.logger.log(
                f"Current Position: {self._round_quantity(position_amt)} | Active closing amount: {self._round_quantity(active_close_amount)} | "
                f"Order quantity: {len(self.active_close_orders)}",
            )
            self.last_log_time = time.time()

            # Basic position limit gate (mirrors legacy basic risk)
            if self.max_position_limit is not None:
                if position_amt >= self.max_position_limit:
                    self.stop_new_orders = True
                    if self.basic_full_since is None:
                        self.basic_full_since = time.time()
                else:
                    self.basic_full_since = None
                    self.stop_new_orders = False

            # Auto-fix mismatches similar to legacy log_status
            mismatch = position_amt - active_close_amount
            if mismatch > 0:
                try:
                    close_qty = mismatch
                    if self.min_order_size and close_qty < self.min_order_size:
                        close_qty = close_qty + self.config.quantity
                    close_side = self.config.close_order_side
                    if hasattr(self.exchange, "reduce_only_close_with_retry"):
                        await self.exchange.reduce_only_close_with_retry(close_qty, close_side)
                    else:
                        await self.exchange.place_close_order(self.config.contract_id, close_qty, mid_price, close_side)
                    self.logger.log(f"Auto-closing excess position {close_qty} via reduce-only", "WARNING")
                except Exception as fix_err:
                    self.logger.log(f"Auto-fix for position mismatch failed: {fix_err}", "ERROR")
                    await self.notifications.notify_error_once(f"Position mismatch auto-fix failed: {fix_err}")
                    return True
            elif mismatch < 0:
                self.logger.log("Active close orders exceed position, cancelling farthest", "WARNING")
                sorted_close = sorted(self.active_close_orders, key=lambda o: abs(Decimal(o["price"]) - mid_price), reverse=True)
                cancelled = Decimal(0)
                for order in sorted_close:
                    if cancelled >= abs(mismatch):
                        break
                    await self.exchange.cancel_order(order["id"])
                    cancelled += Decimal(order.get("size", 0))

            # Release liquidity if basic risk has been full for too long
            if self.basic_release_timeout_minutes > 0 and self.basic_full_since is not None:
                now_ts = time.time()
                if now_ts - self.basic_full_since > self.basic_release_timeout_minutes * 60:
                    if now_ts - self.last_release_attempt > self.basic_release_timeout_minutes * 60:
                        self.last_release_attempt = now_ts
                        release_qty = min(self.config.quantity, position_amt)
                        if release_qty > 0:
                            close_side = self.config.close_order_side
                            try:
                                if hasattr(self.exchange, "reduce_only_close_with_retry"):
                                    await self.exchange.reduce_only_close_with_retry(release_qty, close_side)
                                else:
                                    await self.exchange.place_close_order(self.config.contract_id, release_qty, mid_price, close_side)
                                self.logger.log(f"[RISK-BASIC] Released {release_qty} after sustained full position", "WARNING")
                            except Exception as rel_err:
                                self.logger.log(f"[RISK-BASIC] Release attempt failed: {rel_err}", "WARNING")
            return False
        except Exception as exc:
            self.logger.log(f"Error in periodic status check: {exc}", "ERROR")
            return True

    async def _meet_grid_step_condition(self, best_bid: Decimal, best_ask: Decimal) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = Decimal(next_close_order["price"])
            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit / 100)
                return next_close_price / new_order_close_price > 1 + self.config.grid_step / 100
            new_order_close_price = best_bid * (1 - self.config.take_profit / 100)
            return new_order_close_price / next_close_price > 1 + self.config.grid_step / 100
        return True

    async def _check_price_condition(self, best_bid: Decimal, best_ask: Decimal):
        stop_trading = False
        pause_trading = False
        stop_loss_triggered = False
        if self.config.stop_price != -1:
            if self.config.direction == "buy" and best_ask >= self.config.stop_price:
                stop_trading = True
            elif self.config.direction == "sell" and best_bid <= self.config.stop_price:
                stop_trading = True
        if self.config.pause_price != -1:
            if self.config.direction == "buy" and best_ask >= self.config.pause_price:
                pause_trading = True
            elif self.config.direction == "sell" and best_bid <= self.config.pause_price:
                pause_trading = True
        return stop_trading, pause_trading, stop_loss_triggered

    async def _execute_stop_loss(self, best_bid: Decimal, best_ask: Decimal):
        if self.stop_loss_triggered:
            return
        self.stop_loss_triggered = True
        msg = (
            f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}]\n"
            "Stop-loss triggered. Cancelling open orders and closing position (continue running).\n"
            "触发止损，正在撤单并平掉当前仓位，将继续运行。\n"
        )
        await self.notifications.send_notification(msg.lstrip())
        try:
            orders = await self._get_active_orders()
            for order in orders:
                try:
                    await self.exchange.cancel_order(order.order_id)
                except Exception as cancel_err:
                    self.logger.log(f"Failed to cancel order {order.order_id}: {cancel_err}", "WARNING")
        except Exception as exc:
            self.logger.log(f"Error fetching active orders during stop-loss: {exc}", "ERROR")

        try:
            position_size = abs(await self._get_position_signed())
        except Exception as exc:
            self.logger.log(f"Error fetching position during stop-loss: {exc}", "ERROR")
            position_size = Decimal(0)

        if position_size > 0:
            close_side = self.config.close_order_side
            market_close_result = None
            if hasattr(self.exchange, "place_market_order"):
                try:
                    market_close_result = await self.exchange.place_market_order(
                        self.config.contract_id,
                        position_size,
                        close_side,
                    )
                except Exception as market_err:
                    self.logger.log(f"Market stop-loss order failed: {market_err}", "ERROR")
            if (not market_close_result) or (not getattr(market_close_result, "success", False)):
                fallback_price = best_bid if close_side == "sell" else best_ask
                try:
                    await self.exchange.place_close_order(
                        self.config.contract_id,
                        position_size,
                        fallback_price,
                        close_side,
                    )
                except Exception as fallback_err:
                    self.logger.log(f"Stop-loss fallback order failed: {fallback_err}", "ERROR")

        self.stop_loss_triggered = False

    async def _handle_stop_trading(self):
        msg = (
            f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
            "Stopped trading due to stop price triggered\n"
            "价格已经达到停止交易价格，脚本将停止交易\n"
        )
        await self.notifications.send_notification(msg.lstrip())
        self.stop_new_orders = True


__all__ = ["GridStrategy"]
