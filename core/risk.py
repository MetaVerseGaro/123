"""Risk management helpers extracted from the monolith (bot-backed)."""

import asyncio
import os
import time
from decimal import Decimal
from typing import Optional


class RiskManager:
	def __init__(self, bot):
		self.bot = bot
		self.logger = bot.logger
		self.config = bot.config
		self.cache = bot.cache
		self.exchange_client = bot.exchange_client

	async def calc_qty_by_risk_zigzag(self, entry_price: Decimal, stop_price: Decimal) -> Optional[Decimal]:
		"""Calculate target quantity based on risk_pct and stop distance."""
		try:
			entry_price = Decimal(entry_price)
			stop_price = Decimal(stop_price)
		except Exception:
			return None
		unit_risk = abs(entry_price - stop_price)
		if unit_risk <= 0:
			return None
		equity = await self.bot._get_equity_snapshot()
		if equity is None or equity <= 0:
			return None
		allowed_risk = equity * (self.bot.risk_pct / Decimal(100))
		if allowed_risk <= 0:
			return None
		qty = allowed_risk / unit_risk
		if self.bot.min_order_size:
			qty = max(qty, self.bot.min_order_size)
		return self.bot._round_quantity(qty)

	def calc_tp_price_rr2(self, entry_price: Decimal, stop_price: Decimal, direction: str) -> Optional[Decimal]:
		"""Calculate TP price at RR 1:2 from entry/stop."""
		try:
			entry_price = Decimal(entry_price)
			stop_price = Decimal(stop_price)
		except Exception:
			return None
		if direction == "buy":
			risk = entry_price - stop_price
			if risk <= 0:
				return None
			tp_price = entry_price + risk * 2
		else:
			risk = stop_price - entry_price
			if risk <= 0:
				return None
			tp_price = entry_price - risk * 2
		try:
			return self.exchange_client.round_to_tick(tp_price)
		except Exception:
			return tp_price

	def compute_dynamic_stop(self, direction: str) -> Optional[Decimal]:
		"""Compute dynamic stop based on confirmed pivots and tick buffer."""
		tick = self.config.tick_size
		buffer_ticks = self.bot.break_buffer_ticks * tick
		if direction == "buy" and self.bot.last_confirmed_low is not None:
			return self.bot.last_confirmed_low - buffer_ticks
		if direction == "sell" and self.bot.last_confirmed_high is not None:
			return self.bot.last_confirmed_high + buffer_ticks
		return None

	async def refresh_stop_loss(self, force: bool = False, mode: Optional[str] = None):
		"""Refresh stop-loss tracking without placing native SL orders."""
		mode = mode or self.bot.mode_tag
		if mode != self.bot.mode_tag:
			if self.cache.debug:
				self.logger.log(f"{self.bot.mode_prefix} skip refresh_stop_loss from mode {mode}", "DEBUG")
			return
		if not self.bot.enable_dynamic_sl:
			return

		if self.bot.current_sl_order_id:
			try:
				await self.exchange_client.cancel_order(self.bot.current_sl_order_id)
			except Exception:
				pass
			self.bot.current_sl_order_id = None

		try:
			pos_signed = await self.bot._get_position_signed_cached(force=force)
		except Exception as e:
			self.logger.log(f"[SL] Failed to fetch position for refresh: {e}", "WARNING")
			return

		if pos_signed is None:
			self.logger.log(f"[SL] Position is None, skipping refresh", "WARNING")
			return

		pos_abs = abs(pos_signed)
		if pos_abs == 0:
			self.bot.dynamic_stop_price = None
			self.bot.dynamic_stop_direction = None
			return

		direction = "buy" if pos_signed > 0 else "sell"
		prev_stop = self.bot.dynamic_stop_price
		if self.bot.dynamic_stop_direction and self.bot.dynamic_stop_direction != direction:
			self.bot.dynamic_stop_price = None
		struct_stop = self.compute_dynamic_stop(direction)
		if struct_stop is None:
			return

		if (not force) and self.bot._last_stop_eval_price is not None:
			if abs(struct_stop - self.bot._last_stop_eval_price) < self.config.tick_size:
				if self.cache.debug:
					self.logger.log("[SL] Dynamic stop debounced (no meaningful change)", "DEBUG")
				return

		dyn_stop = struct_stop
		if self.bot.dynamic_stop_price is not None:
			if direction == "buy":
				dyn_stop = max(struct_stop, self.bot.dynamic_stop_price)
			else:
				dyn_stop = min(struct_stop, self.bot.dynamic_stop_price)

		if (not force) and self.bot.dynamic_stop_price is not None:
			if abs(dyn_stop - self.bot.dynamic_stop_price) < self.config.tick_size:
				return

		self.bot.dynamic_stop_price = dyn_stop
		self.bot.dynamic_stop_direction = direction
		self.bot._last_stop_eval_price = dyn_stop
		if self.bot.enable_notifications and prev_stop != self.bot.dynamic_stop_price:
			try:
				await self.bot.send_notification(f"[SL] Dynamic stop updated from {prev_stop} to {self.bot.dynamic_stop_price} for {direction.upper()}")
			except Exception:
				pass
		await self.run_redundancy_check(direction, pos_signed, mode=mode)

	async def run_redundancy_check(self, direction: str, pos_signed: Decimal, mode: Optional[str] = None):
		"""Re-evaluate redundancy and update stop_new_orders state."""
		mode = mode or self.bot.mode_tag
		if mode != self.bot.mode_tag:
			if self.cache.debug:
				self.logger.log(f"{self.bot.mode_prefix} skip redundancy_check from mode {mode}", "DEBUG")
			return
		if not (self.bot.stop_loss_enabled and self.bot.enable_advanced_risk):
			return
		try:
			position_amt = abs(pos_signed)
			if position_amt == 0:
				self.bot._set_stop_new_orders(False, mode=self.bot.mode_tag)
				self.bot.redundancy_insufficient_since = None
				return
			best_bid, best_ask = await self.bot._get_bbo_cached()
			mid_price = (best_bid + best_ask) / 2
			avg_price = mid_price
			stop_price = self.bot.dynamic_stop_price if (self.bot.enable_dynamic_sl and self.bot.dynamic_stop_price) else None
			pos_detail = await self.bot._get_position_detail_prefer_ws()
			if pos_detail:
				pos_signed_detail, avg_price_detail = pos_detail
				if pos_signed_detail != 0:
					avg_price = avg_price_detail

			if stop_price and position_amt > 0:
				if pos_signed > 0:
					per_base_loss = (avg_price - stop_price)
				else:
					per_base_loss = (stop_price - avg_price)
				if per_base_loss <= 0:
					return
				equity = await self.bot._get_equity_snapshot()
				if equity is None:
					return
				max_loss = equity * (self.bot.risk_pct / Decimal(100))
				allowed_position = max_loss / per_base_loss
				if allowed_position < 0:
					allowed_position = Decimal(0)
				allowed_position = self.bot._round_quantity(allowed_position)
				excess = max(Decimal(0), position_amt - allowed_position)
				tolerance = self.bot.min_order_size or Decimal("0")
				if excess >= tolerance and position_amt > 0:
					try:
						close_qty = excess
						close_side = 'sell' if pos_signed > 0 else 'buy'
						await self.exchange_client.reduce_only_close_with_retry(close_qty, close_side)
						self.logger.log(f"[RISK] Reduced excess position {close_qty} to keep stop-loss exposure within limit", "WARNING")
						self.bot._invalidate_position_cache()
					except Exception as e:
						self.logger.log(f"[RISK] Failed to trim excess position: {e}", "ERROR")
				headroom = allowed_position - position_amt
				needed_headroom = self.config.quantity or Decimal(0)
				if headroom < needed_headroom:
					if not self.bot.stop_new_orders and self.bot.enable_notifications:
						await self.bot.send_notification(f"[RISK] Stop new orders after SL update: headroom {headroom} < qty {self.config.quantity}")
					self.bot._set_stop_new_orders(True, mode=self.bot.mode_tag)
					if self.bot.redundancy_insufficient_since is None:
						self.bot.redundancy_insufficient_since = time.time()
					self.bot.last_stop_new_notify = True
					try:
						active_orders = await self.bot._get_active_orders_cached()
						for order in active_orders:
							if order.side != self.config.close_order_side:
								await self.exchange_client.cancel_order(order.order_id)
					except Exception as e:
						self.logger.log(f"[RISK] Cancel open orders after SL update failed: {e}", "WARNING")
				else:
					if self.bot.stop_new_orders and self.bot.enable_notifications:
						await self.bot.send_notification("[RISK] Resume new orders after SL update: redundancy restored")
					self.bot._set_stop_new_orders(False, mode=self.bot.mode_tag)
					self.bot.redundancy_insufficient_since = None
		except Exception as e:
			self.logger.log(f"[RISK] redundancy check after SL update failed: {e}", "WARNING")


__all__ = ["RiskManager"]
