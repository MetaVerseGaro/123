"""Order execution helpers extracted from the monolith (bot-backed)."""

import asyncio
import time
import traceback
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Tuple

from exchanges.base import OrderResult


class EngineManager:
	def __init__(self, bot):
		self.bot = bot
		self.logger = bot.logger
		self.config = bot.config
		self.exchange_client = bot.exchange_client

	async def place_post_only_limit(self, side: str, quantity: Decimal, price: Decimal, reduce_only: bool = False) -> OrderResult:
		quantity = self.bot._round_quantity(quantity)
		if self.bot.min_order_size and quantity < self.bot.min_order_size:
			quantity = self.bot.min_order_size
		if quantity <= 0:
			return OrderResult(success=False, error_message="Invalid quantity")
		try:
			price = self.exchange_client.round_to_tick(price)
		except Exception:
			price = Decimal(price)

		client = self.exchange_client
		if hasattr(client, "place_post_only_order"):
			try:
				return await client.place_post_only_order(self.config.contract_id, quantity, price, side, reduce_only=reduce_only)
			except TypeError:
				try:
					return await client.place_post_only_order(self.config.contract_id, quantity, price, side)
				except Exception as exc:
					self.logger.log(f"[ZIGZAG-TIMING] place_post_only_order fallback err: {exc}", "WARNING")
			except Exception as exc:
				self.logger.log(f"[ZIGZAG-TIMING] place_post_only_order err: {exc}", "WARNING")

		if hasattr(client, "place_limit_order"):
			try:
				return await client.place_limit_order(self.config.contract_id, quantity, price, side, reduce_only=reduce_only)
			except TypeError:
				try:
					return await client.place_limit_order(self.config.contract_id, quantity, price, side)
				except Exception as exc:
					self.logger.log(f"[ZIGZAG-TIMING] place_limit_order fallback err: {exc}", "WARNING")
			except Exception as exc:
				self.logger.log(f"[ZIGZAG-TIMING] place_limit_order err: {exc}", "WARNING")

		try:
			if reduce_only:
				if hasattr(client, "place_close_order"):
					return await client.place_close_order(self.config.contract_id, quantity, price, side)
			else:
				if hasattr(client, "place_open_order"):
					return await client.place_open_order(self.config.contract_id, quantity, side)
		except Exception as exc:
			return OrderResult(success=False, error_message=str(exc))

		return OrderResult(success=False, error_message="No supported post-only/limit order method")

	def build_price_ladder(self, side: str, base_price: Decimal, max_orders: int) -> List[Decimal]:
		prices: List[Decimal] = []
		tick = self.config.tick_size
		for i in range(max_orders):
			if side == "buy":
				px = base_price - tick * i
				if px <= 0:
					px = tick
			else:
				px = base_price + tick * i
			try:
				px = self.exchange_client.round_to_tick(px)
			except Exception:
				pass
			prices.append(px)
		return prices

	async def post_only_entry_batch(self, direction: str, target_qty: Decimal, best_bid: Decimal, best_ask: Decimal, wait_sec: float = 20.0) -> Tuple[Decimal, Decimal]:
		if target_qty <= 0:
			return Decimal(0), await self.bot._get_directional_position(direction, force=True)
		base_price = best_bid if direction == "buy" else best_ask
		if base_price is None or base_price <= 0:
			return Decimal(0), await self.bot._get_directional_position(direction, force=True)
		max_per = self.bot.max_fast_close_qty if self.bot.max_fast_close_qty > 0 else target_qty
		num_orders = int((target_qty / max_per).to_integral_value(rounding=ROUND_HALF_UP))
		if num_orders * max_per < target_qty:
			num_orders += 1
		prices = self.build_price_ladder(direction, base_price, max(num_orders, 1))
		start_qty = await self.bot._get_directional_position(direction, force=True)
		remaining = target_qty
		order_ids: List[str] = []
		for px in prices:
			if remaining <= 0:
				break
			qty = min(max_per, remaining)
			res = await self.place_post_only_limit(direction, qty, px, reduce_only=False)
			if res and res.success:
				order_ids.append(res.order_id)
				remaining -= qty
		start_ts = time.time()
		while time.time() - start_ts < wait_sec:
			await asyncio.sleep(1)
		await self.bot._cancel_order_ids(order_ids)
		await asyncio.sleep(0.1)
		end_qty = await self.bot._get_directional_position(direction, force=True)
		filled = max(Decimal(0), end_qty - start_qty)
		return filled, end_qty

	async def post_only_exit_batch(self, close_side: str, close_qty: Decimal, best_bid: Decimal, best_ask: Decimal, wait_sec: float = 5.0) -> Tuple[Decimal, Decimal]:
		if close_qty <= 0:
			pos_abs = abs(await self.bot._get_position_signed_cached(force=True))
			return Decimal(0), pos_abs
		base_price = best_ask if close_side.lower() == "sell" else best_bid
		if base_price is None or base_price <= 0:
			pos_abs = abs(await self.bot._get_position_signed_cached(force=True))
			return Decimal(0), pos_abs
		max_per = self.bot.max_fast_close_qty if self.bot.max_fast_close_qty > 0 else close_qty
		num_orders = int((close_qty / max_per).to_integral_value(rounding=ROUND_HALF_UP))
		if num_orders * max_per < close_qty:
			num_orders += 1
		prices = self.build_price_ladder(close_side, base_price, max(num_orders, 1))
		start_abs = abs(await self.bot._get_position_signed_cached(force=True))
		remaining = close_qty
		order_ids: List[str] = []
		for px in prices:
			if remaining <= 0:
				break
			qty = min(max_per, remaining)
			res = await self.place_post_only_limit(close_side, qty, px, reduce_only=True)
			if res and res.success:
				order_ids.append(res.order_id)
				remaining -= qty
		start_ts = time.time()
		while time.time() - start_ts < wait_sec:
			await asyncio.sleep(0.5)
		await self.bot._cancel_order_ids(order_ids)
		await asyncio.sleep(0.1)
		end_abs = abs(await self.bot._get_position_signed_cached(force=True))
		closed = max(Decimal(0), start_abs - end_abs)
		return closed, end_abs

	async def cancel_zigzag_tp(self):
		if not self.bot.zigzag_tp_order_id:
			return
		try:
			await self.exchange_client.cancel_order(self.bot.zigzag_tp_order_id)
			self.bot._invalidate_order_cache()
		except Exception as exc:
			self.logger.log(f"[ZIGZAG-TIMING] Cancel TP failed: {exc}", "WARNING")
		finally:
			self.bot.zigzag_tp_order_id = None
			self.bot.zigzag_tp_qty = None

	async def place_zigzag_tp(self, direction: str, filled_qty: Decimal, entry_price: Decimal, stop_price: Decimal):
		await self.cancel_zigzag_tp()
		tp_price = self.bot._calc_tp_price_rr2(entry_price, stop_price, direction)
		if tp_price is None:
			self.logger.log(f"[ZIGZAG-TIMING] Skip TP: tp_price None (entry={entry_price}, stop={stop_price}, dir={direction})", "INFO")
			return
		qty = self.bot._round_quantity(filled_qty / Decimal(2))
		if self.bot.min_order_size and qty < self.bot.min_order_size:
			self.logger.log(
				f"[ZIGZAG-TIMING] Skip TP: qty {qty} < min_order_size {self.bot.min_order_size} (filled={filled_qty})",
				"INFO",
			)
			return
		side = "sell" if direction == "buy" else "buy"
		res = await self.place_post_only_limit(side, qty, tp_price, reduce_only=True)
		if res and res.success:
			self.bot.zigzag_tp_order_id = res.order_id
			self.bot.zigzag_tp_qty = qty
			self.bot._invalidate_order_cache()
			self.logger.log(f"[ZIGZAG-TIMING] TP placed {side.upper()} qty={qty} @ {tp_price} reduce-only", "INFO")
		else:
			self.logger.log(f"[ZIGZAG-TIMING] Place TP failed: {getattr(res, 'error_message', '')}", "WARNING")

	async def notify_zigzag_trade(self, action: str, direction: str, qty: Decimal, price: Decimal, note: str = ""):
		if not self.bot.enable_notifications:
			return
		try:
			qty_val = Decimal(qty)
			price_val = Decimal(price)
			exposure = qty_val * price_val
		except Exception:
			qty_val = qty
			price_val = price
			exposure = None
		msg_parts = [
			"[ZIGZAG-TIMING]",
			action.upper(),
			direction.upper(),
			f"qty={qty_val}",
			f"px={price_val}",
		]
		if exposure is not None:
			msg_parts.append(f"notional={exposure}")
		if note:
			msg_parts.append(note)
		await self.bot.send_notification(" ".join(str(x) for x in msg_parts))

	async def cancel_pending_entry_orders(self):
		if not self.bot._pending_entry_order_ids:
			return
		await self.bot._cancel_order_ids(self.bot._pending_entry_order_ids)
		self.bot._pending_entry_order_ids = []

	async def clear_pending_entry_state(self, clear_direction: bool = False):
		await self.cancel_pending_entry_orders()
		if clear_direction:
			self.bot._set_direction_all(None, lock=True, mode="zigzag_timing")
			self.bot.zigzag_stop_price = None
			self.bot.zigzag_entry_price = None
			self.bot.dynamic_stop_price = None
		self.bot.pending_entry = None
		self.bot.pending_break_price = None
		self.bot.pending_entry_static_mode = False
		self.bot._set_stop_new_orders(False, mode="zigzag_timing")
		self.bot._last_entry_attempt_ts = 0.0

	async def place_static_entry_orders(self, direction: str, break_price: Decimal, quantity: Decimal) -> List[str]:
		await self.cancel_pending_entry_orders()
		if quantity <= 0:
			return []
		max_per = self.bot.max_fast_close_qty if self.bot.max_fast_close_qty > 0 else quantity
		num_orders = int((quantity / max_per).to_integral_value(rounding=ROUND_HALF_UP))
		if num_orders * max_per < quantity:
			num_orders += 1
		prices = self.build_price_ladder(direction, break_price, max(num_orders, 1))
		remaining = quantity
		order_ids: List[str] = []
		for px in prices:
			if remaining <= 0:
				break
			qty = min(max_per, remaining)
			res = await self.place_post_only_limit(direction, qty, px, reduce_only=False)
			if res and res.success:
				order_ids.append(res.order_id)
				remaining -= qty
		self.bot._pending_entry_order_ids = order_ids
		return order_ids

	async def execute_zigzag_stop(self, close_side: str, best_bid: Decimal, best_ask: Decimal):
		await self.cancel_zigzag_tp()
		pos_abs = abs(await self.bot._get_position_signed_cached(force=True))
		ref_price = best_bid if close_side.lower() == "sell" else best_ask
		if pos_abs > 0:
			await self.post_only_exit_batch(close_side, pos_abs, best_bid, best_ask, wait_sec=5.0)
			await self.notify_zigzag_trade("exit", close_side, pos_abs, ref_price, note="stop-hit")
		self.bot._set_direction_all(None, mode="zigzag_timing")
		self.bot.pending_entry = None
		self.bot.pending_break_price = None
		self.bot.pending_entry_static_mode = False
		await self.cancel_pending_entry_orders()
		self.bot.zigzag_stop_price = None
		self.bot.zigzag_entry_price = None
		self.bot.dynamic_stop_price = None

	async def flatten_opposite(self, new_direction: str, best_bid: Decimal, best_ask: Decimal) -> bool:
		opposite_side = "sell" if new_direction == "buy" else "buy"
		await self.bot._cancel_orders_by_side(opposite_side)

		pos_signed = await self.bot._get_position_signed_cached(force=True)
		if (pos_signed > 0 and new_direction == "buy") or (pos_signed < 0 and new_direction == "sell"):
			self.bot._last_flatten_attempt_ts = 0.0
			return True

		pos_abs = abs(pos_signed)
		if pos_abs <= (self.bot.min_order_size or Decimal("0")):
			self.bot._last_flatten_attempt_ts = 0.0
			return True

		close_side = "sell" if pos_signed > 0 else "buy"
		now_ts = time.time()
		if (now_ts - self.bot._last_flatten_attempt_ts) < 5:
			return False
		self.bot._last_flatten_attempt_ts = now_ts
		_, remaining = await self.post_only_exit_batch(close_side, pos_abs, best_bid, best_ask, wait_sec=5.0)
		if remaining <= (self.bot.min_order_size or Decimal("0")):
			self.bot._last_flatten_attempt_ts = 0.0
			return True
		return False

	async def cancel_close_orders(self):
		try:
			active_orders = await self.bot._get_active_orders_cached()
			for order in active_orders:
				if order.side == self.config.close_order_side:
					try:
						await self.exchange_client.cancel_order(order.order_id)
					except Exception as e:
						self.logger.log(f"[REV-SLOW] Cancel close order {order.order_id} failed: {e}", "WARNING")
		except Exception as e:
			self.logger.log(f"[REV-SLOW] Fetch active orders failed: {e}", "WARNING")

	async def close_all_positions_and_orders(self):
		try:
			if hasattr(self.exchange_client, "cancel_all_orders"):
				await self.exchange_client.cancel_all_orders()
		except Exception as e:
			self.logger.log(f"Error cancelling all orders: {e}", "ERROR")

		try:
			pos_signed = await self.bot._get_position_signed_cached()
		except Exception as e:
			self.logger.log(f"Error fetching position during cleanup: {e}", "ERROR")
			pos_signed = Decimal(0)

		pos_abs = abs(pos_signed)
		if pos_abs > 0:
			close_side = 'sell' if pos_signed > 0 else 'buy'
			try:
				await self.exchange_client.reduce_only_close_with_retry(pos_abs, close_side)
			except Exception as e:
				self.logger.log(f"Error closing position during cleanup: {e}", "ERROR")

		if self.bot.current_sl_order_id:
			try:
				await self.exchange_client.cancel_order(self.bot.current_sl_order_id)
			except Exception:
				pass
		self.bot.current_sl_order_id = None

	async def close_position_in_chunks(self, close_side: str, allow_reverse_after: bool = False) -> Decimal:
		remaining = abs(await self.bot._get_position_signed_cached())
		max_chunk = self.bot.max_fast_close_qty if self.bot.max_fast_close_qty and self.bot.max_fast_close_qty > 0 else remaining
		loops = 0
		while remaining > 0 and loops < 20:
			chunk = min(remaining, max_chunk)
			try:
				await self.exchange_client.reduce_only_close_with_retry(chunk, close_side)
			except Exception as exc:
				self.logger.log(f"[FAST-CLOSE] Chunk close failed: {exc}", "WARNING")
			await asyncio.sleep(0.5)
			new_remaining = abs(await self.bot._get_position_signed_cached())
			if new_remaining < remaining:
				remaining = new_remaining
			else:
				loops += 1
				if loops % 3 == 0:
					await asyncio.sleep(1)
		return remaining

	async def reverse_position(self, new_direction: str, pivot_price: Decimal):
		if self.bot.reversing:
			return
		self.bot.reversing = True
		self.logger.log(f"[REV] Trigger reverse to {new_direction.upper()} via ZigZag at {pivot_price}", "WARNING")
		if hasattr(self.exchange_client, "cancel_all_orders"):
			try:
				await self.exchange_client.cancel_all_orders()
			except Exception as e:
				self.logger.log(f"[REV] Cancel all orders failed: {e}", "ERROR")

		try:
			pos_signed = await self.bot._get_position_signed_cached()
		except Exception as e:
			self.logger.log(f"[REV] Failed to fetch position: {e}", "ERROR")
			pos_signed = Decimal(0)

		pos_abs = abs(pos_signed)
		if pos_abs > 0:
			close_side = 'sell' if pos_signed > 0 else 'buy'
			remaining = await self.close_position_in_chunks(close_side, allow_reverse_after=True)
			if remaining >= self.bot.min_order_size:
				self.logger.log(f"[REV] Unable to fully close position, remaining {remaining}", "WARNING")

		self.bot._set_direction_all(new_direction, mode=self.bot.mode_tag)
		self.bot.dynamic_stop_price = None
		self.bot.dynamic_stop_direction = None
		self.bot.last_open_order_time = 0
		self.bot._invalidate_position_cache()
		self.bot._invalidate_order_cache()
		self.bot._set_stop_new_orders(False, mode=self.bot.mode_tag)

		if self.bot.enable_dynamic_sl:
			await self.bot._refresh_stop_loss(force=True)

		if self.bot.enable_notifications:
			await self.bot.send_notification(f"[DIRECTION] Switched to {new_direction.upper()} (fast reverse)")

		placed = await self.place_and_monitor_open_order()
		if not placed:
			self.logger.log("[REV] Failed to place new open order after fast reverse (stop_new_orders?)", "WARNING")
		self.bot.reversing = False

	async def schedule_slow_reverse(self, new_direction: str, pivot_price: Decimal):
		if self.bot.pending_reverse_state == "waiting_next_pivot" and self.bot.pending_reverse_direction == new_direction:
			return
		self.logger.log(f"[REV-SLOW] Observe next pivot for potential reverse to {new_direction.upper()} via ZigZag at {pivot_price}", "WARNING")
		self.bot.pending_reverse_direction = new_direction
		self.bot.pending_reverse_state = "waiting_next_pivot"
		self.bot.pending_original_direction = self.config.direction
		self.bot._set_stop_new_orders(True, mode=self.bot.mode_tag)

	async def resume_after_invalid_reverse(self):
		self.bot.pending_reverse_direction = None
		self.bot.pending_reverse_state = None
		self.bot.pending_original_direction = None
		self.bot._set_stop_new_orders(False, mode=self.bot.mode_tag)

	async def process_slow_reverse_followup(self, pivot) -> bool:
		if self.bot.pending_reverse_state != "waiting_next_pivot":
			return False

		if self.bot.pending_reverse_direction == "sell":
			if pivot.label == "LH":
				await self.bot._cancel_all_orders_safely()
				self.bot.pending_reverse_state = "unwinding"
				return True
			if pivot.label == "HH":
				await self.resume_after_invalid_reverse()
				return True

		if self.bot.pending_reverse_direction == "buy":
			if pivot.label == "HL":
				await self.bot._cancel_all_orders_safely()
				self.bot.pending_reverse_state = "unwinding"
				return True
			if pivot.label == "LL":
				await self.resume_after_invalid_reverse()
				return True

		return False

	async def perform_slow_unwind(self):
		try:
			pos_signed = await self.bot._get_position_signed_cached()
		except Exception as e:
			self.logger.log(f"[REV-SLOW] Failed to fetch position: {e}", "ERROR")
			return

		pos_abs = abs(pos_signed)
		if pos_abs == 0:
			if self.bot.pending_reverse_direction:
				self.bot._set_direction_all(self.bot.pending_reverse_direction, lock=False, mode=self.bot.mode_tag)
				self.bot.direction_lock = None
				self.bot.pending_reverse_direction = None
				self.bot.pending_reverse_state = None
				self.bot.pending_original_direction = None
				self.bot._set_stop_new_orders(False, mode=self.bot.mode_tag)
				self.bot.last_open_order_time = 0
				self.bot._invalidate_position_cache()
				self.bot._invalidate_order_cache()
				if self.bot.enable_notifications:
					await self.bot.send_notification(f"[DIRECTION] Switched to {self.config.direction.upper()} (slow reverse complete)")
				if self.bot.enable_dynamic_sl:
					await self.bot._refresh_stop_loss(force=True)
				placed = await self.place_and_monitor_open_order()
				if not placed:
					self.logger.log("[REV-SLOW] Failed to place new open after slow reverse", "WARNING")
			return

		close_side = 'sell' if pos_signed > 0 else 'buy'
		try:
			result = await self.exchange_client.reduce_only_close_with_retry(pos_abs, close_side)
			self.bot.last_release_attempt = time.time()
			try:
				remaining = abs(await self.bot._get_position_signed_cached(force=True))
				if remaining >= self.bot.min_order_size:
					self.logger.log(f"[REV-SLOW] Partial close during reverse, remaining {remaining}", "WARNING")
			except Exception:
				pass
			if hasattr(result, "success") and not getattr(result, "success", True):
				self.logger.log(f"[REV-SLOW] Close during reverse reported failure: {getattr(result, 'error_message', '')}", "WARNING")
		except Exception as e:
			self.logger.log(f"[REV-SLOW] Unwind attempt failed: {e}", "WARNING")

	async def execute_zigzag_stop_loss(self, close_side: str, best_bid: Decimal, best_ask: Decimal):
		await self.execute_zigzag_stop(close_side, best_bid, best_ask)

	async def place_and_monitor_open_order(self) -> bool:
		try:
			if self.bot.stop_new_orders or self.bot.webhook_block_trading:
				return False
			await self.bot._get_bbo_cached(force=True)
			await self.bot._get_last_trade_price_cached(force=True)
			dir_l = str(self.config.direction).lower()
			stop_threshold = self.bot.dynamic_stop_price if (self.bot.enable_dynamic_sl and self.bot.dynamic_stop_price is not None) else None
			if stop_threshold is not None:
				candidate_price = await self.exchange_client.get_order_price(self.config.direction)
				if self.bot._price_breaches_stop(stop_threshold, dir_l, Decimal(candidate_price)):
					self.logger.log(f"[OPEN] Skip new order: price {candidate_price} breaches stop {stop_threshold}", "WARNING")
					return False
			self.bot.order_filled_event.clear()
			self.bot.current_order_status = 'OPEN'
			self.bot.order_filled_amount = 0.0

			order_result = await self.exchange_client.place_open_order(
				self.config.contract_id,
				self.config.quantity,
				self.config.direction
			)
			if order_result and order_result.order_id:
				self.bot._invalidate_order_info_cache(order_result.order_id)

			if not order_result.success:
				return False

			if order_result.status == 'FILLED':
				return await self.handle_order_result(order_result)
			elif not self.bot.order_filled_event.is_set():
				try:
					await asyncio.wait_for(self.bot.order_filled_event.wait(), timeout=10)
				except asyncio.TimeoutError:
					pass
			await asyncio.sleep(0.25)

			handled = await self.handle_order_result(order_result)
			if handled and self.bot.enable_dynamic_sl:
				await self.bot._refresh_stop_loss()
			if handled:
				self.bot.last_new_order_time = time.time()
				self.bot._invalidate_order_cache()
				self.bot._invalidate_position_cache()
			return handled

		except Exception as e:
			self.logger.log(f"Error placing order: {e}", "ERROR")
			self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
		return False

	async def handle_order_result(self, order_result) -> bool:
		order_id = order_result.order_id
		filled_price = order_result.price
		if self.config.direction:
			self.bot._set_direction_all(self.config.direction, lock=False, mode=self.bot.mode_tag)

		if self.bot.order_filled_event.is_set() or order_result.status == 'FILLED':
			if self.config.boost_mode:
				close_order_result = await self.exchange_client.place_market_order(
					self.config.contract_id,
					self.config.quantity,
					self.config.close_order_side
				)
			else:
				self.bot.last_open_order_time = time.time()
				await self.bot._place_take_profit_order(self.config.quantity, filled_price)
				return True

		else:
			new_order_price = await self.exchange_client.get_order_price(self.config.direction)

			def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
				if direction == "buy":
					return new_order_price <= order_result_price
				elif direction == "sell":
					return new_order_price >= order_result_price
				return False

			current_order_status = None
			if self.config.exchange == "lighter":
				current_order_status = getattr(self.exchange_client.current_order, "status", None)
			if current_order_status is None:
				if not self.bot._order_ws_available():
					order_info = await self.bot._get_order_info_cached(order_id)
				else:
					order_info = await self.exchange_client.get_order_info(order_id)
				current_order_status = getattr(order_info, "status", None)
			if current_order_status is None:
				current_order_status = order_result.status or "OPEN"

			while (
				should_wait(self.config.direction, new_order_price, order_result.price)
				and current_order_status == "OPEN"
			):
				self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
				await asyncio.sleep(5)
				if self.config.exchange == "lighter":
					current_order_status = self.exchange_client.current_order.status
				else:
					if not self.bot._order_ws_available():
						order_info = await self.bot._get_order_info_cached(order_id)
					else:
						order_info = await self.exchange_client.get_order_info(order_id)
					if order_info is not None:
						current_order_status = order_info.status
				new_order_price = await self.exchange_client.get_order_price(self.config.direction)

			self.bot.order_canceled_event.clear()
			self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
			if self.config.exchange == "lighter":
				cancel_result = await self.exchange_client.cancel_order(order_id)
				start_time = time.time()
				while (time.time() - start_time < 10
					   and self.exchange_client.current_order.status not in ['CANCELED', 'FILLED', 'CANCELED-POST-ONLY']):
					await asyncio.sleep(0.1)

				if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED', 'CANCELED-POST-ONLY']:
					raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
				else:
					self.bot.order_filled_amount = self.exchange_client.current_order.filled_size
			else:
				try:
					cancel_result = await self.exchange_client.cancel_order(order_id)
					if not cancel_result.success:
						self.bot.order_canceled_event.set()
						self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
					else:
						self.bot.current_order_status = "CANCELED"

				except Exception as e:
					self.bot.order_canceled_event.set()
					self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

				if self.config.exchange == "backpack" or self.config.exchange == "extended":
					self.bot.order_filled_amount = cancel_result.filled_size
				else:
					if not self.bot.order_canceled_event.is_set():
						try:
							await asyncio.wait_for(self.bot.order_canceled_event.wait(), timeout=5)
						except asyncio.TimeoutError:
							if not self.bot._order_ws_available():
								order_info = await self.bot._get_order_info_cached(order_id, force=True)
							else:
								order_info = await self.exchange_client.get_order_info(order_id)
							self.bot.order_filled_amount = order_info.filled_size
				self.bot._invalidate_order_info_cache(order_id)

			if self.bot.order_filled_amount > 0:
				close_side = self.config.close_order_side
				if self.config.boost_mode:
					close_order_result = await self.exchange_client.place_close_order(
						self.config.contract_id,
						self.bot.order_filled_amount,
						filled_price,
						close_side
					)
					success = close_order_result.success
				else:
					success = await self.bot._place_take_profit_order(self.bot.order_filled_amount, filled_price)

				self.bot.last_open_order_time = time.time()
				if self.config.boost_mode and not success:
					self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")

			return True

		return False


__all__ = ["EngineManager"]
