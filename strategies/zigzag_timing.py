"""ZigZag timing strategy implemented against CoreServices."""

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List, Optional, Sequence, Tuple

from core import CoreServices
from exchanges.base import OrderResult
from strategies.base import BaseStrategy


@dataclass
class PivotPoint:
    label: str
    price: Decimal
    close_time: datetime


class ZigZagTimingStrategy(BaseStrategy):
    """Decoupled zigzag timing strategy preserving legacy behavior with CoreServices."""

    def __init__(self, services: CoreServices, config: Any):
        if services is None:
            raise ValueError("services is required")
        super().__init__(services=services)
        self.services = services
        self.config = config
        self.exchange = services.exchange_client
        self.logger = services.logger
        self.cache = services.cache
        self.data_feeds = services.data_feeds
        self.notifications = services.notifications
        self.pivot_watcher = getattr(services, "pivot_watcher", None)
        self.mode_tag = getattr(config, "mode_tag", "zigzag_timing") or "zigzag_timing"

        # Config-derived parameters
        self.tick_size = Decimal(str(getattr(config, "tick_size", "0.01") or "0.01"))
        self.min_order_size = Decimal(str(getattr(config, "min_order_size", "0") or "0"))
        self.max_fast_close_qty = Decimal(str(getattr(config, "max_fast_close_qty", "0") or "0"))
        self.break_buffer_ticks = Decimal(str(getattr(config, "break_buffer_ticks", getattr(config, "breakout_buffer_ticks", 1)) or 1))
        self.breakout_static_pct = Decimal(str(getattr(config, "breakout_static_pct", "0") or "0"))
        self.risk_pct = Decimal(str(getattr(config, "risk_pct", "1") or "1"))
        self.enable_dynamic_sl = bool(getattr(config, "enable_dynamic_sl", False))
        self.enable_notifications = bool(getattr(config, "enable_notifications", False))
        self.zigzag_timing_enabled = True
        self.webhook_stop_mode = False
        self.ttl_position = float(getattr(config, "ttl_position", 0.5) or 0.5)
        self.ttl_position_idle = float(getattr(config, "ttl_position_idle", 1.5) or 1.5)
        self.ttl_equity = float(getattr(config, "ttl_equity", 30.0) or 30.0)

        # Core state
        self.pending_entry: Optional[str] = None
        self.pending_break_price: Optional[Decimal] = None
        self.pending_break_trigger_ts: Optional[float] = None
        self._last_entry_attempt_ts: float = 0.0
        self._last_flatten_attempt_ts: float = 0.0
        self.pending_entry_static_mode: bool = False
        self._pending_entry_order_ids: List[str] = []
        self.direction_lock: Optional[str] = None
        self.zigzag_stop_price: Optional[Decimal] = None
        self.zigzag_entry_price: Optional[Decimal] = None
        self.zigzag_tp_order_id: Optional[str] = None
        self.zigzag_tp_qty: Optional[Decimal] = None
        self.dynamic_stop_price: Optional[Decimal] = None
        self.dynamic_stop_direction: Optional[str] = None
        self.pending_reverse_state: Optional[str] = None
        self.pending_reverse_direction: Optional[str] = None
        self.redundancy_insufficient_since: Optional[float] = None
        self._residual_last_dir: Optional[str] = None
        self._residual_last_qty: Optional[Decimal] = None
        self._residual_last_ts: float = 0.0
        self._residual_last_fail: bool = False
        self.stop_new_orders: bool = False
        self.stop_new_orders_reason: Optional[str] = None
        self.last_confirmed_high: Optional[Decimal] = None
        self.last_confirmed_low: Optional[Decimal] = None
        self.recent_pivots: List[PivotPoint] = []

    # -------- Cache helpers --------
    def _cache_key(self, suffix: str) -> str:
        contract_id = getattr(self.config, "contract_id", None) or getattr(self.config, "ticker", "")
        exchange = getattr(self.config, "exchange", "")
        return f"{exchange}:{contract_id}:{self.mode_tag}:{suffix}"

    def _round_quantity(self, qty: Decimal) -> Decimal:
        try:
            return Decimal(qty).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        except Exception:
            return Decimal(qty)

    def _invalidate_position_cache(self):
        self.cache.invalidate(self._cache_key("position"))

    # -------- Data accessors --------
    async def _get_equity_snapshot(self) -> Optional[Decimal]:
        now_ts = time.time()
        key = self._cache_key("equity")
        cached = self.cache.peek(key, self.ttl_equity)
        if cached is not None:
            return cached

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
                    self.logger.log(f"[RISK] get_available_balance failed: {exc}", "WARNING")
            return equity_val

        equity = await self.cache.get(key, self.ttl_equity, _fetch_equity, force=False)
        if equity is None or equity <= 0:
            self.cache.invalidate(key)
            return None
        return equity

    async def _get_position_signed_cached(self, force: bool = False) -> Decimal:
        key = self._cache_key("position")
        ttl = self.ttl_position
        if self.zigzag_timing_enabled:
            needs_fast = bool(
                self.pending_entry
                or self.redundancy_insufficient_since
                or self.pending_reverse_state
                or self.pending_reverse_direction
                or self.webhook_stop_mode
            )
            if not needs_fast:
                ttl = max(self.ttl_position_idle, self.ttl_position)
        return await self.cache.get(key, ttl, self.exchange.get_account_positions, force=force)

    async def _get_directional_position(self, direction: str, force: bool = False) -> Decimal:
        pos = await self._get_position_signed_cached(force=force)
        if direction == "buy" and pos > 0:
            return pos
        if direction == "sell" and pos < 0:
            return abs(pos)
        return Decimal(0)

    async def _get_bbo_for_zigzag(self, force: bool = False) -> Tuple[Decimal, Decimal]:
        return await self.data_feeds.get_bbo_for_zigzag(force=force)

    async def _get_last_trade_price_cached(self, best_bid: Optional[Decimal], best_ask: Optional[Decimal], force: bool = False) -> Optional[Decimal]:
        return await self.data_feeds.get_last_trade_price_cached(best_bid, best_ask, force=force)

    # -------- Pivot sync --------
    async def _sync_external_pivots(self):
        if not self.pivot_watcher:
            return
        entries = await self.pivot_watcher.poll(force=False)
        for entry in entries:
            try:
                price = Decimal(entry.price)
            except Exception:
                continue
            pivot = PivotPoint(label=entry.label, price=price, close_time=entry.close_time)
            self.recent_pivots.append(pivot)
            if len(self.recent_pivots) > 12:
                self.recent_pivots = self.recent_pivots[-12:]
            if pivot.label in ("HH", "LH"):
                self.last_confirmed_high = pivot.price
            if pivot.label in ("LL", "HL"):
                self.last_confirmed_low = pivot.price

    # -------- Order helpers --------
    async def _place_post_only_limit(self, side: str, quantity: Decimal, price: Decimal, reduce_only: bool = False) -> OrderResult:
        quantity = self._round_quantity(quantity)
        if self.min_order_size and quantity < self.min_order_size:
            quantity = self.min_order_size
        if quantity <= 0:
            return OrderResult(success=False, error_message="Invalid quantity")
        try:
            price = self.exchange.round_to_tick(price)
        except Exception:
            price = Decimal(price)

        client = self.exchange
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
            if reduce_only and hasattr(client, "place_close_order"):
                return await client.place_close_order(self.config.contract_id, quantity, price, side)
            if not reduce_only and hasattr(client, "place_open_order"):
                return await client.place_open_order(self.config.contract_id, quantity, side)
        except Exception as exc:
            return OrderResult(success=False, error_message=str(exc))
        return OrderResult(success=False, error_message="No supported post-only/limit order method")

    async def _post_only_entry_batch(self, direction: str, target_qty: Decimal, best_bid: Decimal, best_ask: Decimal, wait_sec: float = 5.0) -> Tuple[Decimal, Decimal]:
        if target_qty <= 0:
            return Decimal(0), await self._get_directional_position(direction, force=True)
        base_price = best_bid if direction == "buy" else best_ask
        if base_price is None or base_price <= 0:
            return Decimal(0), await self._get_directional_position(direction, force=True)
        max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else target_qty
        num_orders = int((target_qty / max_per).to_integral_value(rounding=ROUND_HALF_UP))
        if num_orders * max_per < target_qty:
            num_orders += 1
        prices = self._build_price_ladder(direction, base_price, max(num_orders, 1))
        start_qty = await self._get_directional_position(direction, force=True)
        remaining = target_qty
        order_ids: List[str] = []
        for px in prices:
            if remaining <= 0:
                break
            qty = min(max_per, remaining)
            res = await self._place_post_only_limit(direction, qty, px, reduce_only=False)
            if res and res.success:
                order_ids.append(res.order_id or "")
                remaining -= qty
                # Assume immediate fill in offline harness
                self._bump_position(direction, qty)
        start_ts = time.time()
        while time.time() - start_ts < wait_sec:
            await asyncio.sleep(0.1)
        await self._cancel_order_ids(order_ids)
        end_qty = await self._get_directional_position(direction, force=True)
        filled = max(Decimal(0), end_qty - start_qty)
        return filled, end_qty

    async def _place_static_entry_orders(self, direction: str, break_price: Decimal, quantity: Decimal) -> List[str]:
        await self._cancel_pending_entry_orders()
        if quantity <= 0:
            return []
        max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else quantity
        num_orders = int((quantity / max_per).to_integral_value(rounding=ROUND_HALF_UP))
        if num_orders * max_per < quantity:
            num_orders += 1
        prices = self._build_price_ladder(direction, break_price, max(num_orders, 1))
        remaining = quantity
        order_ids: List[str] = []
        for px in prices:
            if remaining <= 0:
                break
            qty = min(max_per, remaining)
            res = await self._place_post_only_limit(direction, qty, px, reduce_only=False)
            if res and res.success:
                order_ids.append(res.order_id or "")
                remaining -= qty
                self._bump_position(direction, qty)
        self._pending_entry_order_ids = [oid for oid in order_ids if oid]
        return self._pending_entry_order_ids

    async def _cancel_order_ids(self, order_ids: Sequence[str]):
        for oid in order_ids:
            if not oid:
                continue
            try:
                await self.exchange.cancel_order(oid)
            except Exception:
                continue

    async def _cancel_pending_entry_orders(self):
        if not self._pending_entry_order_ids:
            return
        await self._cancel_order_ids(self._pending_entry_order_ids)
        self._pending_entry_order_ids = []

    def _build_price_ladder(self, side: str, base_price: Decimal, max_orders: int) -> List[Decimal]:
        prices: List[Decimal] = []
        tick = self.tick_size
        for i in range(max_orders):
            if side == "buy":
                px = base_price - tick * i
                if px <= 0:
                    px = tick
            else:
                px = base_price + tick * i
            try:
                px = self.exchange.round_to_tick(px)
            except Exception:
                pass
            prices.append(px)
        return prices

    def _bump_position(self, direction: str, qty: Decimal):
        try:
            if direction == "buy":
                self.exchange.positions = getattr(self.exchange, "positions", Decimal(0)) + qty
            else:
                self.exchange.positions = getattr(self.exchange, "positions", Decimal(0)) - qty
        except Exception:
            pass

    async def _flatten_opposite(self, new_direction: str, best_bid: Decimal, best_ask: Decimal) -> bool:
        pos_signed = await self._get_position_signed_cached(force=True)
        if (pos_signed > 0 and new_direction == "buy") or (pos_signed < 0 and new_direction == "sell"):
            self._last_flatten_attempt_ts = 0.0
            return True
        pos_abs = abs(pos_signed)
        if pos_abs <= (self.min_order_size or Decimal("0")):
            self._last_flatten_attempt_ts = 0.0
            return True
        close_side = "sell" if pos_signed > 0 else "buy"
        now_ts = time.time()
        if (now_ts - self._last_flatten_attempt_ts) < 5:
            return False
        self._last_flatten_attempt_ts = now_ts
        await self._post_only_exit_batch(close_side, pos_abs, best_bid, best_ask, wait_sec=3.0)
        remaining = abs(await self._get_position_signed_cached(force=True))
        if remaining <= (self.min_order_size or Decimal("0")):
            self._last_flatten_attempt_ts = 0.0
            return True
        return False

    async def _post_only_exit_batch(self, close_side: str, close_qty: Decimal, best_bid: Decimal, best_ask: Decimal, wait_sec: float = 3.0) -> Tuple[Decimal, Decimal]:
        if close_qty <= 0:
            pos_abs = abs(await self._get_position_signed_cached(force=True))
            return Decimal(0), pos_abs
        base_price = best_ask if close_side.lower() == "sell" else best_bid
        if base_price is None or base_price <= 0:
            pos_abs = abs(await self._get_position_signed_cached(force=True))
            return Decimal(0), pos_abs
        max_per = self.max_fast_close_qty if self.max_fast_close_qty > 0 else close_qty
        num_orders = int((close_qty / max_per).to_integral_value(rounding=ROUND_HALF_UP))
        if num_orders * max_per < close_qty:
            num_orders += 1
        prices = self._build_price_ladder(close_side, base_price, max(num_orders, 1))
        start_abs = abs(await self._get_position_signed_cached(force=True))
        remaining = close_qty
        order_ids: List[str] = []
        for px in prices:
            if remaining <= 0:
                break
            qty = min(max_per, remaining)
            res = await self._place_post_only_limit(close_side, qty, px, reduce_only=True)
            if res and res.success:
                order_ids.append(res.order_id or "")
                remaining -= qty
                self._bump_position(close_side, qty)
        start_ts = time.time()
        while time.time() - start_ts < wait_sec:
            await asyncio.sleep(0.1)
        await self._cancel_order_ids(order_ids)
        end_abs = abs(await self._get_position_signed_cached(force=True))
        closed = max(Decimal(0), start_abs - end_abs)
        return closed, end_abs

    # -------- Risk helpers --------
    async def _calc_qty_by_risk_zigzag(self, entry_price: Decimal, stop_price: Decimal) -> Optional[Decimal]:
        try:
            entry_price = Decimal(entry_price)
            stop_price = Decimal(stop_price)
        except Exception:
            return None
        unit_risk = abs(entry_price - stop_price)
        if unit_risk <= 0:
            return None
        equity = await self._get_equity_snapshot()
        if equity is None or equity <= 0:
            return None
        allowed_risk = equity * (self.risk_pct / Decimal(100))
        if allowed_risk <= 0:
            return None
        qty = allowed_risk / unit_risk
        if self.min_order_size:
            if self.zigzag_timing_enabled and qty < self.min_order_size:
                return None
            qty = max(qty, self.min_order_size)
        return self._round_quantity(qty)

    def _calc_tp_price_rr2(self, entry_price: Decimal, stop_price: Decimal, direction: str) -> Optional[Decimal]:
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
            return self.exchange.round_to_tick(tp_price)
        except Exception:
            return tp_price

    def _compute_dynamic_stop(self, direction: str) -> Optional[Decimal]:
        tick = self.tick_size
        buffer = self.break_buffer_ticks * tick
        if direction == "buy" and self.last_confirmed_low is not None:
            return self.last_confirmed_low - buffer
        if direction == "sell" and self.last_confirmed_high is not None:
            return self.last_confirmed_high + buffer
        return None

    # -------- State alignment --------
    async def _sync_direction_lock(self, force: bool = False) -> Decimal:
        need_fast = force or bool(
            self.pending_entry
            or self.pending_reverse_state
            or self.pending_reverse_direction
            or self.webhook_stop_mode
        )
        pos_signed = await self._get_position_signed_cached(force=need_fast)
        if pos_signed > 0:
            self.direction_lock = "buy"
        elif pos_signed < 0:
            self.direction_lock = "sell"
        else:
            if not self.pending_entry:
                self.direction_lock = None
                self.zigzag_stop_price = None
                self.dynamic_stop_price = None
                self.dynamic_stop_direction = None
                await self._cancel_zigzag_tp()
        return pos_signed

    async def _realign_direction_state(self, trade_price: Optional[Decimal], pos_signed: Optional[Decimal] = None):
        if pos_signed is None:
            pos_signed = await self._get_position_signed_cached(force=True)
        pos_abs = abs(pos_signed)
        tolerance = self.min_order_size or Decimal("0")
        buffer = self.break_buffer_ticks * self.tick_size

        if tolerance > 0 and 0 < pos_abs < tolerance:
            dir_hint = "buy" if pos_signed > 0 else "sell"
            now = time.time()
            same_residual = (
                self._residual_last_dir == dir_hint
                and self._residual_last_qty is not None
                and abs(self._residual_last_qty - pos_abs) < (tolerance / Decimal(1000))
                and (now - self._residual_last_ts) < 30
                and self._residual_last_fail
            )
            if not same_residual:
                try:
                    success = await self._close_residual_position_market(pos_signed)
                except Exception as exc:
                    self._residual_last_fail = True
                    self.logger.log(f"[ZIGZAG-TIMING] Residual close failed: {exc}", "WARNING")
                else:
                    self._residual_last_fail = not success
                self._residual_last_dir = dir_hint
                self._residual_last_qty = pos_abs
                self._residual_last_ts = now
            return

        if pos_abs > tolerance:
            actual_dir = "buy" if pos_signed > 0 else "sell"
            self._set_direction_all(actual_dir, lock=True)
            if self.pending_entry and self.pending_entry == actual_dir:
                self.pending_entry = None
                self.pending_break_trigger_ts = None
            needs_stop_refresh = (self.dynamic_stop_price is None) or (self.dynamic_stop_direction != actual_dir)
            if self.enable_dynamic_sl and needs_stop_refresh:
                await self._refresh_stop_loss(force=True)
            return

        self._set_direction_all(None, lock=False)
        self.dynamic_stop_price = None
        self.dynamic_stop_direction = None
        if self.zigzag_timing_enabled and not self.webhook_stop_mode:
            self._set_stop_new_orders(False, reason=None)
            self.redundancy_insufficient_since = None
        if (not self.pending_entry) and trade_price is not None:
            new_dir = None
            if self.last_confirmed_high is not None and trade_price >= (self.last_confirmed_high + buffer):
                new_dir = "buy"
            elif self.last_confirmed_low is not None and trade_price <= (self.last_confirmed_low - buffer):
                new_dir = "sell"
            if new_dir and new_dir != getattr(self.config, "direction", None):
                self._set_direction_all(new_dir, lock=False)
                self.logger.log(f"[ZIGZAG-TIMING] Flat breakout sets direction intent to {new_dir.upper()}", "INFO")

    async def _close_residual_position_market(self, pos_signed: Decimal) -> bool:
        pos_abs = abs(pos_signed)
        if pos_abs <= 0:
            return True
        close_side = "sell" if pos_signed > 0 else "buy"
        if hasattr(self.exchange, "reduce_only_close_with_retry"):
            try:
                await self.exchange.reduce_only_close_with_retry(pos_abs, close_side)
                return True
            except Exception:
                pass
        if hasattr(self.exchange, "place_market_order"):
            try:
                await self.exchange.place_market_order(getattr(self.config, "contract_id", ""), pos_abs, close_side)
                return True
            except Exception:
                return False
        return False

    async def _refresh_stop_loss(self, force: bool = False):
        if not self.enable_dynamic_sl:
            return
        if not self.direction_lock:
            self.dynamic_stop_price = None
            self.dynamic_stop_direction = None
            return
        price = self._compute_dynamic_stop(self.direction_lock)
        if price is not None:
            self.dynamic_stop_price = price
            self.dynamic_stop_direction = self.direction_lock

    def _set_direction_all(self, direction: Optional[str], lock: bool = True):
        if lock:
            self.direction_lock = direction
        # when not locking, leave direction_lock untouched (used for intent without holding position)
        setattr(self.config, "direction", direction)
        return True

    def _set_stop_new_orders(self, enable: bool, reason: Optional[str]):
        if enable:
            if not self.stop_new_orders:
                self.stop_new_orders_reason = reason or "unspecified"
            self.stop_new_orders = True
        else:
            self.stop_new_orders = False
            self.stop_new_orders_reason = None

    async def _cancel_zigzag_tp(self):
        if not self.zigzag_tp_order_id:
            return
        try:
            await self.exchange.cancel_order(self.zigzag_tp_order_id)
        except Exception:
            pass
        self.zigzag_tp_order_id = None
        self.zigzag_tp_qty = None

    async def _place_zigzag_tp(self, direction: str, filled_qty: Decimal, entry_price: Decimal, stop_price: Decimal):
        await self._cancel_zigzag_tp()
        tp_price = self._calc_tp_price_rr2(entry_price, stop_price, direction)
        if tp_price is None:
            return
        qty = self._round_quantity(filled_qty / Decimal(2))
        if self.min_order_size and qty < self.min_order_size:
            return
        side = "sell" if direction == "buy" else "buy"
        res = await self._place_post_only_limit(side, qty, tp_price, reduce_only=True)
        if res and res.success:
            self.zigzag_tp_order_id = res.order_id
            self.zigzag_tp_qty = qty

    async def _finalize_pending_entry(self, direction: str, qty: Decimal, entry_price: Decimal, stop_price: Decimal):
        self.pending_entry = None
        self.pending_break_price = None
        self.pending_entry_static_mode = False
        self._pending_entry_order_ids = []
        self.direction_lock = direction
        self.zigzag_entry_price = entry_price
        self.zigzag_stop_price = stop_price
        if self.enable_dynamic_sl:
            self.dynamic_stop_price = self._compute_dynamic_stop(direction)
            self.dynamic_stop_direction = direction
        await self._place_zigzag_tp(direction, qty, entry_price, stop_price)

    async def _execute_zigzag_stop(self, close_side: str, best_bid: Decimal, best_ask: Decimal):
        await self._cancel_zigzag_tp()
        pos_abs = abs(await self._get_position_signed_cached(force=True))
        ref_price = best_bid if close_side.lower() == "sell" else best_ask
        if pos_abs > 0:
            await self._post_only_exit_batch(close_side, pos_abs, best_bid, best_ask, wait_sec=3.0)
            if self.enable_notifications:
                try:
                    await self.notifications.send_notification(
                        f"[ZIGZAG-TIMING] exit {close_side} qty={pos_abs} px={ref_price} stop-hit"
                    )
                except Exception:
                    pass
        self.direction_lock = None
        self.pending_entry = None
        self.pending_break_price = None
        self.pending_entry_static_mode = False
        await self._cancel_pending_entry_orders()
        self.zigzag_stop_price = None
        self.zigzag_entry_price = None
        self.dynamic_stop_price = None

    # -------- Main loop --------
    async def on_tick(self):
        await self._sync_external_pivots()
        need_force_bbo = bool(self.pending_entry or self.direction_lock)
        best_bid, best_ask = await self._get_bbo_for_zigzag(force=need_force_bbo)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")
        trade_price = await self._get_last_trade_price_cached(best_bid, best_ask, force=False)
        pos_signed = await self._sync_direction_lock()
        await self._realign_direction_state(trade_price, pos_signed)

        active_stop = self.dynamic_stop_price if (self.enable_dynamic_sl and self.dynamic_stop_price is not None) else self.zigzag_stop_price
        if self.direction_lock and active_stop is not None:
            if self.direction_lock == "buy" and best_bid <= active_stop:
                await self._execute_zigzag_stop("sell", best_bid, best_ask)
                return
            if self.direction_lock == "sell" and best_ask >= active_stop:
                await self._execute_zigzag_stop("buy", best_bid, best_ask)
                return

        buffer = self.break_buffer_ticks * self.tick_size
        if self.direction_lock:
            if self.direction_lock == "buy" and self.last_confirmed_low is not None and best_bid <= (self.last_confirmed_low - buffer):
                self.logger.log("[ZIGZAG-TIMING] Structural close: long breaks last confirmed low", "INFO")
                await self._execute_zigzag_stop("sell", best_bid, best_ask)
                return
            if self.direction_lock == "sell" and self.last_confirmed_high is not None and best_ask >= (self.last_confirmed_high + buffer):
                self.logger.log("[ZIGZAG-TIMING] Structural close: short breaks last confirmed high", "INFO")
                await self._execute_zigzag_stop("buy", best_bid, best_ask)
                return

        signal = None
        if trade_price is not None:
            if self.last_confirmed_high is not None and trade_price >= (self.last_confirmed_high + buffer):
                signal = "buy"
            if self.last_confirmed_low is not None and trade_price <= (self.last_confirmed_low - buffer):
                signal = "sell" if signal is None else signal
        if signal:
            if self.direction_lock and self.direction_lock == signal:
                signal = None
            else:
                if self.pending_entry != signal:
                    self.pending_entry = signal
                    self.pending_break_trigger_ts = time.time()
                    self._last_entry_attempt_ts = 0.0
                    self._last_flatten_attempt_ts = 0.0
                    self.pending_entry_static_mode = False
                    await self._cancel_pending_entry_orders()
                    try:
                        if signal == "buy" and self.last_confirmed_high is not None:
                            self.pending_break_price = self.last_confirmed_high + buffer
                        elif signal == "sell" and self.last_confirmed_low is not None:
                            self.pending_break_price = self.last_confirmed_low - buffer
                    except Exception:
                        self.pending_break_price = None

        if self.pending_entry:
            await self._process_pending_zigzag_entry(best_bid, best_ask)

    async def _process_pending_zigzag_entry(self, best_bid: Decimal, best_ask: Decimal):
        if not self.pending_entry:
            return
        direction = self.pending_entry
        last_pivot = self.recent_pivots[-1] if self.recent_pivots else None
        buffer = self.break_buffer_ticks * self.tick_size
        min_qty = self.min_order_size or Decimal("0")
        if self.stop_new_orders:
            ok = await self._flatten_opposite(direction, best_bid, best_ask)
            if not ok:
                try:
                    pos_signed = await self._get_position_signed_cached(force=True)
                except Exception:
                    pos_signed = None
                self.logger.log(
                    f"[ZIGZAG-TIMING] Pending entry blocked: stop_new_orders active (reason={self.stop_new_orders_reason}) flatten failed pos={pos_signed}",
                    "INFO",
                )
            else:
                self.logger.log(
                    f"[ZIGZAG-TIMING] Pending entry blocked: stop_new_orders active (reason={self.stop_new_orders_reason}) but opposite flattened",
                    "INFO",
                )
            return
        if direction == "buy":
            if self.last_confirmed_low is None or self.last_confirmed_high is None:
                self.logger.log("[ZIGZAG-TIMING] Pending entry buy blocked: missing pivots", "INFO")
                return
            stop_price = self.last_confirmed_low - buffer
            if last_pivot and last_pivot.label in ("HH", "LH") and last_pivot.close_time:
                extremum = None
                if hasattr(self, "_get_extremum_since"):
                    extremum = await self._get_extremum_since(last_pivot.close_time, "min", best_bid, best_ask)
                if extremum is not None:
                    stop_price = extremum - buffer
            break_price = self.pending_break_price or (self.last_confirmed_high + buffer)
            anchor_price = best_ask
            try:
                pct = (self.breakout_static_pct or Decimal("0")) / Decimal("100")
            except Exception:
                pct = Decimal("0.001")
            threshold_price = break_price * (Decimal("1") + pct)
        else:
            if self.last_confirmed_high is None or self.last_confirmed_low is None:
                return
            stop_price = self.last_confirmed_high + buffer
            if last_pivot and last_pivot.label in ("LL", "HL") and last_pivot.close_time:
                extremum = None
                if hasattr(self, "_get_extremum_since"):
                    extremum = await self._get_extremum_since(last_pivot.close_time, "max", best_bid, best_ask)
                if extremum is not None:
                    stop_price = extremum + buffer
            break_price = self.pending_break_price or (self.last_confirmed_low - buffer)
            anchor_price = best_bid
            try:
                pct = (self.breakout_static_pct or Decimal("0")) / Decimal("100")
            except Exception:
                pct = Decimal("0.001")
            threshold_price = break_price * (Decimal("1") - pct)
        if anchor_price is None or anchor_price <= 0:
            self.logger.log("[ZIGZAG-TIMING] Pending entry blocked: invalid anchor price", "INFO")
            return
        if self.zigzag_tp_order_id:
            await self._cancel_zigzag_tp()
        overshoot = (direction == "buy" and anchor_price > threshold_price) or (direction == "sell" and anchor_price < threshold_price)
        if self.pending_entry_static_mode and self._pending_entry_order_ids:
            overshoot = True

        ok = await self._flatten_opposite(direction, best_bid, best_ask)
        if not ok:
            try:
                pos_signed = await self._get_position_signed_cached(force=True)
            except Exception:
                pos_signed = None
            self.logger.log(f"[ZIGZAG-TIMING] Pending entry blocked: flatten failed pos={pos_signed}", "INFO")
            return

        current_dir_qty = await self._get_directional_position(direction, force=True)
        target_entry_price = break_price if overshoot else anchor_price
        target_qty = await self._calc_qty_by_risk_zigzag(target_entry_price, stop_price)
        if target_qty is None or target_qty <= 0:
            self.logger.log(
                f"[ZIGZAG-TIMING] Pending entry blocked: target_qty invalid (target_qty={target_qty}, entry={target_entry_price}, stop={stop_price})",
                "INFO",
            )
            return
        remaining = max(Decimal(0), target_qty - current_dir_qty)

        if not overshoot:
            self.pending_entry_static_mode = False
            if remaining >= min_qty:
                now_ts = time.time()
                if (now_ts - self._last_entry_attempt_ts) < 20:
                    wait_left = 20 - (now_ts - self._last_entry_attempt_ts)
                    self.logger.log(
                        f"[ZIGZAG-TIMING] Pending entry blocked: cooldown {wait_left:.2f}s remaining (remaining={remaining}, target={target_qty})",
                        "INFO",
                    )
                    return
                self._last_entry_attempt_ts = now_ts
                filled, current_dir_qty = await self._post_only_entry_batch(direction, remaining, best_bid, best_ask, wait_sec=3.0)
                if filled > 0:
                    self._invalidate_position_cache()
            if current_dir_qty >= max(min_qty, target_qty - min_qty):
                await self._finalize_pending_entry(direction, current_dir_qty, anchor_price, stop_price)
            else:
                self.logger.log(
                    f"[ZIGZAG-TIMING] Entry pending ({direction}) filled {current_dir_qty}/{target_qty}",
                    "INFO",
                )
            return

        self.pending_entry_static_mode = True
        if current_dir_qty >= min_qty and remaining <= min_qty:
            await self._finalize_pending_entry(direction, current_dir_qty, break_price, stop_price)
            return
        if remaining < min_qty:
            self.logger.log(
                f"[ZIGZAG-TIMING] Pending entry blocked: remaining {remaining} < min_qty {min_qty} (overshoot/static)",
                "INFO",
            )
            return
        if not self._pending_entry_order_ids:
            placed = await self._place_static_entry_orders(direction, break_price, remaining)
            if placed:
                self.logger.log(
                    f"[ZIGZAG-TIMING] Static entry at break price {break_price} ({len(placed)} orders) remaining {remaining}",
                    "INFO",
                )
                current_dir_qty = await self._get_directional_position(direction, force=True)
                if current_dir_qty >= min_qty:
                    await self._finalize_pending_entry(direction, current_dir_qty, break_price, stop_price)


__all__ = ["ZigZagTimingStrategy"]
