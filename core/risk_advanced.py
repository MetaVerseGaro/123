"""Advanced risk evaluation logic extracted from trading_bot (pure functions)."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class AdvancedRiskInput:
    position_signed: Decimal
    position_amt: Decimal
    avg_price: Decimal
    stop_price: Optional[Decimal]
    equity: Optional[Decimal]
    risk_pct: Decimal
    quantity: Decimal
    min_order_size: Optional[Decimal]
    enable_advanced_risk: bool
    stop_loss_enabled: bool
    redundancy_insufficient_since: Optional[float]
    last_release_attempt_advanced: float
    release_timeout_minutes: int
    now: float


@dataclass
class AdvancedRiskDecision:
    stop_new_orders: bool
    redundancy_insufficient_since: Optional[float]
    allowed_position: Decimal
    trim_excess_qty: Decimal
    headroom: Decimal
    release_qty: Decimal
    last_release_attempt_advanced: float


def evaluate_advanced_risk(data: AdvancedRiskInput) -> AdvancedRiskDecision:
    """Evaluate redundancy-based gating and release timing without side effects."""
    if not (data.enable_advanced_risk and data.stop_loss_enabled):
        return AdvancedRiskDecision(
            stop_new_orders=False,
            redundancy_insufficient_since=None,
            allowed_position=Decimal(0),
            trim_excess_qty=Decimal(0),
            headroom=Decimal(0),
            release_qty=Decimal(0),
            last_release_attempt_advanced=data.last_release_attempt_advanced,
        )

    if data.position_amt <= 0 or data.stop_price is None or data.equity is None:
        return AdvancedRiskDecision(
            stop_new_orders=False,
            redundancy_insufficient_since=None,
            allowed_position=Decimal(0),
            trim_excess_qty=Decimal(0),
            headroom=Decimal(0),
            release_qty=Decimal(0),
            last_release_attempt_advanced=data.last_release_attempt_advanced,
        )

    if data.position_signed > 0:
        per_base_loss = data.avg_price - data.stop_price
    else:
        per_base_loss = data.stop_price - data.avg_price

    if per_base_loss <= 0:
        return AdvancedRiskDecision(
            stop_new_orders=False,
            redundancy_insufficient_since=None,
            allowed_position=Decimal(0),
            trim_excess_qty=Decimal(0),
            headroom=Decimal(0),
            release_qty=Decimal(0),
            last_release_attempt_advanced=data.last_release_attempt_advanced,
        )

    max_loss = data.equity * (data.risk_pct / Decimal(100))
    allowed_position = max(Decimal(0), max_loss / per_base_loss)
    headroom = allowed_position - data.position_amt
    trim_excess_qty = max(Decimal(0), data.position_amt - allowed_position)

    stop_new = False
    redundancy_insufficient_since = data.redundancy_insufficient_since
    if headroom < data.quantity:
        stop_new = True
        if redundancy_insufficient_since is None:
            redundancy_insufficient_since = data.now
    else:
        redundancy_insufficient_since = None

    release_qty = Decimal(0)
    last_release_attempt_advanced = data.last_release_attempt_advanced
    should_release = (
        stop_new
        and redundancy_insufficient_since is not None
        and data.release_timeout_minutes > 0
        and (data.now - redundancy_insufficient_since) > data.release_timeout_minutes * 60
        and (data.now - last_release_attempt_advanced) > data.release_timeout_minutes * 60
    )
    if should_release:
        release_qty = min(data.quantity, data.position_amt)
        if release_qty > 0:
            last_release_attempt_advanced = data.now

    return AdvancedRiskDecision(
        stop_new_orders=stop_new,
        redundancy_insufficient_since=redundancy_insufficient_since,
        allowed_position=allowed_position,
        trim_excess_qty=trim_excess_qty,
        headroom=headroom,
        release_qty=release_qty,
        last_release_attempt_advanced=last_release_attempt_advanced,
    )


__all__ = [
    "AdvancedRiskInput",
    "AdvancedRiskDecision",
    "evaluate_advanced_risk",
]
