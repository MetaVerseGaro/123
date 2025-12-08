"""Basic risk evaluation logic extracted from trading_bot (pure functions)."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class BasicRiskInput:
    position_amt: Decimal
    max_position_limit: Optional[Decimal]
    min_order_size: Optional[Decimal]
    quantity: Decimal
    enable_basic_risk: bool
    enable_advanced_risk: bool
    basic_release_timeout_minutes: int
    basic_full_since: Optional[float]
    last_release_attempt_basic: float
    now: float


@dataclass
class BasicRiskDecision:
    stop_new_orders: bool
    basic_full_since: Optional[float]
    trim_excess_qty: Decimal
    release_qty: Decimal
    last_release_attempt_basic: float


def evaluate_basic_risk(data: BasicRiskInput) -> BasicRiskDecision:
    """Evaluate basic risk gates and release timing without side effects."""
    if not data.enable_basic_risk:
        return BasicRiskDecision(
            stop_new_orders=False,
            basic_full_since=None,
            trim_excess_qty=Decimal(0),
            release_qty=Decimal(0),
            last_release_attempt_basic=data.last_release_attempt_basic,
        )

    stop_new = False
    basic_full_since = data.basic_full_since
    trim_excess_qty = Decimal(0)
    release_qty = Decimal(0)
    last_release_attempt_basic = data.last_release_attempt_basic

    if data.max_position_limit is not None and data.position_amt >= data.max_position_limit:
        stop_new = True
        if basic_full_since is None:
            basic_full_since = data.now
        if data.position_amt > data.max_position_limit:
            trim_excess_qty = data.position_amt - data.max_position_limit

    if data.max_position_limit is not None and data.position_amt < data.max_position_limit:
        basic_full_since = None

    should_release_basic = (
        data.basic_release_timeout_minutes > 0
        and not data.enable_advanced_risk
        and basic_full_since is not None
        and (data.now - basic_full_since) > data.basic_release_timeout_minutes * 60
        and (data.now - last_release_attempt_basic) > data.basic_release_timeout_minutes * 60
    )
    if should_release_basic:
        release_qty = min(data.quantity, data.position_amt)
        if release_qty > 0:
            last_release_attempt_basic = data.now

    return BasicRiskDecision(
        stop_new_orders=stop_new,
        basic_full_since=basic_full_since,
        trim_excess_qty=trim_excess_qty,
        release_qty=release_qty,
        last_release_attempt_basic=last_release_attempt_basic,
    )


__all__ = [
    "BasicRiskInput",
    "BasicRiskDecision",
    "evaluate_basic_risk",
]
