import sys
import time
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from core.risk_basic import BasicRiskInput, evaluate_basic_risk
from core.risk_advanced import AdvancedRiskInput, evaluate_advanced_risk


def test_basic_risk_trim_and_release():
    now = time.time()
    data = BasicRiskInput(
        position_amt=Decimal("6"),
        max_position_limit=Decimal("5"),
        min_order_size=Decimal("0.1"),
        quantity=Decimal("1"),
        enable_basic_risk=True,
        enable_advanced_risk=False,
        basic_release_timeout_minutes=1,
        basic_full_since=None,
        last_release_attempt_basic=0.0,
        now=now,
    )
    decision = evaluate_basic_risk(data)
    assert decision.stop_new_orders is True
    assert decision.trim_excess_qty == Decimal("1")
    assert decision.release_qty == Decimal("0")
    assert decision.basic_full_since == data.now

    # Advance time beyond timeout to trigger release
    later = now + 70
    release_decision = evaluate_basic_risk(
        BasicRiskInput(
            position_amt=Decimal("5"),
            max_position_limit=Decimal("5"),
            min_order_size=Decimal("0.1"),
            quantity=Decimal("1"),
            enable_basic_risk=True,
            enable_advanced_risk=False,
            basic_release_timeout_minutes=1,
            basic_full_since=decision.basic_full_since,
            last_release_attempt_basic=decision.last_release_attempt_basic,
            now=later,
        )
    )
    assert release_decision.release_qty == Decimal("1")
    assert release_decision.last_release_attempt_basic == later


def test_advanced_risk_stop_new_and_release():
    now = time.time()
    data = AdvancedRiskInput(
        position_signed=Decimal("5"),
        position_amt=Decimal("5"),
        avg_price=Decimal("105"),
        stop_price=Decimal("100"),
        equity=Decimal("1000"),
        risk_pct=Decimal("2"),
        quantity=Decimal("1"),
        min_order_size=Decimal("0.1"),
        enable_advanced_risk=True,
        stop_loss_enabled=True,
        redundancy_insufficient_since=None,
        last_release_attempt_advanced=0.0,
        release_timeout_minutes=1,
        now=now,
    )
    decision = evaluate_advanced_risk(data)
    # allowed_position = (1000 * 0.02) / 5 = 4; headroom = -1 -> stop new
    assert decision.allowed_position == Decimal("4")
    assert decision.stop_new_orders is True
    assert decision.redundancy_insufficient_since == data.now
    assert decision.trim_excess_qty == Decimal("1")
    assert decision.release_qty == Decimal("0")

    later = now + 70
    release_decision = evaluate_advanced_risk(
        AdvancedRiskInput(
            position_signed=Decimal("5"),
            position_amt=Decimal("5"),
            avg_price=Decimal("105"),
            stop_price=Decimal("100"),
            equity=Decimal("1000"),
            risk_pct=Decimal("2"),
            quantity=Decimal("1"),
            min_order_size=Decimal("0.1"),
            enable_advanced_risk=True,
            stop_loss_enabled=True,
            redundancy_insufficient_since=decision.redundancy_insufficient_since,
            last_release_attempt_advanced=decision.last_release_attempt_advanced,
            release_timeout_minutes=1,
            now=later,
        )
    )
    assert release_decision.release_qty == Decimal("1")
    assert release_decision.last_release_attempt_advanced == later


def test_advanced_risk_no_stop_when_headroom_ok():
    data = AdvancedRiskInput(
        position_signed=Decimal("2"),
        position_amt=Decimal("2"),
        avg_price=Decimal("105"),
        stop_price=Decimal("100"),
        equity=Decimal("1000"),
        risk_pct=Decimal("2"),
        quantity=Decimal("1"),
        min_order_size=Decimal("0.1"),
        enable_advanced_risk=True,
        stop_loss_enabled=True,
        redundancy_insufficient_since=None,
        last_release_attempt_advanced=0.0,
        release_timeout_minutes=1,
        now=time.time(),
    )
    decision = evaluate_advanced_risk(data)
    assert decision.stop_new_orders is False
    assert decision.redundancy_insufficient_since is None
    assert decision.trim_excess_qty == Decimal("0")
    assert decision.release_qty == Decimal("0")

