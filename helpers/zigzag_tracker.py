from dataclasses import dataclass
from typing import Optional, Tuple
from decimal import Decimal
import time


@dataclass
class ZigZagEvent:
    label: str  # HH, HL, LH, LL
    price: Decimal
    direction: int  # +1 up, -1 down
    timestamp: float


class ZigZagTracker:
    """
    Lightweight ZigZag tracker approximating TradingView ZigZag++ behaviour.
    Designed for streaming best bid/ask updates; uses deviation (percent) and backstep filters.
    """

    def __init__(self, depth: int = 15, deviation_pct: Decimal = Decimal("5"), backstep: int = 2):
        self.depth = depth
        self.deviation_pct = deviation_pct
        self.backstep = backstep

        self.last_pivot_price: Optional[Decimal] = None
        self.last_pivot_type: Optional[str] = None  # "high" or "low"
        self.last_confirmed_high: Optional[Decimal] = None
        self.last_confirmed_low: Optional[Decimal] = None
        self.candidate_high: Optional[Decimal] = None
        self.candidate_low: Optional[Decimal] = None
        self.bars_since_pivot: int = 0
        self.direction: int = 0  # +1 up, -1 down

    def reset(self):
        self.__init__(self.depth, self.deviation_pct, self.backstep)

    def _deviation_price(self, price: Decimal) -> Decimal:
        return price * self.deviation_pct / Decimal(100)

    def update(self, high: Decimal, low: Decimal, ts: Optional[float] = None) -> Optional[ZigZagEvent]:
        """
        Feed a new high/low sample. Returns a ZigZagEvent when a new pivot confirms.
        """
        ts = ts or time.time()
        self.bars_since_pivot += 1

        # Initialize pivot
        if self.last_pivot_price is None:
            mid = (high + low) / 2
            self.last_pivot_price = mid
            self.last_pivot_type = "low"
            self.last_confirmed_low = mid
            self.candidate_high = high
            self.candidate_low = low
            self.direction = 0
            self.bars_since_pivot = 0
            return None

        dev = self._deviation_price(self.last_pivot_price)

        event: Optional[ZigZagEvent] = None

        if self.direction >= 0:
            # Looking for high swing
            if self.candidate_high is None or high > self.candidate_high:
                self.candidate_high = high

            price_move = self.candidate_high - self.last_pivot_price
            if price_move >= dev and self.bars_since_pivot >= self.backstep:
                prev_high = self.last_confirmed_high
                self.last_confirmed_high = self.candidate_high
                self.last_pivot_price = self.candidate_high
                self.last_pivot_type = "high"
                self.direction = 1
                self.bars_since_pivot = 0
                self.candidate_low = low
                label = "HH" if prev_high is not None and self.candidate_high > prev_high else "LH"
                event = ZigZagEvent(label=label, price=self.candidate_high, direction=self.direction, timestamp=ts)
        if self.direction <= 0 or event is None:
            # Looking for low swing
            if self.candidate_low is None or low < self.candidate_low:
                self.candidate_low = low

            price_move = self.last_pivot_price - self.candidate_low
            if price_move >= dev and self.bars_since_pivot >= self.backstep:
                prev_low = self.last_confirmed_low
                self.last_confirmed_low = self.candidate_low
                self.last_pivot_price = self.candidate_low
                self.last_pivot_type = "low"
                self.direction = -1
                self.bars_since_pivot = 0
                self.candidate_high = high
                label = "LL" if prev_low is not None and self.candidate_low < prev_low else "HL"
                event = ZigZagEvent(label=label, price=self.candidate_low, direction=self.direction, timestamp=ts)

        return event
