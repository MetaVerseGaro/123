from dataclasses import dataclass
from typing import Optional, List, Tuple
from decimal import Decimal
import time


@dataclass
class ZigZagEvent:
    label: str  # HH, HL, LH, LL
    price: Decimal
    direction: int  # +1 up, -1 down
    timestamp: float


@dataclass
class _Pivot:
    idx: int
    price: Decimal
    kind: str  # "high" or "low"
    ts: float


class ZigZagTracker:
    """
    ZigZag tracker aligned with Pine ZigZag depth/deviation/backstep semantics:
    - 深度 depth: 需要左右各 depth 根才能确认 pivot（类似 ta.pivothigh/highest 与 ta.pivotlow/lowest）。
    - backstep: 同向 pivot 之间若距离不超过 backstep，会用更极值的那个替换旧 pivot。
    - deviation_pct: 新 pivot 与上一 pivot 的涨跌幅（百分比）必须达到该阈值才确认反向。
    """

    def __init__(self, depth: int = 15, deviation_pct: Decimal = Decimal("5"), backstep: int = 2):
        self.depth = depth
        self.deviation_pct = deviation_pct
        self.backstep = backstep

        self.candles: List[Tuple[Decimal, Decimal, float]] = []  # (high, low, ts)
        self.last_pivot: Optional[_Pivot] = None
        self.last_confirmed_high: Optional[Decimal] = None
        self.last_confirmed_low: Optional[Decimal] = None

    def reset(self):
        self.__init__(self.depth, self.deviation_pct, self.backstep)

    def _change_pct(self, new_price: Decimal, old_price: Decimal, kind: str) -> Decimal:
        if old_price == 0:
            return Decimal(0)
        if kind == "high":
            return (new_price - old_price) / old_price * Decimal(100)
        return (old_price - new_price) / old_price * Decimal(100)

    def _register_pivot(self, idx: int, price: Decimal, kind: str, ts: float) -> Optional[ZigZagEvent]:
        # 同向 pivot 近距离更极值 -> 替换，不产生新事件
        if self.last_pivot and self.last_pivot.kind == kind:
            if idx - self.last_pivot.idx <= self.backstep:
                if (kind == "high" and price > self.last_pivot.price) or (kind == "low" and price < self.last_pivot.price):
                    self.last_pivot = _Pivot(idx=idx, price=price, kind=kind, ts=ts)
                    if kind == "high":
                        self.last_confirmed_high = price
                    else:
                        self.last_confirmed_low = price
                return None

        # 反向时要求达到 deviation 百分比
        if self.last_pivot:
            move_pct = self._change_pct(price, self.last_pivot.price, kind)
            if move_pct < self.deviation_pct:
                return None

        # 生成事件并更新状态
        if kind == "high":
            prev_high = self.last_confirmed_high
            self.last_confirmed_high = price
            label = "HH" if prev_high is not None and price > prev_high else "LH"
            direction = 1
        else:
            prev_low = self.last_confirmed_low
            self.last_confirmed_low = price
            label = "LL" if prev_low is not None and price < prev_low else "HL"
            direction = -1

        self.last_pivot = _Pivot(idx=idx, price=price, kind=kind, ts=ts)
        return ZigZagEvent(label=label, price=price, direction=direction, timestamp=ts)

    def update(self, high: Decimal, low: Decimal, ts: Optional[float] = None) -> Optional[ZigZagEvent]:
        """
        推入一根 K 线的 high/low；当确认 pivot 时返回 ZigZagEvent。
        采用类似 ta.pivothigh/pivotlow(depth, depth) 的对称确认方式，再结合 backstep/deviation 过滤。
        """
        ts = ts or time.time()
        high = Decimal(high)
        low = Decimal(low)
        self.candles.append((high, low, ts))

        if len(self.candles) < 2 * self.depth + 1:
            return None

        pivot_idx = len(self.candles) - self.depth - 1  # 待确认的 pivot 位置（左右各 depth 根已具备）
        if pivot_idx < self.depth:
            return None

        window = self.candles[pivot_idx - self.depth:pivot_idx + self.depth + 1]
        pivot_high = max(x[0] for x in window)
        pivot_low = min(x[1] for x in window)
        cur_high, cur_low, cur_ts = self.candles[pivot_idx]

        candidates = []
        if cur_high >= pivot_high:
            score = self._change_pct(cur_high, self.last_pivot.price, "high") if self.last_pivot else cur_high - cur_low
            candidates.append(("high", cur_high, score))
        if cur_low <= pivot_low:
            score = self._change_pct(cur_low, self.last_pivot.price, "low") if self.last_pivot else cur_high - cur_low
            candidates.append(("low", cur_low, score))

        if not candidates:
            return None

        # 选取最有意义的候选（优先幅度更大的）
        candidates.sort(key=lambda x: x[2], reverse=True)
        kind, price, _ = candidates[0]
        return self._register_pivot(pivot_idx, price, kind, cur_ts)
