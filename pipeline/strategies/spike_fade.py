"""EXAMPLE strategy of the lab: short-only spike fade. It is also the benchmark.

Hypothesis: after a fast rise of the ETH price, the price falls back. When the mid price rises by
``jump_threshold_pct`` or more within ``lookback_s`` seconds, sell short with a post-only limit at
the best ask. The engine closes the short at the take-profit, the stop-loss or the time limit.

Result so far (see RESEARCH.md): this hypothesis is WRONG for 2024-2026. The strategy loses about
7-8 bp per trade. That is fine: it is an example of how to build, run and measure a strategy.
"""

import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal

from pipeline.strategies import Entry, Strategy


@dataclass(frozen=True)
class Params:
    jump_threshold_pct: Decimal = Decimal("0.50")  # 0.50 means 0.50%, not 50%
    lookback_s: float = 60.0
    take_profit_pct: Decimal = Decimal("0.15")
    stop_loss_pct: Decimal = Decimal("0.50")
    max_holding_s: float = 120.0
    cooldown_s: float = 30.0

    def __post_init__(self):
        for name in ("jump_threshold_pct", "take_profit_pct", "stop_loss_pct"):
            value = Decimal(str(getattr(self, name)))
            if not value.is_finite() or not 0 < value < 100:
                raise ValueError(f"{name} must be in (0, 100)")
            object.__setattr__(self, name, value)
        for name in ("lookback_s", "max_holding_s", "cooldown_s"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{name} must be a finite number")
            if value < 0 or (value == 0 and name != "cooldown_s"):
                raise ValueError(f"{name} must be positive")


class SpikeFade(Strategy):
    name = "spike_fade"
    Params = Params

    def __init__(self, params=None):
        super().__init__(params)
        self.history = deque()  # (exchange time in ms, mid price)
        self.jump_pct = None
        self.above_threshold = False

    def reset(self, reason):
        self.history.clear()
        self.jump_pct = None
        self.above_threshold = False

    def on_book(self, book, account):
        p = self.params
        self.history.append((book.exchange_ms, book.mid))
        cutoff = book.exchange_ms - p.lookback_s * 1000
        # Keep the observation immediately before the window boundary, not a future observation.
        while len(self.history) > 1 and self.history[1][0] <= cutoff:
            self.history.popleft()
        if self.history[0][0] > cutoff:
            self.jump_pct = None  # the lookback window is not complete yet
            return None
        self.jump_pct = (book.mid / self.history[0][1] - 1) * 100
        above = self.jump_pct >= p.jump_threshold_pct
        crossed = above and not self.above_threshold
        self.above_threshold = above
        # React only when the jump first crosses the threshold, not on each book above it.
        if not crossed or account.position or account.entry_pending:
            return None
        if account.last_exit_ms is not None and account.now_ms < account.last_exit_ms + p.cooldown_s * 1000:
            return None
        return Entry(
            side="short",
            style="maker",
            take_profit_pct=p.take_profit_pct,
            stop_loss_pct=p.stop_loss_pct,
            max_holding_s=p.max_holding_s,
            note={"jump_pct": self.jump_pct},
        )

    def status(self):
        return {"current_jump_pct": self.jump_pct}
