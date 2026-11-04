"""TEMPLATE for YOUR strategy. Copy this file, or edit it directly.

1. Write your hypothesis here, before you write code:
       "When ... happens, the ETH price ... within ... seconds, because ..."
2. Put your parameters into ``Params`` and into ``[strategies.my_strategy]`` in paper.toml.
3. Write your rule in ``on_book`` (and ``on_trade`` if you use trades).
4. Test it: ``uv run pytest -q``. Then run it next to the example strategy for 24 hours.

If you rename this file, use the new name in ``run = [...]`` in paper.toml.

What you get in each call
-------------------------
book     a ``pipeline.market.Book``:
           book.bid, book.ask, book.mid      best prices (Decimal)
           book.bids, book.asks              5 levels: ((price, size), ...) in ETH
           book.spread_bps                   (ask - bid) / mid in basis points
           book.exchange_ms                  exchange time in milliseconds
trade    a dict from the public trade feed:
           trade["px"], trade["sz"]          price and size (strings)
           trade["side"]                     "B" = a buyer took liquidity, "A" = a seller did
           trade["time"]                     exchange time in milliseconds
account  a ``pipeline.paper_engine.Account``:
           account.now_ms, account.balance_usdc, account.equity_usdc
           account.position                  None, or {"side", "size", "entry_price", "entry_ms"}
           account.entry_pending             True while an entry order waits for a fill
           account.last_exit_ms              time of the last closed trade (for a cooldown)

What you can return
-------------------
None                                         do nothing
Entry(side="long" or "short",                open ONE position (the engine sets price and size)
      max_holding_s=...,                     the engine closes the position after this time
      take_profit_pct=... or None,           percentage points: 0.20 means 0.20%
      stop_loss_pct=... or None,
      style="maker" or "taker",              maker: post-only limit at the touch (cheap, may not fill)
                                             taker: market order (fills, but pays 4.5 bp + slippage)
      note={...})                            any values that you want in trades.csv
Exit(reason="...")                           close the open position
"""

from collections import deque
from dataclasses import dataclass

from pipeline.strategies import Entry, Exit, Strategy  # noqa: F401  (Exit: for your own exits)


@dataclass(frozen=True)
class Params:
    # Your parameters. Use Decimal for prices and percentages, float for seconds.
    lookback_s: float = 60.0
    max_holding_s: float = 300.0


class MyStrategy(Strategy):
    name = "my_strategy"
    Params = Params

    def __init__(self, params=None):
        super().__init__(params)
        self.history = deque()  # (exchange_ms, mid): keep only what your rule needs

    def reset(self, reason):
        # After a data gap the history has a hole. Start again.
        self.history.clear()

    def on_book(self, book, account):
        p = self.params
        self.history.append((book.exchange_ms, book.mid))
        while self.history and self.history[0][0] < book.exchange_ms - p.lookback_s * 1000:
            self.history.popleft()

        if account.position or account.entry_pending:
            return None

        # TODO: write your rule here. For example (this is not a recommendation):
        #
        #   change_pct = (book.mid / self.history[0][1] - 1) * 100
        #   if change_pct <= Decimal("-1.0"):
        #       return Entry(side="long", style="taker", max_holding_s=p.max_holding_s,
        #                    note={"change_pct": change_pct})
        return None

    def on_trade(self, trade, account):
        return None

    def status(self):
        return {"history_points": len(self.history)}
