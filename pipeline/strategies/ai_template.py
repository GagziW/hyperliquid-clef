"""TEMPLATE: connect an AI model to a paper strategy.

The model runs in ``background`` (slow, every ``interval_s`` seconds). ``on_book`` (fast, about
two times each second) only reads the latest model answer. If the answer is too old or missing,
the strategy does not trade. Code, not the model, sets price, size and exits.

With ``provider = "mock"`` (the default) no key is necessary: the mock model always says HOLD,
so this template never trades. To use a real model:

- ``provider = "jev"`` needs ``TYPESAFE_API_KEY`` in the environment,
- ``provider = "clef"`` needs ``CLOUDFLARE_ACCOUNT_ID`` and ``CLOUDFLARE_AUTH_TOKEN``,
- or write your own ``decide()`` for another API (for example Claude). It must return a
  ``pipeline.decision.Decision``.

Never put keys into this file or into paper.toml. Model calls can cost money.
"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal

from pipeline.decision import Model
from pipeline.market import InfrastructureError
from pipeline.strategies import Entry, Strategy


@dataclass(frozen=True)
class Params:
    provider: str = "mock"  # "mock", "jev", "clef" or "clef-flash"
    interval_s: float = 30.0  # time between two model calls
    max_answer_age_s: float = 60.0  # an older answer means: no trade
    min_probability: float = 0.80
    take_profit_pct: Decimal = Decimal("0.20")
    stop_loss_pct: Decimal = Decimal("0.40")
    max_holding_s: float = 120.0

    def __post_init__(self):
        if self.provider not in {"mock", "jev", "clef", "clef-flash"}:
            raise ValueError("provider must be mock, jev, clef or clef-flash")
        if not 5 <= self.interval_s <= 3600:
            raise ValueError("interval_s must be 5..3600")


class AITemplate(Strategy):
    name = "ai_template"
    Params = Params

    def __init__(self, params=None):
        super().__init__(params)
        self.answer = None  # (Decision, exchange_ms of the book that the model saw)
        self.last_error = None

    async def background(self, feed, client):
        model = Model(client, self.params.provider)
        if missing := model.missing():
            raise InfrastructureError("Missing environment variables: " + ", ".join(missing))
        while True:
            book = feed.book
            if book is not None and book.age_ms() <= 1500:
                try:
                    decision = await model.decide(feed.state(book))
                    if self.params.provider != "mock":
                        self.model_calls += 1
                    self.answer, self.last_error = (decision, book.exchange_ms), None
                except InfrastructureError as exc:
                    self.answer, self.last_error = None, str(exc)  # no answer, no trade
            await asyncio.sleep(self.params.interval_s)

    def on_book(self, book, account):
        if self.answer is None or account.position or account.entry_pending:
            return None
        decision, seen_ms = self.answer
        if book.exchange_ms - seen_ms > self.params.max_answer_age_s * 1000:
            return None
        if decision.action == "HOLD" or decision.probabilities[decision.action] < self.params.min_probability:
            return None
        self.answer = None  # use each answer for at most one entry
        return Entry(
            side="long" if decision.action == "BUY" else "short",
            take_profit_pct=self.params.take_profit_pct,
            stop_loss_pct=self.params.stop_loss_pct,
            max_holding_s=self.params.max_holding_s,
            note={"model": decision.model, "probabilities": decision.probabilities},
        )

    def status(self):
        action = self.answer[0].action if self.answer else None
        return {"latest_model_action": action, "last_error": self.last_error}
