"""Strategies for the paper trader.

Each file in this folder is one strategy. The file name is the strategy name that you put in
``paper.toml``. A strategy file contains one subclass of ``Strategy`` and, optionally, a
``Params`` dataclass with its parameters.

- ``spike_fade.py``  the EXAMPLE strategy of the lab. It is also the benchmark: keep it running.
- ``my_strategy.py`` the TEMPLATE for your own strategy.
- ``ai_template.py`` a TEMPLATE that shows how to connect an AI model.

The engine (``pipeline/paper_engine.py``) does the rest: fees, latency, fills, exits and P&L.
"""

import importlib
import inspect
import re
import typing
from dataclasses import MISSING, asdict, fields, is_dataclass
from decimal import Decimal

from pipeline.paper_engine import Account, Entry, Exit, serializable

__all__ = ["Account", "Entry", "Exit", "Strategy", "load_strategy"]


class Strategy:
    """Base class for a paper strategy. Override the methods that you need.

    The engine calls ``on_book`` for each fresh order book (about two times each second) and
    ``on_trade`` for each public trade. Return ``Entry(...)`` to request a position, ``Exit()`` to
    close the open position, or ``None`` to do nothing. The engine ignores an ``Entry`` while a
    position or an entry order is open.
    """

    name = None  # defaults to the file name
    Params = None  # a dataclass with your parameters, read from [strategies.<name>] in paper.toml

    def __init__(self, params=None):
        if params is None and self.Params is not None:
            params = self.Params()
        self.params = params
        self.name = self.name or type(self).__module__.rsplit(".", 1)[-1]
        self.model_calls = 0  # increment this if your strategy calls a paid model

    def on_book(self, book, account: Account):
        return None

    def on_trade(self, trade, account: Account):
        return None

    def reset(self, reason):
        """Called after a data gap. Clear the history that you keep, because it has a hole now."""

    def status(self):
        """Values to show in status.json while the strategy runs (for example your signal)."""
        return {}

    async def background(self, feed, client):
        """Optional slow task, for example an AI model call each 30 s. See ai_template.py.

        ``feed`` is the live ``pipeline.market.Feed`` and ``client`` an ``httpx.AsyncClient``.
        Never call a model inside ``on_book``: it runs many times each second.
        """

    def record(self):
        params = asdict(self.params) if is_dataclass(self.params) else {}
        return serializable({"name": self.name, "params": params})


def has_background(strategy):
    return type(strategy).background is not Strategy.background


def build_params(cls, values):
    params = cls.Params
    if params is None:
        if values:
            raise ValueError(f"Strategy {cls.__name__} has no Params, but paper.toml gives some")
        return None
    hints = typing.get_type_hints(params)
    allowed = {f.name for f in fields(params)}
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(
            f"Unknown parameters for {cls.__name__}: {', '.join(sorted(unknown))}. "
            f"Allowed: {', '.join(sorted(allowed))}"
        )
    converted = {}
    for f in fields(params):
        if f.name not in values:
            if f.default is MISSING and f.default_factory is MISSING:
                raise ValueError(f"Missing parameter {f.name} for {cls.__name__}")
            continue
        value = values[f.name]
        if hints.get(f.name) is Decimal and not isinstance(value, bool):
            value = Decimal(str(value))
        converted[f.name] = value
    try:
        return params(**converted)
    except (TypeError, ArithmeticError) as exc:
        raise ValueError(f"Invalid parameters for {cls.__name__}: {exc}") from None


def load_strategy(name, values=None):
    """Import ``pipeline/strategies/<name>.py`` and create its strategy with the given parameters."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name or ""):
        raise ValueError(f"Invalid strategy name {name!r}: use lowercase letters, digits and _")
    try:
        module = importlib.import_module(f"pipeline.strategies.{name}")
    except ModuleNotFoundError as exc:
        if exc.name == f"pipeline.strategies.{name}":
            raise ValueError(f"No strategy file pipeline/strategies/{name}.py") from None
        raise
    classes = [
        cls
        for _, cls in inspect.getmembers(module, inspect.isclass)
        if issubclass(cls, Strategy) and cls is not Strategy and cls.__module__ == module.__name__
    ]
    if len(classes) != 1:
        raise ValueError(f"pipeline/strategies/{name}.py must define exactly one Strategy subclass")
    cls = classes[0]
    strategy = cls(build_params(cls, values or {}))
    strategy.name = name
    return strategy
