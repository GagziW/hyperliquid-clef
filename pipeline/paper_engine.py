"""Deterministic paper-trading simulator. No exchange or model access.

The engine owns everything that must be the SAME for every strategy: fees, latency, the fill
model, exits, P&L and the account. A strategy (see ``pipeline/strategies``) only decides when to
enter and in which direction, and returns an ``Entry`` or an ``Exit`` request.
"""

import math
from dataclasses import asdict, dataclass, field, fields
from decimal import ROUND_FLOOR, Decimal
from uuid import uuid4

from pipeline.market import Asset, Book
from pipeline.orders import round_price


def serializable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: serializable(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [serializable(v) for v in value]
    return value


def decimal_value(name, value):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    try:
        value = Decimal(str(value))
    except ArithmeticError:
        raise ValueError(f"Invalid {name}") from None
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def seconds_value(name, value, allow_zero=False):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
        or (value == 0 and not allow_zero)
    ):
        raise ValueError(f"{name} must be finite and {'nonnegative' if allow_zero else 'positive'}")
    return value


@dataclass(frozen=True)
class SimulationConfig:
    """FIXED lab settings. Every student uses these defaults, so all results are comparable."""

    network: str = "mainnet"
    starting_balance_usdc: Decimal = Decimal("1000")
    order_usdc: Decimal = Decimal("20")
    maker_fee_bps: Decimal = Decimal("1.5")
    taker_fee_bps: Decimal = Decimal("4.5")
    slippage_bps: Decimal = Decimal("1")
    latency_ms: float = 250.0
    entry_timeout_s: float = 5.0
    max_book_age_ms: float = 1500.0
    gap_reset_s: float = 5.0

    def __post_init__(self):
        for name in (
            "starting_balance_usdc",
            "order_usdc",
            "maker_fee_bps",
            "taker_fee_bps",
            "slippage_bps",
        ):
            object.__setattr__(self, name, decimal_value(name, getattr(self, name)))
        if self.network not in {"mainnet", "testnet"}:
            raise ValueError("Unsupported network")
        if not Decimal("10") <= self.order_usdc <= Decimal("25"):
            raise ValueError("Paper order size must be 10..25 USDC")
        if self.starting_balance_usdc < self.order_usdc:
            raise ValueError("Starting paper balance must cover the requested order")
        for name in ("maker_fee_bps", "taker_fee_bps", "slippage_bps"):
            if not 0 <= getattr(self, name) < 100:
                raise ValueError(f"{name} must be in [0, 100)")
        for name in ("entry_timeout_s", "max_book_age_ms", "gap_reset_s"):
            seconds_value(name, getattr(self, name))
        seconds_value("latency_ms", self.latency_ms, allow_zero=True)
        if self.entry_timeout_s * 1000 <= self.latency_ms:
            raise ValueError("Entry timeout must exceed simulated latency")

    def record(self):
        return serializable(asdict(self))

    def is_standard(self):
        """True if every value equals the official lab value (the dataclass default)."""
        return all(getattr(self, f.name) == f.default for f in fields(self))


@dataclass(frozen=True)
class Entry:
    """A strategy's request to open one position. The engine sets price and size."""

    side: str  # "long" or "short"
    max_holding_s: float
    take_profit_pct: Decimal | None = None  # percentage points: 0.15 means 0.15%
    stop_loss_pct: Decimal | None = None
    style: str = "maker"  # "maker": post-only limit at the touch; "taker": market order
    note: dict = field(default_factory=dict)  # logged with the trade, for your analysis

    def __post_init__(self):
        if self.side not in {"long", "short"}:
            raise ValueError("Entry side must be 'long' or 'short'")
        if self.style not in {"maker", "taker"}:
            raise ValueError("Entry style must be 'maker' or 'taker'")
        seconds_value("max_holding_s", self.max_holding_s)
        if self.max_holding_s > 86400:
            raise ValueError("max_holding_s must be at most 86400 (24 hours)")
        for name in ("take_profit_pct", "stop_loss_pct"):
            value = getattr(self, name)
            if value is not None:
                value = decimal_value(name, value)
                if not 0 < value < 100:
                    raise ValueError(f"{name} must be in (0, 100)")
                object.__setattr__(self, name, value)
        if not isinstance(self.note, dict):
            raise ValueError("Entry note must be a dict")


@dataclass(frozen=True)
class Exit:
    """A strategy's request to close the open position at the next fresh book."""

    reason: str = "strategy_exit"


@dataclass(frozen=True)
class Account:
    """Read-only view of the paper account, given to the strategy with each event."""

    now_ms: int
    balance_usdc: Decimal
    equity_usdc: Decimal
    position: dict | None
    entry_pending: bool
    exit_pending: bool
    last_exit_ms: int | None


class PaperEngine:
    def __init__(self, config: SimulationConfig, strategy, asset: Asset):
        self.config, self.strategy, self.asset = config, strategy, asset
        self.balance = config.starting_balance_usdc
        self.position = None
        self.entry = None
        self.exit = None
        self.trades = []
        self.events = []
        self.last_book = None
        self.first_quote_ms = None
        self.last_quote_ms = None
        self.last_exchange_ms = None
        self.last_generation = None
        self.last_exit_ms = None
        self.equity = self.balance
        self.mark_available = True
        self.peak_equity = self.balance
        self.max_drawdown = Decimal("0")
        self.entries_requested = 0
        self.entries_expired = 0
        self.entries_rejected = 0
        self.books_observed = 0
        self.history_resets = 0
        self.funding_boundaries_crossed = 0

    # ── Helpers ──────────────────────────────────────────────
    def record(self, event, now_ms, **fields):
        self.events.append(serializable({"event": event, "time_ms": int(now_ms), **fields}))

    def drain_events(self):
        events, self.events = self.events, []
        return events

    def account(self, now_ms):
        position = None
        if self.position:
            position = {k: self.position[k] for k in ("side", "size", "entry_price", "entry_ms")}
        return Account(
            now_ms=int(now_ms),
            balance_usdc=self.balance,
            equity_usdc=self.equity,
            position=position,
            entry_pending=self.entry is not None,
            exit_pending=self.exit is not None,
            last_exit_ms=self.last_exit_ms,
        )

    @staticmethod
    def depth_price(levels, size):
        """Average price to trade ``size`` against the displayed levels, or None if too shallow."""
        remaining, cost = size, Decimal("0")
        for price, available in levels:
            fill = min(remaining, available)
            cost += fill * price
            remaining -= fill
            if remaining == 0:
                return cost / size
        return None

    def slipped(self, price, is_buy):
        slip = self.config.slippage_bps / 10000
        return price * (1 + slip) if is_buy else price * (1 - slip)

    def exit_quote(self, book, position):
        # Closing a short buys against the asks; closing a long sells against the bids.
        levels = book.asks if position["side"] == "short" else book.bids
        return self.depth_price(levels, position["size"])

    @staticmethod
    def gross_pnl(position, exit_price):
        move = position["entry_price"] - exit_price
        return position["size"] * (move if position["side"] == "short" else -move)

    # ── Data gaps and timers ────────────────────────────────
    def gap(self, now_ms, reason):
        self.last_exchange_ms = None
        self.last_generation = None
        self.history_resets += 1
        self.strategy.reset(reason)
        if self.entry:
            self.record("entry_cancelled", now_ms, reason=reason, order=self.entry)
            self.entry = None
        if self.position and not self.exit:
            self.request_exit(now_ms, "data_gap")
        self.record("signal_history_reset", now_ms, reason=reason)

    def tick(self, now_ms):
        if self.entry and now_ms >= self.entry["expires_ms"]:
            self.record("entry_expired", now_ms, order=self.entry)
            self.entry = None
            self.entries_expired += 1
        if (
            self.position
            and not self.exit
            and now_ms >= self.position["entry_ms"] + self.position["max_holding_s"] * 1000
        ):
            self.request_exit(now_ms, "max_holding_time")

    # ── Entries ─────────────────────────────────────────────
    def request_entry(self, request: Entry, book, now_ms):
        is_buy = request.side == "long"
        if request.style == "maker":
            # Post-only: a buy rests at the best bid, a sell rests at the best ask.
            price = round_price(book.bid if is_buy else book.ask, self.asset.size_decimals, is_buy)
            fee_bps = self.config.maker_fee_bps
        else:
            price = book.ask if is_buy else book.bid
            fee_bps = self.config.taker_fee_bps
        budget = min(self.config.order_usdc, self.balance / (1 + fee_bps / 10000))
        size = (budget / price).quantize(Decimal(10) ** -self.asset.size_decimals, rounding=ROUND_FLOOR)
        if size <= 0 or price * size < 10:
            self.record("entry_blocked", now_ms, reason="insufficient_paper_equity_or_rounded_notional")
            return
        self.entry = {
            "trade_id": uuid4().hex,
            "side": request.side,
            "style": request.style,
            "price": price,
            "size": size,
            "requested_ms": now_ms,
            "active_ms": now_ms + self.config.latency_ms,
            "resting_ms": None,
            "expires_ms": now_ms + self.config.entry_timeout_s * 1000,
            "take_profit_pct": request.take_profit_pct,
            "stop_loss_pct": request.stop_loss_pct,
            "max_holding_s": request.max_holding_s,
            "note": serializable(request.note),
        }
        self.entries_requested += 1
        self.record("entry_requested", now_ms, order=self.entry)
        self.process_entry(book)

    def process_entry(self, book):
        order = self.entry
        if not order or order["resting_ms"] is not None:
            return
        # Wait for a post-latency book rather than inventing the book at exchange arrival.
        if min(book.exchange_ms, book.received_wall_ms) < order["active_ms"]:
            return
        is_buy = order["side"] == "long"
        if order["style"] == "taker":
            price = self.depth_price(book.asks if is_buy else book.bids, order["size"])
            if price is None:
                self.reject_entry(book, "insufficient_depth")
                return
            price = self.slipped(price, is_buy)
            fee = price * order["size"] * self.config.taker_fee_bps / 10000
            self.open_position(order, price, fee, book.exchange_ms, book.received_wall_ms, None)
            return
        if (is_buy and book.ask <= order["price"]) or (not is_buy and book.bid >= order["price"]):
            self.reject_entry(book, "post_only_would_cross")
            return
        order["resting_ms"] = book.exchange_ms
        self.record("entry_resting", book.received_wall_ms, order=order)

    def reject_entry(self, book, reason):
        self.record("entry_rejected", book.received_wall_ms, reason=reason, order=self.entry)
        self.entry = None
        self.entries_rejected += 1

    def open_position(self, order, price, fee, entry_ms, now_ms, qualifying_trade):
        is_short = order["side"] == "short"
        tp, sl = order["take_profit_pct"], order["stop_loss_pct"]
        target = None if tp is None else price * (1 - tp / 100 if is_short else 1 + tp / 100)
        stop = None if sl is None else price * (1 + sl / 100 if is_short else 1 - sl / 100)
        self.position = {
            "trade_id": order["trade_id"],
            "side": order["side"],
            "style": order["style"],
            "size": order["size"],
            "entry_price": price,
            "entry_ms": entry_ms,
            "entry_fee": fee,
            "target_price": target,
            "stop_price": stop,
            "max_holding_s": order["max_holding_s"],
            "note": order["note"],
        }
        self.balance -= fee
        self.entry = None
        self.record("position_opened", now_ms, position=self.position, qualifying_trade=qualifying_trade)
        if self.last_book:
            self.mark(self.last_book)

    # ── Exits ───────────────────────────────────────────────
    def request_exit(self, now_ms, reason):
        self.exit = {
            "reason": reason,
            "requested_ms": int(now_ms),
            "active_ms": now_ms + self.config.latency_ms,
        }
        self.record("exit_requested", now_ms, trade_id=self.position["trade_id"], **self.exit)

    def check_exits(self, book, now_ms):
        px = self.exit_quote(book, self.position)
        if px is None:
            return
        if self.exit:
            if now_ms >= self.exit["active_ms"]:
                self.close(now_ms, px)
            return
        position = self.position
        is_short = position["side"] == "short"
        stop, target = position["stop_price"], position["target_price"]
        equity = self.balance + self.gross_pnl(position, px)
        if stop is not None and (px >= stop if is_short else px <= stop):
            self.request_exit(now_ms, "stop_loss")
        elif px * position["size"] > equity:
            self.request_exit(now_ms, "exposure_limit")
        elif target is not None and (px <= target if is_short else px >= target):
            self.request_exit(now_ms, "take_profit")

    def mark(self, book):
        equity = self.balance
        if self.position:
            px = self.exit_quote(book, self.position)
            if px is None:
                self.mark_available = False
                return
            px = self.slipped(px, self.position["side"] == "short")
            gross = self.gross_pnl(self.position, px)
            equity += gross - px * self.position["size"] * self.config.taker_fee_bps / 10000
        self.equity, self.mark_available = equity, True
        self.peak_equity = max(self.peak_equity, self.equity)
        self.max_drawdown = max(self.max_drawdown, self.peak_equity - self.equity)

    def close(self, now_ms, price):
        position = self.position
        price = self.slipped(price, position["side"] == "short")
        gross = self.gross_pnl(position, price)
        exit_fee = position["size"] * price * self.config.taker_fee_bps / 10000
        pnl = gross - position["entry_fee"] - exit_fee
        self.balance += gross - exit_fee
        boundaries = int(now_ms // 3600000) - int(position["entry_ms"] // 3600000)
        self.funding_boundaries_crossed += max(0, boundaries)
        trade = serializable(
            {
                **position,
                "exit_ms": int(now_ms),
                "exit_price": price,
                "exit_fee": exit_fee,
                "gross_pnl_usdc": gross,
                "pnl_after_fees_usdc": pnl,
                "net_bps": pnl / (position["entry_price"] * position["size"]) * 10000,
                "holding_s": (now_ms - position["entry_ms"]) / 1000,
                "exit_reason": self.exit["reason"],
                "paper_balance_usdc": self.balance,
                "funding_boundaries_crossed": max(0, boundaries),
                "funding_included": False,
            }
        )
        self.trades.append(trade)
        self.position = self.exit = None
        self.last_exit_ms = int(now_ms)
        self.record("position_closed", now_ms, trade=trade)

    # ── Strategy requests ───────────────────────────────────
    def apply(self, request, book, now_ms):
        if request is None:
            return
        if isinstance(request, Exit):
            if self.position and not self.exit:
                self.request_exit(now_ms, request.reason)
        elif isinstance(request, Entry):
            # One position at a time: no averaging down, no second pending entry.
            if not self.position and not self.entry and book is not None:
                self.request_entry(request, book, now_ms)
        else:
            raise TypeError(f"Strategy returned {type(request).__name__}; expected Entry, Exit or None")

    # ── Market events ───────────────────────────────────────
    def on_book(self, book: Book):
        now_ms = int(book.received_wall_ms)
        if self.last_generation is not None and self.last_generation != book.generation:
            self.gap(now_ms, "feed_reconnected")
        if self.last_exchange_ms is not None and book.exchange_ms <= self.last_exchange_ms:
            return
        if (
            self.last_exchange_ms is not None
            and book.exchange_ms - self.last_exchange_ms > self.config.gap_reset_s * 1000
        ):
            self.gap(now_ms, "missing_book_updates")
        self.last_generation, self.last_exchange_ms = book.generation, book.exchange_ms
        self.last_book, self.last_quote_ms = book, now_ms
        if self.first_quote_ms is None:
            self.first_quote_ms = now_ms
        self.books_observed += 1
        self.tick(now_ms)
        self.process_entry(book)
        if self.position:
            self.check_exits(book, now_ms)
        self.mark(book)
        self.apply(self.strategy.on_book(book, self.account(now_ms)), book, now_ms)

    def on_trade(self, trade, received_ms):
        self.tick(received_ms)
        self.fill_resting_entry(trade, received_ms)
        self.apply(self.strategy.on_trade(trade, self.account(received_ms)), self.last_book, received_ms)

    def fill_resting_entry(self, trade, received_ms):
        order = self.entry
        if (
            not order
            or order["style"] != "maker"
            or order["resting_ms"] is None
            or trade.get("coin") != "ETH"
        ):
            return
        is_buy = order["side"] == "long"
        # A resting buy fills only on seller-initiated trades ("A"); a resting sell only on buyers ("B").
        if trade.get("side") != ("A" if is_buy else "B"):
            return
        try:
            price, volume, exchange_ms = Decimal(trade["px"]), Decimal(trade["sz"]), int(trade["time"])
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return
        if not price.is_finite() or not volume.is_finite() or price <= 0 or volume <= 0:
            return
        if received_ms - exchange_ms > self.config.max_book_age_ms or exchange_ms > received_ms + 250:
            return
        # A touch is NOT a fill. Require a later trade strictly through the limit, with enough size.
        through = price < order["price"] if is_buy else price > order["price"]
        if (
            not order["resting_ms"] <= exchange_ms < order["expires_ms"]
            or not through
            or volume < order["size"]
        ):
            return
        fee = order["price"] * order["size"] * self.config.maker_fee_bps / 10000
        self.open_position(order, order["price"], fee, exchange_ms, received_ms, trade)

    # ── Checkpoint and summary ──────────────────────────────
    def identity(self):
        return {
            "simulation": self.config.record(),
            "strategy": self.strategy.record(),
            "asset": asdict(self.asset),
        }

    def checkpoint(self):
        return serializable(
            {
                "version": 2,
                **self.identity(),
                "balance": self.balance,
                "position": self.position,
                "entry": self.entry,
                "exit": self.exit,
                "trades": self.trades,
                "last_exit_ms": self.last_exit_ms,
                "equity": self.equity,
                "mark_available": self.mark_available,
                "peak_equity": self.peak_equity,
                "max_drawdown": self.max_drawdown,
                "first_quote_ms": self.first_quote_ms,
                "last_quote_ms": self.last_quote_ms,
                "entries_requested": self.entries_requested,
                "entries_expired": self.entries_expired,
                "entries_rejected": self.entries_rejected,
                "books_observed": self.books_observed,
                "history_resets": self.history_resets,
                "funding_boundaries_crossed": self.funding_boundaries_crossed,
            }
        )

    def restore(self, state, now_ms):
        identity = serializable(self.identity())
        if state.get("version") != 2 or any(state.get(k) != v for k, v in identity.items()):
            raise ValueError("Paper checkpoint configuration/metadata mismatch")
        for name in ("balance", "equity", "peak_equity", "max_drawdown"):
            setattr(self, name, Decimal(state[name]))
        for name in (
            "last_exit_ms",
            "first_quote_ms",
            "last_quote_ms",
            "entries_requested",
            "entries_expired",
            "entries_rejected",
            "books_observed",
            "history_resets",
            "funding_boundaries_crossed",
        ):
            setattr(self, name, state[name])
        self.trades = state["trades"]
        self.mark_available = state.get("mark_available", False)
        self.entry, self.exit, self.position = state["entry"], state["exit"], state["position"]
        optional = ("take_profit_pct", "stop_loss_pct", "target_price", "stop_price")
        for record, names in (
            (self.entry, ("price", "size", "take_profit_pct", "stop_loss_pct")),
            (self.position, ("size", "entry_price", "entry_fee", "target_price", "stop_price")),
        ):
            if record:
                for name in names:
                    if record[name] is not None or name not in optional:
                        record[name] = Decimal(record[name])
        self.gap(now_ms, "process_restart")
        self.record("paper_account_restored", now_ms, closed_trades=len(self.trades))

    def summary(self, now_ms):
        pnl = sum((Decimal(t["pnl_after_fees_usdc"]) for t in self.trades), Decimal("0"))
        wins = sum(Decimal(t["pnl_after_fees_usdc"]) > 0 for t in self.trades)
        nets = [Decimal(t["net_bps"]) for t in self.trades]
        observed_s = None
        if self.first_quote_ms is not None and self.last_quote_ms is not None:
            observed_s = (self.last_quote_ms - self.first_quote_ms) / 1000
        return serializable(
            {
                "mode": "paper",
                "strategy": self.strategy.name,
                "strategy_params": self.strategy.record()["params"],
                "strategy_status": self.strategy.status(),
                "simulation": self.config.record(),
                "simulation_standard": self.config.is_standard(),
                "observed_s": observed_s,
                "closed_trades": len(self.trades),
                "winning_trades": wins,
                "win_rate": wins / len(self.trades) if self.trades else None,
                "closed_pnl_after_fees_usdc": pnl,
                "mean_net_bps_per_trade": sum(nets) / len(nets) if nets else None,
                "paper_balance_usdc": self.balance,
                "estimated_equity_if_closed_usdc": self.equity if self.mark_available else None,
                "max_observed_drawdown_usdc": self.max_drawdown,
                "last_quote_age_s": (now_ms - self.last_quote_ms) / 1000
                if self.last_quote_ms is not None
                else None,
                "open_position": self.position,
                "pending_entry": self.entry,
                "pending_exit": self.exit,
                "entries_requested": self.entries_requested,
                "entries_expired": self.entries_expired,
                "entries_rejected": self.entries_rejected,
                "books_observed": self.books_observed,
                "history_resets": self.history_resets,
                "funding_boundaries_crossed": self.funding_boundaries_crossed,
                "funding_included": False,
                "orders_submitted": 0,
                "model_calls": self.strategy.model_calls,
                "fill_model": "maker: post-latency passive check, full-size trade-through; "
                "taker: post-latency depth plus slippage; exits: delayed depth plus slippage",
            }
        )
