"""Deterministic, short-only spike-fade simulation. No exchange or model access."""

import math
from collections import deque
from dataclasses import asdict, dataclass
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


@dataclass(frozen=True)
class PaperConfig:
    network: str = "mainnet"
    jump_threshold_pct: Decimal = Decimal("0.50")
    lookback_s: float = 60.0
    take_profit_pct: Decimal = Decimal("0.15")
    stop_loss_pct: Decimal = Decimal("0.50")
    max_holding_s: float = 120.0
    cooldown_s: float = 30.0
    starting_balance_usdc: Decimal = Decimal("1000")
    order_usdc: Decimal = Decimal("20")
    maker_fee_bps: Decimal = Decimal("1.5")
    taker_fee_bps: Decimal = Decimal("4.5")
    exit_slippage_bps: Decimal = Decimal("1")
    latency_ms: float = 250.0
    entry_timeout_s: float = 5.0
    max_book_age_ms: float = 1500.0
    gap_reset_s: float = 5.0

    def __post_init__(self):
        decimal_fields = (
            "jump_threshold_pct",
            "take_profit_pct",
            "stop_loss_pct",
            "starting_balance_usdc",
            "order_usdc",
            "maker_fee_bps",
            "taker_fee_bps",
            "exit_slippage_bps",
        )
        for name in decimal_fields:
            value = getattr(self, name)
            if isinstance(value, bool):
                raise ValueError(f"{name} must be numeric")
            try:
                value = Decimal(str(value))
            except ArithmeticError:
                raise ValueError(f"Invalid {name}") from None
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
            object.__setattr__(self, name, value)
        if self.network not in {"mainnet", "testnet"}:
            raise ValueError("Unsupported network")
        for name in ("jump_threshold_pct", "take_profit_pct", "stop_loss_pct"):
            if not 0 < getattr(self, name) < 100:
                raise ValueError(f"{name} must be in (0, 100)")
        if not Decimal("10") <= self.order_usdc <= Decimal("25"):
            raise ValueError("Paper order size must be 10..25 USDC")
        if self.starting_balance_usdc < self.order_usdc:
            raise ValueError("Starting paper balance must cover the requested order")
        for name in ("maker_fee_bps", "taker_fee_bps", "exit_slippage_bps"):
            if not 0 <= getattr(self, name) < 100:
                raise ValueError(f"{name} must be in [0, 100)")
        for name in ("lookback_s", "max_holding_s", "entry_timeout_s", "max_book_age_ms", "gap_reset_s"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be finite and positive")
        for name in ("cooldown_s", "latency_ms"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.entry_timeout_s * 1000 <= self.latency_ms:
            raise ValueError("Entry timeout must exceed simulated latency")

    def record(self):
        return serializable(asdict(self))


class SpikeFade:
    def __init__(self, config: PaperConfig, asset: Asset):
        self.config, self.asset = config, asset
        self.balance = config.starting_balance_usdc
        self.position = None
        self.entry = None
        self.exit = None
        self.trades = []
        self.events = []
        self.history = deque()
        self.last_book = None
        self.last_quote_ms = None
        self.last_exchange_ms = None
        self.last_generation = None
        self.jump_pct = None
        self.above_threshold = False
        self.cooldown_until_ms = 0
        self.equity = self.balance
        self.mark_available = True
        self.peak_equity = self.balance
        self.max_drawdown = Decimal("0")
        self.entries_requested = 0
        self.entries_expired = 0
        self.entries_rejected = 0
        self.books_observed = 0
        self.funding_boundaries_crossed = 0

    def record(self, event, now_ms, **fields):
        self.events.append(serializable({"event": event, "time_ms": int(now_ms), **fields}))

    def drain_events(self):
        events, self.events = self.events, []
        return events

    def gap(self, now_ms, reason):
        self.history.clear()
        self.last_exchange_ms = None
        self.last_generation = None
        self.jump_pct = None
        self.above_threshold = False
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
            and now_ms >= self.position["entry_ms"] + self.config.max_holding_s * 1000
        ):
            self.request_exit(now_ms, "max_holding_time")

    def cover_quote(self, book, size):
        remaining, cost = size, Decimal("0")
        for price, available in book.asks:
            fill = min(remaining, available)
            cost += fill * price
            remaining -= fill
            if remaining == 0:
                return cost / size
        return None

    def confirm_entry(self, book):
        if not self.entry or self.entry["resting_ms"] is not None:
            return
        if min(book.exchange_ms, book.received_wall_ms) < self.entry["active_ms"]:
            return
        if book.bid >= self.entry["price"]:
            self.record(
                "entry_rejected", book.received_wall_ms, reason="post_only_would_cross", order=self.entry
            )
            self.entry = None
            self.entries_rejected += 1
            return
        # Wait for a post-latency book rather than inventing the book at exchange arrival.
        self.entry["resting_ms"] = book.exchange_ms
        self.record("entry_resting", book.received_wall_ms, order=self.entry)

    def mark(self, book):
        equity = self.balance
        if self.position:
            px = self.cover_quote(book, self.position["size"])
            if px is None:
                self.mark_available = False
                return
            px *= 1 + self.config.exit_slippage_bps / 10000
            gross = self.position["size"] * (self.position["entry_price"] - px)
            equity += gross - px * self.position["size"] * self.config.taker_fee_bps / 10000
        self.equity, self.mark_available = equity, True
        self.peak_equity = max(self.peak_equity, self.equity)
        self.max_drawdown = max(self.max_drawdown, self.peak_equity - self.equity)

    def request_exit(self, now_ms, reason):
        self.exit = {
            "reason": reason,
            "requested_ms": int(now_ms),
            "active_ms": now_ms + self.config.latency_ms,
        }
        self.record("exit_requested", now_ms, trade_id=self.position["trade_id"], **self.exit)

    def close(self, now_ms, price):
        position = self.position
        price *= 1 + self.config.exit_slippage_bps / 10000
        gross = position["size"] * (position["entry_price"] - price)
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
                "holding_s": (now_ms - position["entry_ms"]) / 1000,
                "exit_reason": self.exit["reason"],
                "paper_balance_usdc": self.balance,
                "funding_boundaries_crossed": max(0, boundaries),
                "funding_included": False,
            }
        )
        self.trades.append(trade)
        self.position = self.exit = None
        self.cooldown_until_ms = now_ms + self.config.cooldown_s * 1000
        self.record("short_closed", now_ms, trade=trade)

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
        self.books_observed += 1
        self.tick(now_ms)
        self.confirm_entry(book)

        if self.position:
            px = self.cover_quote(book, self.position["size"])
            if px is not None:
                if self.exit and now_ms >= self.exit["active_ms"]:
                    self.close(now_ms, px)
                elif not self.exit:
                    entry = self.position["entry_price"]
                    gross_notional = px * self.position["size"]
                    equity = self.balance + self.position["size"] * (entry - px)
                    if px >= self.position["stop_price"]:
                        self.request_exit(now_ms, "stop_loss")
                    elif gross_notional > equity:
                        self.request_exit(now_ms, "exposure_limit")
                    elif px <= self.position["target_price"]:
                        self.request_exit(now_ms, "take_profit")
        self.mark(book)

        self.history.append((book.exchange_ms, book.mid))
        cutoff = book.exchange_ms - self.config.lookback_s * 1000
        # Keep the observation immediately before the window boundary, not a future observation.
        while len(self.history) > 1 and self.history[1][0] <= cutoff:
            self.history.popleft()
        if self.history[0][0] > cutoff:
            self.jump_pct = None
            return
        self.jump_pct = (book.mid / self.history[0][1] - 1) * 100
        above = self.jump_pct >= self.config.jump_threshold_pct
        crossed = above and not self.above_threshold
        self.above_threshold = above
        if not crossed or self.position or self.entry or now_ms < self.cooldown_until_ms:
            return
        price = round_price(book.ask, self.asset.size_decimals, False)
        budget = min(self.config.order_usdc, self.balance / (1 + self.config.maker_fee_bps / 10000))
        size = (budget / price).quantize(Decimal(10) ** -self.asset.size_decimals, rounding=ROUND_FLOOR)
        if size <= 0 or price * size < 10:
            self.record("entry_blocked", now_ms, reason="insufficient_paper_equity_or_rounded_notional")
            return
        self.entry = {
            "trade_id": uuid4().hex,
            "price": price,
            "size": size,
            "requested_ms": now_ms,
            "active_ms": now_ms + self.config.latency_ms,
            "resting_ms": None,
            "expires_ms": now_ms + self.config.entry_timeout_s * 1000,
            "jump_pct": self.jump_pct,
        }
        self.entries_requested += 1
        self.record("entry_requested", now_ms, order=self.entry)
        self.confirm_entry(book)

    def on_trade(self, trade, received_ms):
        self.tick(received_ms)
        if (
            not self.entry
            or self.entry["resting_ms"] is None
            or trade.get("coin") != "ETH"
            or trade.get("side") != "B"
        ):
            return
        try:
            price, volume, exchange_ms = Decimal(trade["px"]), Decimal(trade["sz"]), int(trade["time"])
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return
        if not price.is_finite() or not volume.is_finite() or price <= 0 or volume <= 0:
            return
        if received_ms - exchange_ms > self.config.max_book_age_ms or exchange_ms > received_ms + 250:
            return
        order = self.entry
        # A touch is NOT a fill. Require a later buyer-initiated trade strictly through the limit.
        if (
            not order["resting_ms"] <= exchange_ms < order["expires_ms"]
            or price <= order["price"]
            or volume < order["size"]
        ):
            return
        entry_fee = order["price"] * order["size"] * self.config.maker_fee_bps / 10000
        self.position = {
            "trade_id": order["trade_id"],
            "size": order["size"],
            "entry_price": order["price"],
            "entry_ms": exchange_ms,
            "entry_fee": entry_fee,
            "entry_jump_pct": order["jump_pct"],
            "target_price": order["price"] * (1 - self.config.take_profit_pct / 100),
            "stop_price": order["price"] * (1 + self.config.stop_loss_pct / 100),
        }
        self.balance -= entry_fee
        self.entry = None
        self.record("short_opened", received_ms, position=self.position, qualifying_trade=trade)
        if self.last_book:
            self.mark(self.last_book)

    def checkpoint(self):
        return serializable(
            {
                "version": 1,
                "config": self.config.record(),
                "asset": asdict(self.asset),
                "balance": self.balance,
                "position": self.position,
                "entry": self.entry,
                "exit": self.exit,
                "trades": self.trades,
                "cooldown_until_ms": self.cooldown_until_ms,
                "equity": self.equity,
                "mark_available": self.mark_available,
                "peak_equity": self.peak_equity,
                "max_drawdown": self.max_drawdown,
                "last_quote_ms": self.last_quote_ms,
                "entries_requested": self.entries_requested,
                "entries_expired": self.entries_expired,
                "entries_rejected": self.entries_rejected,
                "books_observed": self.books_observed,
                "funding_boundaries_crossed": self.funding_boundaries_crossed,
            }
        )

    def restore(self, state, now_ms):
        if (
            state.get("version") != 1
            or state.get("config") != self.config.record()
            or state.get("asset") != asdict(self.asset)
        ):
            raise ValueError("Paper checkpoint configuration/metadata mismatch")
        for name in ("balance", "equity", "peak_equity", "max_drawdown"):
            setattr(self, name, Decimal(state[name]))
        for name in (
            "cooldown_until_ms",
            "last_quote_ms",
            "entries_requested",
            "entries_expired",
            "books_observed",
            "funding_boundaries_crossed",
        ):
            setattr(self, name, state[name])
        self.trades = state["trades"]
        self.entries_rejected = state.get("entries_rejected", 0)
        self.mark_available = state.get("mark_available", False)
        self.entry, self.exit, self.position = state["entry"], state["exit"], state["position"]
        if self.entry:
            for field in ("price", "size", "jump_pct"):
                self.entry[field] = Decimal(self.entry[field])
        if self.position:
            for field in ("size", "entry_price", "entry_fee", "entry_jump_pct", "target_price", "stop_price"):
                self.position[field] = Decimal(self.position[field])
        self.gap(now_ms, "process_restart")
        self.record("paper_account_restored", now_ms, closed_trades=len(self.trades))

    def summary(self, now_ms):
        pnl = sum((Decimal(t["pnl_after_fees_usdc"]) for t in self.trades), Decimal("0"))
        wins = sum(Decimal(t["pnl_after_fees_usdc"]) > 0 for t in self.trades)
        return serializable(
            {
                "mode": "paper",
                "strategy": "short_spike_fade",
                "config": self.config.record(),
                "closed_trades": len(self.trades),
                "winning_trades": wins,
                "win_rate": wins / len(self.trades) if self.trades else None,
                "closed_pnl_after_fees_usdc": pnl,
                "paper_balance_usdc": self.balance,
                "estimated_equity_if_closed_usdc": self.equity if self.mark_available else None,
                "max_observed_drawdown_usdc": self.max_drawdown,
                "last_quote_age_s": (now_ms - self.last_quote_ms) / 1000
                if self.last_quote_ms is not None
                else None,
                "open_short": self.position,
                "pending_entry": self.entry,
                "pending_exit": self.exit,
                "current_jump_pct": self.jump_pct,
                "entries_requested": self.entries_requested,
                "entries_expired": self.entries_expired,
                "entries_rejected": self.entries_rejected,
                "books_observed": self.books_observed,
                "funding_boundaries_crossed": self.funding_boundaries_crossed,
                "funding_included": False,
                "orders_submitted": 0,
                "model_calls": 0,
                "fill_model": "post-latency passive check, full-size trade-through entry; delayed ask-depth exit plus slippage",
            }
        )
