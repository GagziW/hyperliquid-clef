import asyncio
import json
import math
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal

import httpx
from websockets.asyncio.client import connect

from pipeline.config import Settings


class InfrastructureError(Exception):
    """A safe, short diagnostic that never includes request headers or credentials."""


@dataclass(frozen=True)
class Asset:
    asset_id: int
    size_decimals: int

    def __post_init__(self):
        if self.asset_id < 0 or not 0 <= self.size_decimals <= 6:
            raise InfrastructureError("Invalid ETH perpetual metadata")


@dataclass(frozen=True)
class Book:
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    exchange_ms: int
    received_mono: float
    received_wall_ms: float
    generation: int

    @property
    def bid(self):
        return self.bids[0][0]

    @property
    def ask(self):
        return self.asks[0][0]

    @property
    def mid(self):
        return (self.bid + self.ask) / 2

    @property
    def spread_bps(self):
        return float((self.ask - self.bid) / self.mid * 10000)

    def age_ms(self):
        return (time.monotonic() - self.received_mono) * 1000

    def estimated_exchange_age_ms(self):
        return self.received_wall_ms - self.exchange_ms + self.age_ms()

    @classmethod
    def parse(cls, data: dict, generation: int = 0):
        if data.get("coin") != "ETH":
            raise ValueError("Unexpected book symbol")
        sides = []
        for side in data["levels"]:
            levels = tuple((Decimal(x["px"]), Decimal(x["sz"])) for x in side[:5])
            if not levels or any(
                not p.is_finite() or not s.is_finite() or p <= 0 or s <= 0 for p, s in levels
            ):
                raise ValueError("Invalid book levels")
            sides.append(levels)
        if len(sides) != 2:
            raise ValueError("Invalid book sides")
        if any(sides[0][i][0] <= sides[0][i + 1][0] for i in range(len(sides[0]) - 1)):
            raise ValueError("Unsorted bids")
        if any(sides[1][i][0] >= sides[1][i + 1][0] for i in range(len(sides[1]) - 1)):
            raise ValueError("Unsorted asks")
        book = cls(*sides, int(data["time"]), time.monotonic(), time.time() * 1000, generation)
        if book.bid >= book.ask:
            raise ValueError("Crossed book")
        return book


class Hyperliquid:
    def __init__(self, client: httpx.AsyncClient, settings: Settings):
        self.client = client
        self.settings = settings

    async def info(self, body):
        try:
            response = await self.client.post(self.settings.http_url + "/info", json=body)
            if response.status_code != 200:
                raise InfrastructureError(f"Hyperliquid info HTTP {response.status_code}")
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise InfrastructureError(f"Hyperliquid info {type(exc).__name__}") from None

    async def eth_meta(self):
        data = await self.info({"type": "metaAndAssetCtxs"})
        try:
            if not isinstance(data, list) or len(data) != 2:
                raise ValueError
            for index, entry in enumerate(data[0]["universe"]):
                if entry["name"] == "ETH" and not entry.get("isDelisted", False):
                    decimals, context = entry["szDecimals"], data[1][index]
                    if (
                        isinstance(decimals, bool)
                        or not isinstance(decimals, int)
                        or not isinstance(context, dict)
                    ):
                        raise ValueError
                    return Asset(index, decimals), context
        except (KeyError, IndexError, TypeError, ValueError):
            raise InfrastructureError("Invalid ETH perpetual metadata response") from None
        raise InfrastructureError("Active ETH perpetual missing from exchange metadata")


class Feed:
    def __init__(self, settings: Settings, emit, on_book=None, on_trade=None):
        self.settings = settings
        self.emit = emit
        self.on_book = on_book
        self.on_trade = on_trade
        self.book: Book | None = None
        self.generation = 0
        self.history = deque(maxlen=2000)
        self.trades = deque(maxlen=10000)
        self.trade_ids = set()
        self.message_count = 0
        self.book_intervals_ms = deque(maxlen=10000)

    def subscriptions(self):
        return [
            {"type": "l2Book", "coin": "ETH", "fast": self.settings.fast_book},
            {"type": "trades", "coin": "ETH"},
        ]

    async def run(self):
        backoff = 1
        while True:
            self.book = None
            self.history.clear()
            self.trades.clear()
            self.trade_ids.clear()
            self.generation += 1
            try:
                async with connect(
                    self.settings.ws_url,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=2,
                    max_queue=32,
                ) as ws:
                    for subscription in self.subscriptions():
                        await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
                    self.emit(
                        {
                            "event": "feed_connected",
                            "generation": self.generation,
                            "book_subscription": "fast" if self.settings.fast_book else "slow",
                        }
                    )
                    backoff = 1
                    async for raw in ws:
                        self.ingest(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.book = None
                self.emit(
                    {"event": "feed_disconnected", "error_type": type(exc).__name__, "retry_s": backoff}
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def ingest(self, message):
        self.message_count += 1
        channel, data = message.get("channel"), message.get("data")
        if channel in {"l2Book", "trades"}:
            self.emit(
                {
                    "event": "market_data",
                    "generation": self.generation,
                    "received_wall_ms": time.time() * 1000,
                    "payload": message,
                }
            )
        if channel == "l2Book":
            book = Book.parse(data, self.generation)
            if self.book and book.exchange_ms < self.book.exchange_ms:
                return
            if self.book:
                self.book_intervals_ms.append((book.received_mono - self.book.received_mono) * 1000)
            self.book = book
            self.history.append((book.received_mono, float(book.mid)))
            if self.on_book:
                self.on_book(book)
        elif channel == "trades":
            for trade in data:
                tid = trade.get("tid")
                if trade.get("coin") != "ETH" or tid is None:
                    continue
                # tid alone can repeat when the same pair of orders matches in another block.
                trade_key = (int(trade["time"]), tid)
                if trade_key in self.trade_ids:
                    continue
                if trade.get("side") not in {"B", "A"}:
                    continue
                size = float(trade["sz"])
                if not math.isfinite(size) or size <= 0:
                    continue
                if len(self.trades) == self.trades.maxlen:
                    self.trade_ids.discard(self.trades[0][0])
                self.trade_ids.add(trade_key)
                self.trades.append((trade_key, int(trade["time"]), trade["side"], size))
                if self.on_trade:
                    self.on_trade(trade)

    async def wait_book(self, timeout_s=15):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            book = self.book
            if book and book.age_ms() <= self.settings.max_book_age_ms:
                return book
            await asyncio.sleep(0.02)
        raise InfrastructureError("No fresh ETH websocket book within timeout")

    def state(self, book: Book):
        bid_depth = sum(s for _, s in book.bids)
        ask_depth = sum(s for _, s in book.asks)
        recent = [t for t in self.trades if book.exchange_ms - 60000 <= t[1] <= book.exchange_ms]
        buy_size = sum(t[3] for t in recent if t[2] == "B")
        sell_size = sum(t[3] for t in recent if t[2] == "A")
        state = {
            "market": "ETH perpetual / USDC collateral",
            "mode": "infrastructure experiment",
            "exchange_time_ms": book.exchange_ms,
            "mid": float(book.mid),
            "bid": float(book.bid),
            "ask": float(book.ask),
            "spread_bps": book.spread_bps,
            "depth_5_imbalance": float((bid_depth - ask_depth) / (bid_depth + ask_depth)),
            "observed_trade_count_60s": len(recent),
            "observed_taker_buy_fraction_60s": buy_size / (buy_size + sell_size) if recent else None,
            "observed_history_s": book.received_mono - self.history[0][0] if self.history else 0,
            "horizon_s": 60,
            "constraints": {
                "target_max_leverage": 1,
                "post_only": True,
                "order_usdc": str(self.settings.order_usdc),
                "strategy_validated": False,
            },
        }
        for seconds in (5, 30):
            earlier = [p for t, p in self.history if t <= book.received_mono - seconds]
            state[f"mid_return_{seconds}s_bps"] = (
                (float(book.mid) / earlier[-1] - 1) * 10000 if earlier else None
            )
        return state
