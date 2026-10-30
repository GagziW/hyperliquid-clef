import argparse
import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal

import httpx
import pytest

from pipeline import paper
from pipeline.market import Asset, Book
from pipeline.paper_strategy import PaperConfig, SpikeFade

BASE = 1791612600000


def book(seconds, mid="100", generation=1, ask_size="1"):
    mid = Decimal(str(mid))
    ts = BASE + int(seconds * 1000)
    return Book(
        ((mid - Decimal("0.01"), Decimal("1")),),
        ((mid + Decimal("0.01"), Decimal(ask_size)),),
        ts,
        ts / 1000,
        ts,
        generation,
    )


def ready(**overrides):
    config = PaperConfig(lookback_s=2, latency_ms=250, **overrides)
    engine = SpikeFade(config, Asset(1, 4))
    engine.on_book(book(0))
    engine.on_book(book(1))
    engine.on_book(book(2, "100.60"))
    assert engine.entry is not None
    return engine


def fill(engine, seconds=2.3):
    engine.on_book(book(seconds - 0.025, "100.60"))
    engine.on_trade(
        {"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + int(seconds * 1000)},
        BASE + int(seconds * 1000) + 50,
    )
    assert engine.position is not None
    return engine.position


def test_requires_complete_lookback_and_threshold_crossing():
    engine = SpikeFade(PaperConfig(lookback_s=2), Asset(1, 4))
    engine.on_book(book(0))
    engine.on_book(book(1, "101"))
    assert engine.entry is None
    engine.on_book(book(2, "101"))
    assert engine.entry is not None
    engine.tick(BASE + 8000)
    assert engine.entry is None
    engine.on_book(book(3, "101"))
    assert engine.entries_requested == 1


@pytest.mark.parametrize(
    "change",
    [
        {"side": "A"},
        {"px": "100.61"},
        {"time": BASE + 2100},
        {"sz": "0.0001"},
        {"coin": "BTC"},
        {"px": "NaN"},
        {"sz": "Infinity"},
    ],
)
def test_touch_wrong_side_early_or_small_trade_is_not_a_fill(change):
    engine = ready()
    engine.on_book(book(2.275, "100.60"))
    trade = {"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + 2300}
    trade.update(change)
    engine.on_trade(trade, BASE + 2350)
    assert engine.position is None


def test_trade_before_post_latency_book_is_not_a_fill():
    engine = ready()
    engine.on_trade({"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + 2300}, BASE + 2350)
    assert engine.position is None
    assert engine.entry["resting_ms"] is None


def test_post_only_rejected_when_price_crosses_during_latency():
    engine = ready()
    engine.on_book(book(2.3, "100.70"))
    assert engine.entry is None
    assert engine.entries_rejected == 1
    engine.on_trade({"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + 2400}, BASE + 2450)
    assert engine.position is None


def test_already_resting_entry_is_not_rejected_by_a_later_price_rise():
    engine = ready()
    engine.on_book(book(2.3, "100.60"))
    assert engine.entry["resting_ms"] == BASE + 2300
    engine.on_book(book(2.4, "100.70"))
    engine.on_trade({"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + 2450}, BASE + 2500)
    assert engine.position is not None
    assert engine.entries_rejected == 0


def test_passive_trade_through_opens_only_one_small_short():
    engine = ready()
    order = engine.entry.copy()
    position = fill(engine)
    assert position["entry_price"] == order["price"]
    assert position["size"] * position["entry_price"] <= 20
    assert engine.balance == Decimal("1000") - position["entry_fee"]
    engine.on_book(book(3, "102"))
    assert engine.entry is None
    assert engine.entries_requested == 1


def test_take_profit_uses_executable_ask_not_mid_and_delays_exit():
    engine = ready(take_profit_pct="0.10")
    position = fill(engine)
    target = position["target_price"]
    engine.on_book(book(3, target))
    assert engine.exit is None  # Mid at target still leaves ask above target.
    engine.on_book(book(3.1, target - Decimal("0.02")))
    assert engine.exit["reason"] == "take_profit"
    engine.on_book(book(3.2, target - Decimal("0.02")))
    assert not engine.trades
    engine.on_book(book(3.4, target - Decimal("0.03")))
    trade = engine.trades[0]
    assert engine.position is None
    assert Decimal(trade["exit_price"]) > target - Decimal("0.02")  # Slippage applied.
    assert Decimal(trade["pnl_after_fees_usdc"]) < Decimal(trade["gross_pnl_usdc"])
    assert engine.balance - Decimal("1000") == Decimal(trade["pnl_after_fees_usdc"])


def test_stop_is_not_guaranteed_to_fill_at_stop_price():
    engine = ready()
    position = fill(engine)
    engine.on_book(book(3, "101.2"))
    assert engine.exit["reason"] == "stop_loss"
    engine.on_book(book(3.4, "102"))
    trade = engine.trades[0]
    assert Decimal(trade["exit_price"]) > position["stop_price"]
    assert Decimal(trade["pnl_after_fees_usdc"]) < 0


def test_max_holding_requests_exit_without_inventing_a_stale_fill():
    engine = ready(max_holding_s=1)
    fill(engine)
    engine.tick(BASE + 4000)
    assert engine.exit["reason"] == "max_holding_time"
    assert engine.position is not None
    assert not engine.trades
    engine.on_book(book(4.5, "100.60"))
    assert engine.trades[0]["exit_reason"] == "max_holding_time"
    assert engine.trades[0]["holding_s"] > 1


def test_expired_entry_cannot_fill_later():
    engine = ready(entry_timeout_s=1)
    engine.tick(BASE + 3000)
    engine.on_trade({"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": BASE + 3100}, BASE + 3150)
    assert engine.position is None
    assert engine.entries_expired == 1


def test_gap_cancels_unfilled_entry_and_rewarms_history():
    engine = ready()
    engine.gap(BASE + 2500, "disconnect")
    assert engine.entry is None
    engine.on_book(book(3, "102", generation=2))
    assert engine.entry is None
    assert engine.jump_pct is None


def test_book_gap_requests_risk_exit_and_no_averaging_down():
    engine = ready(gap_reset_s=2)
    fill(engine)
    engine.on_book(book(5, "102"))
    assert engine.exit["reason"] == "data_gap"
    assert engine.entry is None


def test_shallow_ask_depth_does_not_invent_a_complete_exit():
    engine = ready()
    fill(engine)
    engine.on_book(book(3, "102", ask_size="0.0001"))
    assert engine.exit is None
    engine.request_exit(BASE + 3100, "stop_loss")
    engine.on_book(book(3.5, "102", ask_size="0.0001"))
    assert engine.position is not None
    assert not engine.trades


def test_restart_keeps_position_balance_and_exits_after_fresh_data():
    engine = ready()
    fill(engine)
    state = engine.checkpoint()
    restored = SpikeFade(engine.config, engine.asset)
    restored.restore(json.loads(json.dumps(state)), BASE + 3000)
    assert restored.balance == engine.balance
    assert restored.position["size"] == engine.position["size"]
    assert restored.exit["reason"] == "data_gap"
    restored.on_book(book(3.5))
    assert len(restored.trades) == 1
    assert restored.position is None


def test_pending_entry_is_cancelled_on_restart_and_changed_config_rejected():
    engine = ready()
    restored = SpikeFade(engine.config, engine.asset)
    restored.restore(engine.checkpoint(), BASE + 3000)
    assert restored.entry is None
    with pytest.raises(ValueError, match="mismatch"):
        SpikeFade(replace(engine.config, stop_loss_pct="0.70"), engine.asset).restore(
            engine.checkpoint(), BASE + 3000
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"jump_threshold_pct": "NaN"},
        {"take_profit_pct": "0"},
        {"stop_loss_pct": "-1"},
        {"max_holding_s": float("inf")},
        {"lookback_s": 0},
        {"latency_ms": -1},
        {"order_usdc": "26"},
        {"starting_balance_usdc": "10"},
        {"entry_timeout_s": 0.1},
        {"max_holding_s": True},
    ],
)
def test_invalid_config_is_rejected(kwargs):
    with pytest.raises(ValueError):
        PaperConfig(**kwargs)


def test_csv_checkpoint_and_status_are_consistent_and_lock_exclusive(tmp_path):
    engine = ready()
    fill(engine)
    engine.on_book(book(3, "100.3"))
    engine.on_book(book(3.4, "100.3"))
    logs = paper.PaperLogs(tmp_path, engine.config)
    try:
        logs.save(engine, BASE + 4000, True)
        with pytest.raises(ValueError, match="already"):
            paper.PaperLogs(tmp_path, engine.config)
        assert len(json.loads((logs.path / "state.json").read_text())["trades"]) == 1
        assert (logs.path / "trades.csv").read_text().count(engine.trades[0]["trade_id"]) == 1
        logs.save(engine, BASE + 5000, False)
        assert (logs.path / "trades.csv").read_text().count(engine.trades[0]["trade_id"]) == 1
        assert json.loads((tmp_path / "status.json").read_text())["running"] is False
    finally:
        logs.close()


def test_different_parameters_get_separate_experiment_directories(tmp_path):
    logs = paper.PaperLogs(tmp_path, PaperConfig())
    first = logs.path
    logs.close()
    logs = paper.PaperLogs(tmp_path, PaperConfig(jump_threshold_pct="0.8"))
    assert first != logs.path
    logs.close()


def test_live_driver_paper_round_trip_without_model_or_exchange_calls(tmp_path, monkeypatch):
    real_client = httpx.AsyncClient

    def handler(request):
        assert request.url.path == "/info"
        assert json.loads(request.content) == {"type": "metaAndAssetCtxs"}
        return httpx.Response(200, json=[{"universe": [{"name": "ETH", "szDecimals": 4}]}, [{}]])

    monkeypatch.setattr(
        paper.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )

    class FakeFeed:
        def __init__(self, settings, emit, on_book, on_trade):
            self.emit, self.on_book, self.on_trade = emit, on_book, on_trade
            self.book = None

        async def run(self):
            self.emit({"event": "feed_connected"})
            for index, mid in enumerate(("100", "100", "100.6", "100.3", "100.3")):
                await asyncio.sleep(0.025)
                current = book(index, mid)
                now = time.time() * 1000
                self.book = replace(
                    current, exchange_ms=int(now), received_wall_ms=now, received_mono=time.monotonic()
                )
                self.on_book(self.book)
                if index == 2:
                    await asyncio.sleep(0.005)
                    self.on_trade(
                        {"coin": "ETH", "side": "B", "px": "101", "sz": "1", "time": int(time.time() * 1000)}
                    )
            await asyncio.sleep(10)

    monkeypatch.setattr(paper, "Feed", FakeFeed)
    config = PaperConfig(lookback_s=0.03, latency_ms=0)
    args = argparse.Namespace(output=tmp_path, duration=0.18)
    assert asyncio.run(paper.run(config, args)) == 0
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["closed_trades"] == 1
    assert status["orders_submitted"] == status["model_calls"] == 0
    assert status["funding_included"] is False


def test_failed_feed_records_error_and_releases_output_lock(tmp_path, monkeypatch):
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        paper.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json=[{"universe": [{"name": "ETH", "szDecimals": 4}]}, [{}]]
                )
            ),
            **kwargs,
        ),
    )

    class FailedFeed:
        def __init__(self, settings, emit, on_book, on_trade):
            self.book = None

        async def run(self):
            raise RuntimeError("feed failed")

    monkeypatch.setattr(paper, "Feed", FailedFeed)
    config = PaperConfig()
    args = argparse.Namespace(output=tmp_path, duration=1)
    assert asyncio.run(paper.run(config, args)) == 1
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["running"] is False
    assert status["error"] == "RuntimeError: feed failed"
    assert status["orders_submitted"] == status["model_calls"] == 0
    logs = paper.PaperLogs(tmp_path, config)
    logs.close()
