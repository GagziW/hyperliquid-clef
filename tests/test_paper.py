import argparse
import asyncio
import json
import sys
import time
from dataclasses import replace
from decimal import Decimal

import httpx
import pytest

from pipeline import compare, paper
from pipeline.market import Asset, Book
from pipeline.paper_engine import Entry, Exit, PaperEngine, SimulationConfig
from pipeline.strategies import Strategy, load_strategy
from pipeline.strategies.spike_fade import Params, SpikeFade

BASE = 1791612600000


def book(seconds, mid="100", generation=1, ask_size="1", bid_size="1"):
    mid = Decimal(str(mid))
    ts = BASE + int(seconds * 1000)
    return Book(
        ((mid - Decimal("0.01"), Decimal(bid_size)),),
        ((mid + Decimal("0.01"), Decimal(ask_size)),),
        ts,
        ts / 1000,
        ts,
        generation,
    )


def engine_for(strategy, **simulation):
    simulation.setdefault("latency_ms", 250)
    return PaperEngine(SimulationConfig(**simulation), strategy, Asset(1, 4))


def ready(simulation=None, **params):
    engine = engine_for(SpikeFade(Params(lookback_s=2, **params)), **(simulation or {}))
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


class Scripted(Strategy):
    """Sends one Entry, then an Exit when ``exit_now`` is set. For engine tests only."""

    name = "scripted"

    def __init__(self, request):
        super().__init__()
        self.request, self.sent, self.exit_now = request, False, False

    def on_book(self, book, account):
        if account.position and self.exit_now:
            return Exit("strategy_exit")
        if not self.sent and not account.position and not account.entry_pending:
            self.sent = True
            return self.request
        return None


# ── The example strategy: spike fade ────────────────────────────────────────────────────


def test_requires_complete_lookback_and_threshold_crossing():
    engine = engine_for(SpikeFade(Params(lookback_s=2)))
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
    assert position["side"] == "short"
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
    assert Decimal(trade["net_bps"]) < 0


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


def test_cooldown_blocks_a_new_entry_after_a_close():
    engine = ready(max_holding_s=1, cooldown_s=30)
    fill(engine)
    engine.tick(BASE + 4000)
    engine.on_book(book(4.5, "100.60"))
    assert engine.last_exit_ms is not None
    for second in range(5, 12):
        engine.on_book(book(second, "100" if second % 2 else "100.70"))
    assert engine.entries_requested == 1


def test_expired_entry_cannot_fill_later():
    engine = ready({"entry_timeout_s": 1})
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
    assert engine.strategy.jump_pct is None
    assert engine.history_resets == 1  # the gap already forgot the old generation


def test_book_gap_requests_risk_exit_and_no_averaging_down():
    engine = ready({"gap_reset_s": 2})
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
    restored = PaperEngine(engine.config, SpikeFade(engine.strategy.params), engine.asset)
    restored.restore(json.loads(json.dumps(state)), BASE + 3000)
    assert restored.balance == engine.balance
    assert restored.position["size"] == engine.position["size"]
    assert restored.exit["reason"] == "data_gap"
    restored.on_book(book(3.5))
    assert len(restored.trades) == 1
    assert restored.position is None


def test_pending_entry_is_cancelled_on_restart_and_changed_config_rejected():
    engine = ready()
    restored = PaperEngine(engine.config, SpikeFade(engine.strategy.params), engine.asset)
    restored.restore(engine.checkpoint(), BASE + 3000)
    assert restored.entry is None
    changed = SpikeFade(replace(engine.strategy.params, stop_loss_pct="0.70"))
    with pytest.raises(ValueError, match="mismatch"):
        PaperEngine(engine.config, changed, engine.asset).restore(engine.checkpoint(), BASE + 3000)
    with pytest.raises(ValueError, match="mismatch"):
        PaperEngine(
            replace(engine.config, latency_ms=500), SpikeFade(engine.strategy.params), engine.asset
        ).restore(engine.checkpoint(), BASE + 3000)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"jump_threshold_pct": "NaN"},
        {"take_profit_pct": "0"},
        {"stop_loss_pct": "-1"},
        {"max_holding_s": float("inf")},
        {"lookback_s": 0},
        {"max_holding_s": True},
    ],
)
def test_invalid_example_parameters_are_rejected(kwargs):
    with pytest.raises(ValueError):
        Params(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"latency_ms": -1},
        {"order_usdc": "26"},
        {"starting_balance_usdc": "10"},
        {"entry_timeout_s": 0.1},
        {"taker_fee_bps": "NaN"},
        {"network": "other"},
    ],
)
def test_invalid_simulation_is_rejected(kwargs):
    with pytest.raises(ValueError):
        SimulationConfig(**kwargs)


def test_only_the_lab_defaults_are_standard():
    assert SimulationConfig().is_standard()
    assert not SimulationConfig(taker_fee_bps="2").is_standard()


# ── Engine features for your own strategy: long, taker, own exits ───────────────────────


def long_engine(**entry):
    request = Entry(side="long", max_holding_s=60, **entry)
    engine = engine_for(Scripted(request))
    engine.on_book(book(0))
    assert engine.entry["side"] == "long"
    return engine


def test_long_maker_entry_rests_at_bid_and_fills_only_on_a_seller_trade_through():
    engine = long_engine(take_profit_pct="0.10", stop_loss_pct="0.50")
    assert engine.entry["price"] == Decimal("99.99")
    engine.on_book(book(0.3))
    assert engine.entry["resting_ms"] == BASE + 300
    engine.on_trade({"coin": "ETH", "side": "A", "px": "99.99", "sz": "1", "time": BASE + 400}, BASE + 450)
    engine.on_trade({"coin": "ETH", "side": "B", "px": "99.98", "sz": "1", "time": BASE + 400}, BASE + 450)
    assert engine.position is None  # a touch and a buyer trade are not fills
    engine.on_trade({"coin": "ETH", "side": "A", "px": "99.98", "sz": "1", "time": BASE + 500}, BASE + 550)
    assert engine.position["side"] == "long"
    assert engine.position["target_price"] > engine.position["entry_price"] > engine.position["stop_price"]


def test_long_post_only_is_rejected_if_the_ask_falls_to_the_limit():
    engine = long_engine()
    engine.on_book(book(0.3, "99.97"))
    assert engine.entry is None
    assert engine.entries_rejected == 1


def test_long_take_profit_uses_executable_bid_and_makes_money():
    engine = long_engine(take_profit_pct="0.10", stop_loss_pct="0.50")
    engine.on_book(book(0.3))
    engine.on_trade({"coin": "ETH", "side": "A", "px": "99.98", "sz": "1", "time": BASE + 400}, BASE + 450)
    engine.on_book(book(1, "100.10"))
    assert engine.exit["reason"] == "take_profit"
    engine.on_book(book(1.3, "100.10"))
    trade = engine.trades[0]
    assert Decimal(trade["exit_price"]) < Decimal("100.09")  # sold below the bid: slippage
    assert Decimal(trade["pnl_after_fees_usdc"]) > 0
    assert trade["side"] == "long"


def test_long_stop_loss_loses_money():
    engine = long_engine(take_profit_pct="0.10", stop_loss_pct="0.50")
    engine.on_book(book(0.3))
    engine.on_trade({"coin": "ETH", "side": "A", "px": "99.98", "sz": "1", "time": BASE + 400}, BASE + 450)
    engine.on_book(book(1, "99.40"))
    assert engine.exit["reason"] == "stop_loss"
    engine.on_book(book(1.3, "99.40"))
    assert Decimal(engine.trades[0]["pnl_after_fees_usdc"]) < 0


def test_taker_entry_fills_after_latency_against_depth_with_slippage_and_taker_fee():
    engine = long_engine(style="taker")
    engine.on_book(book(0.1))
    assert engine.position is None  # still inside the latency
    engine.on_book(book(0.3))
    position = engine.position
    assert position["entry_price"] > Decimal("100.01")
    assert position["entry_fee"] == position["entry_price"] * position["size"] * Decimal("4.5") / 10000


def test_taker_entry_without_enough_depth_is_rejected():
    engine = long_engine(style="taker")
    engine.on_book(book(0.3, ask_size="0.0001"))
    assert engine.position is None
    assert engine.entries_rejected == 1


def test_strategy_exit_request_closes_after_latency():
    engine = long_engine(style="taker")
    engine.on_book(book(0.3))
    engine.strategy.exit_now = True
    engine.on_book(book(1))
    assert engine.exit["reason"] == "strategy_exit"
    engine.on_book(book(1.3))
    assert engine.trades[0]["exit_reason"] == "strategy_exit"


def test_wrong_strategy_return_value_is_an_error():
    class Broken(Strategy):
        def on_book(self, book, account):
            return "BUY"

    with pytest.raises(TypeError, match="expected Entry"):
        engine_for(Broken()).on_book(book(0))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"side": "up", "max_holding_s": 10},
        {"side": "long", "max_holding_s": 0},
        {"side": "long", "max_holding_s": 90000},
        {"side": "long", "max_holding_s": 10, "style": "limit"},
        {"side": "long", "max_holding_s": 10, "take_profit_pct": "0"},
    ],
)
def test_invalid_entry_requests_are_rejected(kwargs):
    with pytest.raises(ValueError):
        Entry(**kwargs)


# ── Strategy loading and configuration ──────────────────────────────────────────────────


def test_strategy_loader_finds_files_and_checks_parameters():
    assert load_strategy("spike_fade", {"jump_threshold_pct": 0.8}).params.jump_threshold_pct == Decimal(
        "0.8"
    )
    assert load_strategy("my_strategy").name == "my_strategy"
    assert load_strategy("ai_template").params.provider == "mock"
    with pytest.raises(ValueError, match="Unknown parameters"):
        load_strategy("spike_fade", {"jump": 1})
    with pytest.raises(ValueError, match="No strategy file"):
        load_strategy("does_not_exist")
    with pytest.raises(ValueError, match="Invalid strategy name"):
        load_strategy("../paper")


def test_template_strategy_does_not_trade_until_you_write_a_rule():
    engine = engine_for(load_strategy("my_strategy"))
    for second, mid in enumerate(("100", "95", "105", "100")):
        engine.on_book(book(second, mid))
    assert engine.entries_requested == 0


def test_config_file_selects_strategies_and_rejects_unknown_keys(tmp_path):
    path = tmp_path / "paper.toml"
    path.write_text('run = ["spike_fade"]\n[simulation]\nlatency_ms = 250.0\n')
    simulation, strategies = paper.load_config(path)
    assert simulation.is_standard() and [s.name for s in strategies] == ["spike_fade"]
    assert [s.name for s in paper.load_config(path, ["my_strategy"])[1]] == ["my_strategy"]
    path.write_text("[simulation]\nfee = 1\n")
    with pytest.raises(ValueError, match="simulation"):
        paper.load_config(path)
    path.write_text('run = ["spike_fade", "spike_fade"]\n')
    with pytest.raises(ValueError, match="only once"):
        paper.load_config(path)


def test_repository_config_is_standard_and_runs_the_example():
    simulation, strategies = paper.load_config(paper.Path(paper.__file__).parent.parent / "paper.toml")
    assert simulation.is_standard()
    assert strategies[0].name == "spike_fade"
    assert strategies[0].params == Params()


# ── Logs, runner and comparison ─────────────────────────────────────────────────────────


def test_csv_checkpoint_and_status_are_consistent_and_lock_exclusive(tmp_path):
    engine = ready()
    fill(engine)
    engine.on_book(book(3, "100.3"))
    engine.on_book(book(3.4, "100.3"))
    logs = paper.PaperLogs(tmp_path, engine.config, engine.strategy)
    try:
        logs.save(engine, BASE + 4000, True)
        with pytest.raises(ValueError, match="already"):
            paper.PaperLogs(tmp_path, engine.config, engine.strategy)
        assert len(json.loads((logs.path / "state.json").read_text())["trades"]) == 1
        assert (logs.path / "trades.csv").read_text().count(engine.trades[0]["trade_id"]) == 1
        logs.save(engine, BASE + 5000, False)
        assert (logs.path / "trades.csv").read_text().count(engine.trades[0]["trade_id"]) == 1
        assert json.loads((tmp_path / "status.json").read_text())["running"] is False
    finally:
        logs.close()


def test_different_parameters_get_separate_experiment_directories(tmp_path):
    logs = paper.PaperLogs(tmp_path, SimulationConfig(), SpikeFade())
    first = logs.path
    logs.close()
    logs = paper.PaperLogs(tmp_path, SimulationConfig(), SpikeFade(Params(jump_threshold_pct="0.8")))
    assert first != logs.path
    logs.close()


def mock_metadata(monkeypatch):
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


def test_live_driver_runs_all_strategies_on_the_same_data(tmp_path, monkeypatch):
    mock_metadata(monkeypatch)

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
    simulation = SimulationConfig(latency_ms=0)
    strategies = [SpikeFade(Params(lookback_s=0.03)), load_strategy("my_strategy")]
    args = argparse.Namespace(output=tmp_path, duration=0.18)
    assert asyncio.run(paper.run(simulation, strategies, args)) == 0
    example = json.loads((tmp_path / "spike_fade" / "status.json").read_text())
    mine = json.loads((tmp_path / "my_strategy" / "status.json").read_text())
    assert example["closed_trades"] == 1
    assert example["orders_submitted"] == example["model_calls"] == 0
    assert example["funding_included"] is False
    assert example["simulation_standard"] is False  # latency changed for the test
    assert mine["closed_trades"] == 0
    assert mine["books_observed"] == example["books_observed"]


def test_failed_feed_records_error_and_releases_output_lock(tmp_path, monkeypatch):
    mock_metadata(monkeypatch)

    class FailedFeed:
        def __init__(self, settings, emit, on_book, on_trade):
            self.book = None

        async def run(self):
            raise RuntimeError("feed failed")

    monkeypatch.setattr(paper, "Feed", FailedFeed)
    simulation, strategy = SimulationConfig(), SpikeFade()
    args = argparse.Namespace(output=tmp_path, duration=1)
    assert asyncio.run(paper.run(simulation, [strategy], args)) == 1
    status = json.loads((tmp_path / "spike_fade" / "status.json").read_text())
    assert status["running"] is False
    assert status["error"] == "RuntimeError: feed failed"
    assert status["orders_submitted"] == status["model_calls"] == 0
    logs = paper.PaperLogs(tmp_path / "spike_fade", simulation, strategy)
    logs.close()


def test_failed_background_task_stops_the_run(tmp_path, monkeypatch):
    mock_metadata(monkeypatch)

    class QuietFeed:
        def __init__(self, settings, emit, on_book, on_trade):
            self.book = None

        async def run(self):
            await asyncio.sleep(10)

    class Crashing(Strategy):
        name = "crashing"

        async def background(self, feed, client):
            raise RuntimeError("model down")

    monkeypatch.setattr(paper, "Feed", QuietFeed)
    args = argparse.Namespace(output=tmp_path, duration=2)
    assert asyncio.run(paper.run(SimulationConfig(), [Crashing()], args)) == 1
    status = json.loads((tmp_path / "crashing" / "status.json").read_text())
    assert "model down" in status["error"]


def test_compare_marks_non_standard_runs(tmp_path, monkeypatch, capsys):
    for name, standard in (("spike_fade", True), ("my_strategy", False)):
        (tmp_path / name).mkdir()
        (tmp_path / name / "status.json").write_text(
            json.dumps({"simulation_standard": standard, "closed_trades": 3, "win_rate": 0.5})
        )
    monkeypatch.setattr(sys, "argv", ["compare", str(tmp_path)])
    compare.main()
    output = capsys.readouterr().out
    assert output.index("spike_fade") < output.index("my_strategy")
    assert "NOT COMPARABLE" in output and "50.0 %" in output


def test_a_locked_view_file_does_not_stop_the_run(tmp_path, monkeypatch):
    # On Windows, Excel can lock trades.csv. The checkpoint must still be written.
    def locked(self, target):
        raise PermissionError("locked by another program")

    temp = tmp_path / "trades.csv.tmp"
    temp.write_text("x")
    monkeypatch.setattr(paper.time, "sleep", lambda s: None)
    monkeypatch.setattr(paper.Path, "replace", locked)
    assert paper.replace_file(temp, tmp_path / "trades.csv", required=False) is False
    with pytest.raises(PermissionError):
        paper.replace_file(temp, tmp_path / "trades.csv")
