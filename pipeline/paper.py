"""Headless paper trader using only public Hyperliquid data.

All strategies in ``run = [...]`` (paper.toml) see the SAME live market events in one process,
with the SAME fixed simulation settings. For this reason their results are directly comparable.
"""

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import platform
import signal
import socket
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import httpx

from pipeline.config import Settings
from pipeline.keepawake import keep_awake
from pipeline.market import Feed, Hyperliquid
from pipeline.paper_engine import PaperEngine, SimulationConfig
from pipeline.strategies import has_background, load_strategy


def replace_file(temp, path, required=True):
    # On Windows a file that another program has open (Excel, an editor, --status) cannot be
    # replaced for a moment. Retry briefly; a view such as trades.csv can wait for the next save.
    for attempt in range(20):
        try:
            temp.replace(path)
            return True
        except PermissionError:
            if attempt == 19:
                if required:
                    raise
                return False
            time.sleep(0.05)


def write_json(path, value, required=True):
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w") as file:
        json.dump(value, file, indent=2, allow_nan=False)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    return replace_file(temp, path, required)


def lock_exclusive(file):
    """Non-blocking exclusive lock that works on Windows, macOS and Linux."""
    try:
        if os.name == "nt":
            import msvcrt

            file.seek(0)
            msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def unlock(file):
    if os.name == "nt":
        import msvcrt

        try:
            file.seek(0)
            msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    file.close()


TRADE_COLUMNS = [
    "trade_id",
    "side",
    "style",
    "entry_ms",
    "exit_ms",
    "entry_price",
    "exit_price",
    "size",
    "target_price",
    "stop_price",
    "entry_fee",
    "exit_fee",
    "gross_pnl_usdc",
    "pnl_after_fees_usdc",
    "net_bps",
    "holding_s",
    "exit_reason",
    "paper_balance_usdc",
    "funding_boundaries_crossed",
    "funding_included",
    "note",
]


class PaperLogs:
    """Output of one strategy: <output>/<strategy>/status.json and <output>/<strategy>/<config-hash>/."""

    def __init__(self, root, simulation, strategy):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / "paper.lock").open("a+")
        if not lock_exclusive(self.lock):
            self.lock.close()
            raise ValueError("A paper trader already owns this output directory") from None
        identity = {"simulation": simulation.record(), "strategy": strategy.record()}
        fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:12]
        self.path = self.root / fingerprint
        self.path.mkdir(exist_ok=True)
        self.file = None
        self.day = None
        self.host = {
            "hostname": socket.gethostname(),
            "system": platform.system(),
            "architecture": platform.machine(),
        }

    def emit(self, event):
        now = datetime.now(timezone.utc)
        day = now.strftime("%Y-%m-%d")
        if self.day != day:
            if self.file:
                self.file.close()
            self.file = (self.path / f"events-{day}.jsonl").open("a", buffering=1)
            self.day = day
        self.file.write(json.dumps({"recorded_utc": now.isoformat(), **event}, allow_nan=False) + "\n")

    def save(self, engine, now_ms, running, error=None):
        # The checkpoint is authoritative; CSV is a regenerated view, never an append-only double count.
        write_json(self.path / "state.json", engine.checkpoint())
        temp = self.path / "trades.csv.tmp"
        with temp.open("w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=TRADE_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for trade in engine.trades:
                writer.writerow({**trade, "note": json.dumps(trade.get("note", {}), sort_keys=True)})
        replace_file(temp, self.path / "trades.csv", required=False)
        summary = {
            **engine.summary(now_ms),
            "running": running,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "host": self.host,
            "pid": os.getpid(),
            "output_directory": str(self.path),
            "error": error,
        }
        write_json(self.path / "summary.json", summary, required=False)
        write_json(self.root / "status.json", summary, required=False)
        return summary

    def close(self):
        if self.file:
            self.file.close()
        unlock(self.lock)


def load_config(path, selected=None):
    """Read paper.toml: the FIXED [simulation] settings and the strategies to run."""
    with path.open("rb") as file:
        values = tomllib.load(file)
    unknown = set(values) - {"run", "simulation", "strategies"}
    if unknown:
        raise ValueError("Unknown paper.toml keys: " + ", ".join(sorted(unknown)))
    try:
        simulation = SimulationConfig(**values.get("simulation", {}))
    except TypeError as exc:
        raise ValueError(f"Unknown [simulation] setting: {exc}") from None
    names = selected or values.get("run") or ["spike_fade"]
    if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
        raise ValueError("run must be a list of strategy names")
    if len(set(names)) != len(names):
        raise ValueError("Each strategy can run only once")
    params = values.get("strategies", {})
    if not isinstance(params, dict):
        raise ValueError("[strategies] must be a table")
    return simulation, [load_strategy(name, params.get(name, {})) for name in names]


def install_stop_handlers(loop, stop):
    """Ctrl+C and SIGTERM stop the run cleanly. Windows has no loop signal handlers."""
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        return lambda: [loop.remove_signal_handler(sig) for sig in (signal.SIGINT, signal.SIGTERM)]
    except (NotImplementedError, RuntimeError):
        previous = signal.signal(signal.SIGINT, lambda *_: loop.call_soon_threadsafe(stop.set))
        return lambda: signal.signal(signal.SIGINT, previous)


async def run(simulation, strategies, args):
    logs = []
    try:
        for strategy in strategies:
            logs.append(PaperLogs(args.output / strategy.name, simulation, strategy))
    except ValueError:
        for item in logs:
            item.close()
        raise
    engines = []
    feed_task = None
    background = {}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    remove_handlers = install_stop_handlers(loop, stop)
    error = None
    summaries = {}
    queue = asyncio.Queue(maxsize=4096)

    def enqueue(event):
        if queue.full():
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(("gap", "queue_overflow", int(time.time() * 1000)))
        queue.put_nowait(event)

    def feed_event(event):
        if event["event"] != "market_data":
            for item in logs:
                item.emit(event)
        if event["event"] in {"feed_connected", "feed_disconnected"}:
            enqueue(("gap", event["event"], int(time.time() * 1000)))

    def emit_all(event):
        for item in logs:
            item.emit(event)

    try:
        settings = Settings(network=simulation.network, max_book_age_ms=simulation.max_book_age_ms)
        async with httpx.AsyncClient(timeout=10) as client:
            asset, _ = await Hyperliquid(client, settings).eth_meta()
            for strategy, item in zip(strategies, logs, strict=True):
                engine = PaperEngine(simulation, strategy, asset)
                state_path = item.path / "state.json"
                if state_path.exists():
                    engine.restore(json.loads(state_path.read_text()), int(time.time() * 1000))
                engines.append(engine)
                item.emit(
                    {
                        "event": "paper_started",
                        "simulation": simulation.record(),
                        "strategy": strategy.record(),
                        "host": item.host,
                        "orders_submitted": 0,
                    }
                )
                print(
                    json.dumps(
                        {
                            "event": "paper_started",
                            "strategy": strategy.name,
                            "output_directory": str(item.path),
                            "params": strategy.record()["params"],
                        }
                    ),
                    flush=True,
                )
            if not simulation.is_standard():
                print(
                    json.dumps(
                        {
                            "warning": "The [simulation] settings differ from the lab values. "
                            "These results are NOT comparable with other students' results."
                        }
                    ),
                    flush=True,
                )
            feed = Feed(
                settings,
                feed_event,
                on_book=lambda book: enqueue(("book", book, int(time.time() * 1000))),
                on_trade=lambda trade: enqueue(("trade", trade, int(time.time() * 1000))),
            )
            feed_task = asyncio.create_task(feed.run())
            for strategy in strategies:
                if has_background(strategy):
                    background[strategy.name] = asyncio.create_task(strategy.background(feed, client))
            started, last_save = time.monotonic(), 0.0
            stale = False
            while not stop.is_set() and (args.duration == 0 or time.monotonic() - started < args.duration):
                event = None
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=0.2)
                except TimeoutError:
                    pass
                now_ms = int(time.time() * 1000)
                if event:
                    kind, data, received_ms = event
                    if kind == "book":
                        age = data.estimated_exchange_age_ms()
                        if (
                            data.age_ms() <= simulation.max_book_age_ms
                            and -250 <= age <= simulation.max_book_age_ms
                        ):
                            stale = False
                            for engine in engines:
                                engine.on_book(data)
                        elif not stale:
                            stale = True
                            for engine in engines:
                                engine.gap(now_ms, "stale_book_or_clock_skew")
                    elif kind == "trade":
                        if (
                            not stale
                            and feed.book is not None
                            and feed.book.age_ms() <= simulation.max_book_age_ms
                        ):
                            for engine in engines:
                                engine.on_trade(data, received_ms)
                    else:
                        for engine in engines:
                            engine.gap(received_ms, data)
                if feed.book is not None and feed.book.age_ms() > simulation.max_book_age_ms and not stale:
                    stale = True
                    for engine in engines:
                        engine.gap(now_ms, "stale_local_book")
                periodic = time.monotonic() - last_save >= 5
                for engine, item in zip(engines, logs, strict=False):
                    engine.tick(now_ms)
                    events = engine.drain_events()
                    if events or periodic:
                        summaries[engine.strategy.name] = summary = item.save(engine, now_ms, running=True)
                        for record in events:
                            item.emit(record)
                            if record["event"] in {"position_opened", "position_closed", "entry_expired"}:
                                print(json.dumps({"strategy": engine.strategy.name, **record}), flush=True)
                        item.emit({"event": "paper_status", **summary})
                if periodic:
                    last_save = time.monotonic()
                if feed_task.done():
                    await feed_task
                    raise RuntimeError("Public data task stopped unexpectedly")
                for name, task in background.items():
                    if task.done():
                        cause = task.exception() if not task.cancelled() else None
                        raise RuntimeError(f"Background task of strategy {name} stopped: {cause!r}")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        emit_all({"event": "paper_error", "error": error})
    finally:
        for task in [feed_task, *background.values()]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    if error is None:
                        error = f"{type(exc).__name__}: {exc}"
                        emit_all({"event": "paper_error", "error": error})
        for engine, item in zip(engines, logs, strict=False):
            if engine.entry:
                item.emit(
                    {
                        "event": "entry_cancelled",
                        "reason": "process_stopping",
                        "order": engine.checkpoint()["entry"],
                    }
                )
                engine.entry = None
            summaries[engine.strategy.name] = item.save(
                engine, int(time.time() * 1000), running=False, error=error
            )
        for item in logs[len(engines) :]:
            summaries[item.root.name] = summary = {
                "running": False,
                "error": error,
                "orders_submitted": 0,
                "host": item.host,
            }
            write_json(item.root / "status.json", summary)
        emit_all({"event": "paper_stopped", "error": error})
        for item in logs:
            item.close()
        remove_handlers()
    print(json.dumps(summaries, indent=2), flush=True)
    return 1 if error else 0


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="ETH PAPER trader. No exchange submissions. Strategies are set in paper.toml."
    )
    parser.add_argument("--config", type=Path, default=root / "paper.toml")
    parser.add_argument("--output", type=Path, default=root / "runs" / "paper")
    parser.add_argument("--duration", type=float, default=0, help="Run seconds; 0 means until stopped")
    parser.add_argument(
        "--strategy",
        action="append",
        help="Run only this strategy (repeat the option for more). Default: run = [...] in paper.toml",
    )
    parser.add_argument("--allow-sleep", action="store_true", help="Do not keep the computer awake")
    parser.add_argument(
        "--status", action="store_true", help="Read the latest paper status without connecting"
    )
    args = parser.parse_args()
    if args.status:
        paths = sorted(args.output.glob("*/status.json"))
        if not paths:
            parser.error("No paper status exists in this output directory")
        print(json.dumps({p.parent.name: json.loads(p.read_text()) for p in paths}, indent=2))
        return
    if not math.isfinite(args.duration) or args.duration < 0:
        parser.error("Duration must be finite and nonnegative")
    try:
        simulation, strategies = load_config(args.config, args.strategy)
        if args.allow_sleep:
            code = asyncio.run(run(simulation, strategies, args))
        else:
            with keep_awake():
                code = asyncio.run(run(simulation, strategies, args))
    except (ValueError, OSError, TypeError) as exc:
        parser.error(str(exc))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
