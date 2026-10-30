"""Headless paper trader using only public Hyperliquid data."""

import argparse
import asyncio
import csv
import fcntl
import hashlib
import json
import math
import os
import platform
import signal
import socket
import time
import tomllib
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path

import httpx

from pipeline.config import Settings
from pipeline.market import Feed, Hyperliquid
from pipeline.paper_strategy import PaperConfig, SpikeFade


def write_json(path, value):
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w") as file:
        json.dump(value, file, indent=2, allow_nan=False)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    temp.replace(path)


class PaperLogs:
    def __init__(self, root, config):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / "paper.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise ValueError("A paper trader already owns this output directory") from None
        fingerprint = hashlib.sha256(json.dumps(config.record(), sort_keys=True).encode()).hexdigest()[:12]
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
        columns = [
            "trade_id",
            "entry_ms",
            "exit_ms",
            "entry_price",
            "exit_price",
            "size",
            "entry_jump_pct",
            "target_price",
            "stop_price",
            "entry_fee",
            "exit_fee",
            "gross_pnl_usdc",
            "pnl_after_fees_usdc",
            "holding_s",
            "exit_reason",
            "paper_balance_usdc",
            "funding_boundaries_crossed",
            "funding_included",
        ]
        with temp.open("w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(engine.trades)
        temp.replace(self.path / "trades.csv")
        summary = {
            **engine.summary(now_ms),
            "running": running,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "host": self.host,
            "pid": os.getpid(),
            "output_directory": str(self.path),
            "error": error,
        }
        write_json(self.path / "summary.json", summary)
        write_json(self.root / "status.json", summary)
        return summary

    def close(self):
        if self.file:
            self.file.close()
        self.lock.close()


def load_config(args):
    with args.config.open("rb") as file:
        values = tomllib.load(file)
    supported = {f.name for f in fields(PaperConfig)}
    unknown = set(values) - supported
    if unknown:
        raise ValueError("Unknown paper settings: " + ", ".join(sorted(unknown)))
    for name in ("jump_threshold_pct", "lookback_s", "take_profit_pct", "stop_loss_pct", "max_holding_s"):
        override = getattr(args, name)
        if override is not None:
            values[name] = override
    return PaperConfig(**values)


async def run(config, args):
    logs = PaperLogs(args.output, config)
    engine = None
    feed_task = None
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    error = None
    summary = None
    queue = asyncio.Queue(maxsize=4096)

    def enqueue(event):
        if queue.full():
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(("gap", "queue_overflow", int(time.time() * 1000)))
        queue.put_nowait(event)

    def feed_event(event):
        if event["event"] != "market_data":
            logs.emit(event)
        if event["event"] in {"feed_connected", "feed_disconnected"}:
            enqueue(("gap", event["event"], int(time.time() * 1000)))

    try:
        settings = Settings(network=config.network, max_book_age_ms=config.max_book_age_ms)
        async with httpx.AsyncClient(timeout=10) as client:
            asset, _ = await Hyperliquid(client, settings).eth_meta()
            engine = SpikeFade(config, asset)
            state_path = logs.path / "state.json"
            if state_path.exists():
                engine.restore(json.loads(state_path.read_text()), int(time.time() * 1000))
            logs.emit(
                {
                    "event": "paper_started",
                    "config": config.record(),
                    "host": logs.host,
                    "orders_submitted": 0,
                    "model_calls": 0,
                }
            )
            print(
                json.dumps(
                    {
                        "event": "paper_started",
                        "host": logs.host,
                        "output_directory": str(logs.path),
                        "config": config.record(),
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
                        if data.age_ms() <= config.max_book_age_ms and -250 <= age <= config.max_book_age_ms:
                            stale = False
                            engine.on_book(data)
                        elif not stale:
                            stale = True
                            engine.gap(now_ms, "stale_book_or_clock_skew")
                    elif kind == "trade":
                        if (
                            not stale
                            and feed.book is not None
                            and feed.book.age_ms() <= config.max_book_age_ms
                        ):
                            engine.on_trade(data, received_ms)
                    else:
                        engine.gap(received_ms, data)
                if feed.book is not None and feed.book.age_ms() > config.max_book_age_ms and not stale:
                    stale = True
                    engine.gap(now_ms, "stale_local_book")
                engine.tick(now_ms)
                events = engine.drain_events()
                if events or time.monotonic() - last_save >= 5:
                    summary = logs.save(engine, now_ms, running=True)
                    last_save = time.monotonic()
                    for item in events:
                        logs.emit(item)
                        if item["event"] in {"short_opened", "short_closed", "entry_expired"}:
                            print(json.dumps(item), flush=True)
                    logs.emit({"event": "paper_status", **summary})
                if feed_task.done():
                    await feed_task
                    raise RuntimeError("Public data task stopped unexpectedly")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        logs.emit({"event": "paper_error", "error": error})
    finally:
        if feed_task:
            feed_task.cancel()
            try:
                await feed_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                if error is None:
                    error = f"{type(exc).__name__}: {exc}"
                    logs.emit({"event": "paper_error", "error": error})
        if engine:
            if engine.entry:
                logs.emit(
                    {
                        "event": "entry_cancelled",
                        "reason": "process_stopping",
                        "order": engine.checkpoint()["entry"],
                    }
                )
                engine.entry = None
            summary = logs.save(engine, int(time.time() * 1000), running=False, error=error)
        else:
            summary = {
                "running": False,
                "error": error,
                "orders_submitted": 0,
                "model_calls": 0,
                "host": logs.host,
            }
            write_json(logs.root / "status.json", summary)
        logs.emit({"event": "paper_stopped", "error": error})
        logs.close()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
    print(json.dumps(summary, indent=2), flush=True)
    return 1 if error else 0


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Short-only ETH PAPER trader. No exchange submissions or model calls."
    )
    parser.add_argument("--config", type=Path, default=root / "paper.toml")
    parser.add_argument("--output", type=Path, default=root / "runs" / "paper")
    parser.add_argument("--duration", type=float, default=0, help="Run seconds; 0 means until stopped")
    parser.add_argument("--jump-threshold-pct")
    parser.add_argument("--lookback-s", type=float)
    parser.add_argument("--take-profit-pct")
    parser.add_argument("--stop-loss-pct")
    parser.add_argument("--max-holding-s", type=float)
    parser.add_argument(
        "--status", action="store_true", help="Read the latest paper status without connecting"
    )
    args = parser.parse_args()
    if args.status:
        path = args.output / "status.json"
        if not path.exists():
            parser.error("No paper status exists in this output directory")
        print(path.read_text())
        return
    if not math.isfinite(args.duration) or args.duration < 0:
        parser.error("Duration must be finite and nonnegative")
    try:
        config = load_config(args)
        code = asyncio.run(run(config, args))
    except (ValueError, OSError, TypeError) as exc:
        parser.error(str(exc))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
