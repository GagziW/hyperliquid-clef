"""Re-check the buy-the-dip rule chosen by dip.py on extra history, and measure its downside.

The rule (picked on 2024-01..2025-06 only): after ETH falls 1.5% or more within 60 s, rest a buy
limit 0.20% below that price for up to 30 s; if filled, hold 300 s and sell at market. No target,
no stop. 2023 was not used for any choice, so it is a second unseen period.

Usage:  python3 -I backtest/dip_check.py --months 2023-01..2026-09
"""

import argparse
import importlib.util
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extract, bt = load("extract"), load("analyze")
THRESHOLD, ENTRY, HOLD, NONE = 1.5, "above20", 300, 1e9


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", required=True)
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()

    extract.THRESHOLDS = [THRESHOLD]
    meta, paths = extract.extract(extract.chunks(argparse.Namespace(months=args.months, days=None)), args.workers)
    n = meta["fwd_s"] + 1
    evs = sorted(
        (e[0], *bt.get_path(paths, e[5], n, -1))
        for e in meta["events"]
        if e[1] == -1 and e[2] == THRESHOLD and e[4] <= bt.MAX_GAP
    )
    trades = bt.simulate(bt.prepare(evs, ENTRY), NONE, NONE, HOLD, False)
    when = lambda ts: datetime.fromtimestamp(ts, timezone.utc)  # noqa: E731

    print("\nBUY 0.20% BELOW AFTER A 1.5% ONE-MINUTE FALL, HOLD 5 MINUTES (after costs)")
    for year, s in bt.by_year(trades).items():
        note = "  <- never used for any choice" if year == 2023 else ""
        print(f"  {year}: {bt.fmt(s)}{note}")
    print(f"  all : {bt.fmt(bt.stats(trades))}")

    ranked = sorted(trades, key=lambda t: t[1])
    print("\n  worst 5 trades: " + ", ".join(f"{when(t[0]):%Y-%m-%d} {t[1]:+.0f} bp" for t in ranked[:5]))
    print("  best 5 trades:  " + ", ".join(f"{when(t[0]):%Y-%m-%d} {t[1]:+.0f} bp" for t in ranked[-5:][::-1]))
    big_losses = sum(t[1] <= -100 for t in trades)
    print(f"  trades losing 1% or more: {big_losses} of {len(trades)}")

    peak = cum = drawdown = 0.0
    streak = longest = 0
    for t in trades:
        cum += t[1]
        peak = max(peak, cum)
        drawdown = max(drawdown, peak - cum)
        streak = streak + 1 if t[1] <= 0 else 0
        longest = max(longest, streak)
    print(f"  cumulative {cum:+.0f} bp; worst peak-to-trough {drawdown:.0f} bp; longest losing streak {longest}")
    print(f"  at $20 per trade: {20 * cum / 1e4:+.2f} USD over {len(trades)} trades "
          f"({len(trades) / (len(meta['chunks']) / 12):.0f} trades per year)")


if __name__ == "__main__":
    main()
