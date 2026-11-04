"""Compare the strategies of one paper run side by side.

Usage:  uv run python -m pipeline.compare            (reads runs/paper/*/status.json)
        uv run python -m pipeline.compare runs/paper
"""

import argparse
import json
from decimal import Decimal
from pathlib import Path

ROWS = [
    ("simulation settings", lambda s: "standard" if s.get("simulation_standard") else "NOT COMPARABLE"),
    ("running", lambda s: "yes" if s.get("running") else "no"),
    ("observed hours", lambda s: f"{s['observed_s'] / 3600:.1f}" if s.get("observed_s") else "-"),
    ("order books observed", lambda s: s.get("books_observed", "-")),
    ("data gaps", lambda s: s.get("history_resets", "-")),
    ("entries requested", lambda s: s.get("entries_requested", "-")),
    ("entries rejected", lambda s: s.get("entries_rejected", "-")),
    ("entries expired", lambda s: s.get("entries_expired", "-")),
    ("closed trades", lambda s: s.get("closed_trades", "-")),
    ("win rate", lambda s: f"{s['win_rate'] * 100:.1f} %" if s.get("win_rate") is not None else "-"),
    ("P&L after fees (USDC)", lambda s: number(s.get("closed_pnl_after_fees_usdc"), 4)),
    ("mean net per trade (bp)", lambda s: number(s.get("mean_net_bps_per_trade"), 2)),
    ("max drawdown (USDC)", lambda s: number(s.get("max_observed_drawdown_usdc"), 4)),
    ("model calls", lambda s: s.get("model_calls", 0)),
    ("error", lambda s: s.get("error") or "-"),
]


def number(value, places):
    return "-" if value is None else f"{Decimal(str(value)):.{places}f}"


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Compare paper strategies of one run")
    parser.add_argument("output", nargs="?", type=Path, default=root / "runs" / "paper")
    args = parser.parse_args()
    statuses = {p.parent.name: json.loads(p.read_text()) for p in sorted(args.output.glob("*/status.json"))}
    if not statuses:
        parser.error(f"No */status.json in {args.output}")
    names = list(statuses)
    if "spike_fade" in names:  # the benchmark first
        names.insert(0, names.pop(names.index("spike_fade")))
    table = [["", *names]] + [[label, *(str(f(statuses[n])) for n in names)] for label, f in ROWS]
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for index, row in enumerate(table):
        print("  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)))
        if index == 0:
            print("  ".join("-" * width for width in widths))
    print("\nFunding is not modelled. Paper results do not show live performance.")


if __name__ == "__main__":
    main()
