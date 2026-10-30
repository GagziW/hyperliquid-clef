"""Export compact, shareable paper-trading data from a paper experiment directory.

Usage:  python3 -I scripts/paper_summary.py runs/paper/<experiment-id> data/paper-trading

Writes trades.csv (closed trades plus net basis points), daily.csv (signals, fills, resets and
estimated blind time per UTC day) and summary.json. The raw event logs are not copied.
"""

import csv
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

BLIND_S = 60  # a signal-history reset leaves the 60 s lookback empty for this long
SUMMARY_KEYS = [
    "strategy", "config", "closed_trades", "winning_trades", "win_rate", "closed_pnl_after_fees_usdc",
    "paper_balance_usdc", "max_observed_drawdown_usdc", "entries_requested", "entries_expired",
    "entries_rejected", "books_observed", "funding_included", "orders_submitted", "model_calls", "updated_utc",
]


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def main():
    source, target = Path(sys.argv[1]), Path(sys.argv[2])
    target.mkdir(parents=True, exist_ok=True)
    state = json.loads((source / "state.json").read_text())
    trades = state["trades"]

    with (target / "trades.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["entry_utc", "exit_utc", "entry_price", "exit_price", "size_eth", "holding_s",
                         "exit_reason", "pnl_after_fees_usdc", "net_bps"])
        for t in trades:
            notional = Decimal(t["entry_price"]) * Decimal(t["size"])
            pnl = Decimal(t["pnl_after_fees_usdc"])
            writer.writerow([f"{utc(t['entry_ms']):%Y-%m-%d %H:%M:%S}", f"{utc(t['exit_ms']):%Y-%m-%d %H:%M:%S}",
                             t["entry_price"], f"{Decimal(t['exit_price']):.2f}", t["size"], t["holding_s"],
                             t["exit_reason"], f"{pnl:.4f}", f"{pnl / notional * 10000:.2f}"])

    days, resets, first_ms, last_ms = {}, [], None, None
    counted = ("entry_requested", "entry_rejected", "entry_expired", "short_opened", "feed_disconnected")
    for path in sorted(source.glob("events-*.jsonl")):
        with path.open() as f:
            for line in f:
                if '"paper_status"' in line:
                    continue
                event = json.loads(line)
                ms = event.get("time_ms") or datetime.fromisoformat(event["recorded_utc"]).timestamp() * 1000
                first_ms = ms if first_ms is None else min(first_ms, ms)
                last_ms = ms if last_ms is None else max(last_ms, ms)
                day = days.setdefault(f"{utc(ms):%Y-%m-%d}", {k: 0 for k in counted} | {
                    "trades_closed": 0, "pnl_after_fees_usdc": Decimal("0"), "history_resets": 0, "blind_s": 0.0})
                kind = event["event"]
                if kind in counted:
                    day[kind] += 1
                elif kind == "short_closed":
                    day["trades_closed"] += 1
                    day["pnl_after_fees_usdc"] += Decimal(event["trade"]["pnl_after_fees_usdc"])
                elif kind == "signal_history_reset":
                    day["history_resets"] += 1
                    resets.append(ms / 1000)
    covered_until = 0.0
    for start in sorted(resets):
        lo, hi = max(start, covered_until), start + BLIND_S
        if hi > lo:
            days[f"{utc(lo * 1000):%Y-%m-%d}"]["blind_s"] += hi - lo
        covered_until = max(covered_until, hi)

    with (target / "daily.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        columns = ["signals", "rejected", "expired", "filled", "trades_closed", "pnl_after_fees_usdc",
                   "history_resets", "feed_disconnects", "blind_pct"]
        writer.writerow(["date_utc", *columns])
        for date, d in sorted(days.items()):
            writer.writerow([date, d["entry_requested"], d["entry_rejected"], d["entry_expired"], d["short_opened"],
                             d["trades_closed"], f"{d['pnl_after_fees_usdc']:.4f}", d["history_resets"],
                             d["feed_disconnected"], f"{100 * d['blind_s'] / 86400:.1f}"])

    status = json.loads((source / "summary.json").read_text())
    nets = [Decimal(t["pnl_after_fees_usdc"]) / (Decimal(t["entry_price"]) * Decimal(t["size"])) * 10000
            for t in trades]
    summary = {k: status.get(k) for k in SUMMARY_KEYS}
    summary.update({
        "first_event_utc": utc(first_ms).isoformat(),
        "last_event_utc": utc(last_ms).isoformat(),
        "observed_days": round((last_ms - first_ms) / 86400000, 2),
        "mean_net_bps_per_trade": float(sum(nets) / len(nets)) if nets else None,
        "history_resets": len(resets),
        "blind_pct": round(100 * sum(d["blind_s"] for d in days.values()) / ((last_ms - first_ms) / 1000), 2),
    })
    (target / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(json.dumps({k: summary[k] for k in ("closed_trades", "win_rate", "closed_pnl_after_fees_usdc",
                                              "mean_net_bps_per_trade", "observed_days", "blind_pct")}))


if __name__ == "__main__":
    main()
