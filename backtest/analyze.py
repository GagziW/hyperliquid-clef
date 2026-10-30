"""Event study and parameter backtest of the spike-fade rule on Binance ETHUSDT 1s data.

Either streams the data in-process (--months, nothing written except results.json) or reads a
small extracted directory (--dir). Prices are basis points relative to the signal-second close.
Rise events are traded short; fall events are mirrored and traded long. Standard library only.

Usage:
    python3 -I backtest/analyze.py --months 2024-01..2026-09 --results results.json   # about 7 minutes
    python3 -I backtest/analyze.py --dir /tmp/eth-validate --bot-only --results /dev/null
"""

import argparse
import bisect
import importlib.util
import json
import math
import time
from array import array
from datetime import datetime, timezone
from itertools import accumulate
from operator import neg
from pathlib import Path

MAKER, TAKER, SLIP = 1.5, 4.5, 1.0  # bps, matching paper.toml
COOLDOWN = 30
MAX_GAP = 5
EPS = 1e-6
SPLIT = int(datetime(2025, 7, 1, tzinfo=timezone.utc).timestamp())  # train < SPLIT <= test

THRESHOLDS = [0.3, 0.4, 0.5, 0.7, 1.0]
ENTRIES = ["passive", "taker", "above10", "above20", "confirm10"]
TPS = [10, 15, 20, 30, 50, 80]
SLS = [20, 30, 50, 80, 120]
HOLDS = [60, 120, 300]
BOT = (0.5, "short", "passive", 15, 50, 120, False)


def get_path(paths, pid, n, direction):
    o = pid * 3 * n
    H, L, C = paths[o : o + n], paths[o + n : o + 2 * n], paths[o + 2 * n : o + 3 * n]
    if direction == 1:
        return H, L, C
    return array("f", map(neg, L)), array("f", map(neg, H)), array("f", map(neg, C))  # long -> short


def entry(mode, H, L, C):
    """Return ((fill_second, entry_bps, fee_bps), None) or (None, seconds_busy)."""
    if mode == "passive" or mode.startswith("above"):
        e = 0.0 if mode == "passive" else float(mode[5:])
        window = 5 if mode == "passive" else 30
        if L[1] > e + EPS:
            return None, 1  # post-only would cross: market already trading above our price
        for k in range(1, window + 1):
            if H[k] > e + EPS:  # a trade strictly through our sell price
                return (k, e, MAKER), None
        return None, window
    if mode == "taker":
        return (1, min(C[0], C[1]) - SLIP, TAKER), None
    if mode.startswith("confirm"):
        drop, peak = float(mode[7:]), H[0]
        for k in range(1, 61):
            peak = max(peak, H[k])
            if C[k] <= peak - drop:
                return (k, C[k] - SLIP, TAKER), None
        return None, 60
    raise ValueError(mode)


class Prepared:
    __slots__ = ("ts", "fill", "busy", "C", "neg_min", "cum_max")


def prepare(events, mode):
    out = []
    for ts, H, L, C in events:
        p = Prepared()
        p.ts, p.C = ts, C
        p.fill, p.busy = entry(mode, H, L, C)
        if p.fill:
            f = p.fill[0]
            p.neg_min = array("f", accumulate(map(neg, L[f + 1 : f + 301]), max))  # -running min
            p.cum_max = array("f", accumulate(H[f + 1 : f + 301], max))
        out.append(p)
    return out


def simulate(prepared, tp, sl, hold, maker_tp):
    busy_until, trades = -1, []
    for p in prepared:
        if p.ts < busy_until:
            continue
        if p.fill is None:
            busy_until = p.ts + p.busy
            continue
        f, e, entry_fee = p.fill
        target = e - tp
        j_sl = bisect.bisect_left(p.cum_max, e + sl)
        j_tp = (bisect.bisect_right if maker_tp else bisect.bisect_left)(p.neg_min, -target)
        if j_sl < hold and j_sl <= j_tp:  # same second: assume the stop came first
            k = f + 1 + j_sl
            px, fee, reason = max(p.C[k], e + sl) + SLIP, TAKER, "stop"
        elif j_tp < hold:
            k = f + 1 + j_tp
            px, fee = (target, MAKER) if maker_tp else (p.C[k] + SLIP, TAKER)
            reason = "target"
        else:
            k = f + hold
            px, fee, reason = p.C[k] + SLIP, TAKER, "time"
        trades.append((p.ts, (e - px) - entry_fee - fee, reason))
        busy_until = p.ts + k + COOLDOWN
    return trades


def stats(trades):
    n = len(trades)
    if n == 0:
        return {"n": 0}
    nets = [t[1] for t in trades]
    mean = sum(nets) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in nets) / (n - 1)) if n > 1 else 0.0
    reasons = {}
    for t in trades:
        reasons[t[2]] = reasons.get(t[2], 0) + 1
    return {
        "n": n,
        "win_rate": sum(x > 0 for x in nets) / n,
        "mean_bps": mean,
        "t": mean / (sd / math.sqrt(n)) if sd > 0 else 0.0,
        "total_bps": sum(nets),
        "reasons": reasons,
    }


def by_year(trades):
    years = {}
    for t in trades:
        years.setdefault(datetime.fromtimestamp(t[0], timezone.utc).year, []).append(t)
    return {y: stats(v) for y, v in sorted(years.items())}


def fmt(s):
    if not s or s["n"] == 0:
        return "n=0"
    return f"n={s['n']:5d} win={100 * s['win_rate']:5.1f}% mean={s['mean_bps']:+7.2f}bp t={s['t']:+6.2f}"


def label(c):
    thr, side, mode, tp, sl, hold, mk = c
    return f"{thr}% {side:5} {mode:9} tp={tp:3d} sl={sl:3d} hold={hold:3d} exit={'maker' if mk else 'taker'}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", help="stream YYYY-MM..YYYY-MM in-process")
    parser.add_argument("--dir", type=Path, help="read an extracted directory instead")
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--bot-only", action="store_true", help="only list the current bot's trades")
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    started = time.time()

    if args.months:
        spec = importlib.util.spec_from_file_location("extract", Path(__file__).with_name("extract.py"))
        extract = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(extract)
        plan = extract.chunks(argparse.Namespace(months=args.months, days=None))
        meta, paths = extract.extract(plan, args.workers)
    else:
        meta = json.loads((args.dir / "events.json").read_text())
        paths = array("f")
        with (args.dir / "paths.f32").open("rb") as f:
            paths.frombytes(f.read())
    n = meta["fwd_s"] + 1
    span_days = sum(c["seconds"] for c in meta["chunks"]) / 86400
    good = [e for e in meta["events"] if e[4] <= MAX_GAP]

    def group(thr, dirs):
        return sorted((e[0], *get_path(paths, e[5], n, e[1])) for e in good if e[2] == thr and e[1] in dirs)

    if args.bot_only:
        thr, side, mode, tp, sl, hold, mk = BOT
        trades = simulate(prepare(group(thr, (1,)), mode), tp, sl, hold, mk)
        for ts, net, reason in trades:
            print(f"  {datetime.fromtimestamp(ts, timezone.utc):%m-%d %H:%M:%S}  {reason:6}  {net:+7.2f} bp")
        print(f"  {fmt(stats(trades))}")
        return

    print(f"{len(meta['events'])} events, {len(good)} without data gaps, {span_days:.0f} days, "
          f"{len(meta['randoms'])} random samples ({time.time() - started:.0f}s)\n")

    # ---- 1. Event study: what does price do after a spike? (positive = moves back) ----
    horizons = [5, 10, 30, 60, 120, 300]

    def race(H, L):
        for k in range(1, 121):
            if H[k] >= 50:
                return 0
            if L[k] <= -15:
                return 1
        return 0

    def study_row(items):
        sums, hits, count = [0.0] * len(horizons), 0, 0
        for H, L, C in items:
            for i, h in enumerate(horizons):
                sums[i] -= C[h]
            hits += race(H, L)
            count += 1
        if count == 0:
            return 0, [math.nan] * len(horizons), math.nan
        return count, [s / count for s in sums], hits / count

    print("EVENT STUDY: average move back toward the pre-spike price, in bp (positive = reversal)")
    print(f"{'signal':>18} {'events':>7} {'per day':>8} " + " ".join(f"{h:>6}s" for h in horizons)
          + "  P(-15bp before +50bp in 120s)")
    study = {}
    for d, name in ((1, "rise"), (-1, "fall")):
        for thr in THRESHOLDS:
            items = (get_path(paths, e[5], n, d) for e in good if e[1] == d and e[2] == thr)
            count, means, p_race = study_row(items)
            study[f"{name} {thr}"] = {"n": count, "mean_reversal_bps": means, "p_race": p_race}
            print(f"{name + ' >= ' + str(thr) + '%':>18} {count:7d} {count / span_days:8.2f} "
                  + " ".join(f"{m:+7.2f}" for m in means) + f"  {100 * p_race:5.1f}%")
    count, means, p_race = study_row(get_path(paths, r[2], n, 1) for r in meta["randoms"] if r[1] <= MAX_GAP)
    study["random"] = {"n": count, "mean_reversal_bps": means, "p_race": p_race}
    print(f"{'random times':>18} {count:7d} {'':>8} " + " ".join(f"{m:+7.2f}" for m in means)
          + f"  {100 * p_race:5.1f}%")
    print("Round-trip cost to beat: about 7 bp (maker in, taker out) or 3 bp (maker both ways)\n", flush=True)

    # ---- 2. Grid backtest, one position at a time, 30 s cooldown ----
    results, bot_trades = [], None
    for thr in THRESHOLDS:
        for side in ("short", "both"):
            evs = group(thr, (1,) if side == "short" else (1, -1))
            for mode in ENTRIES:
                prepared = prepare(evs, mode)
                for tp in TPS:
                    for sl in SLS:
                        for hold in HOLDS:
                            for maker_tp in (False, True):
                                cfg = (thr, side, mode, tp, sl, hold, maker_tp)
                                trades = simulate(prepared, tp, sl, hold, maker_tp)
                                if cfg == BOT:
                                    bot_trades = trades
                                results.append({
                                    "cfg": cfg,
                                    "train": stats([t for t in trades if t[0] < SPLIT]),
                                    "test": stats([t for t in trades if t[0] >= SPLIT]),
                                    "all": stats(trades),
                                    "years": by_year(trades),
                                })
                del prepared
            del evs
        print(f"grid: threshold {thr}% done ({time.time() - started:.0f}s)", flush=True)

    bot = next(r for r in results if r["cfg"] == BOT)
    print("\nCURRENT BOT SETTINGS (0.5%, short, passive entry, tp 15, sl 50, 120 s, taker exit)")
    for part in ("train", "test", "all"):
        print(f"  {part:5}: {fmt(bot[part])}  exits={bot[part].get('reasons')}")
    for y, s in bot["years"].items():
        print(f"  {y}: {fmt(s)}")

    eligible = [r for r in results if r["train"].get("n", 0) >= 100]
    pos_train = sum(r["train"]["mean_bps"] > 0 for r in eligible)
    pos_test = sum(r["test"].get("n", 0) > 0 and r["test"]["mean_bps"] > 0 for r in eligible)
    print(f"\n{len(results)} configurations; {len(eligible)} with >= 100 training trades")
    print(f"  profitable after costs: {pos_train} in training (2024-01..2025-06), "
          f"{pos_test} in testing (2025-07..2026-09)")

    top = sorted(eligible, key=lambda r: r["train"]["mean_bps"], reverse=True)[:15]
    print("\nTOP 15 BY TRAINING MEAN, AND HOW THEY DID ON UNSEEN TEST DATA")
    for r in top:
        print(f"  {label(r['cfg'])}\n     train {fmt(r['train'])}\n     test  {fmt(r['test'])}")
    held_up = sum(r["test"].get("n", 0) > 0 and r["test"]["mean_bps"] > 0 for r in top)
    print(f"  {held_up} of the top 15 stayed profitable on the test period")
    if top:
        print("  best training configuration by year:")
        for y, s in top[0]["years"].items():
            print(f"    {y}: {fmt(s)}")

    best_test = sorted(
        (r for r in results if r["test"].get("n", 0) >= 100), key=lambda r: r["test"]["mean_bps"], reverse=True
    )[:5]
    print("\nBEST 5 ON TEST DATA ALONE (hindsight, for reference only)")
    for r in best_test:
        print(f"  {label(r['cfg'])}\n     train {fmt(r['train'])}\n     test  {fmt(r['test'])}")

    by_entry = {}
    for r in eligible:
        m = r["cfg"][2]
        if m not in by_entry or r["train"]["mean_bps"] > by_entry[m]["train"]["mean_bps"]:
            by_entry[m] = r
    print("\nBEST TRAINING CONFIGURATION PER ENTRY STYLE")
    for m, r in by_entry.items():
        print(f"  {label(r['cfg'])}\n     train {fmt(r['train'])}\n     test  {fmt(r['test'])}")

    args.results.write_text(json.dumps({
        "study": study,
        "bot": bot,
        "bot_trades": bot_trades,
        "top_train": top,
        "best_test_hindsight": best_test,
        "best_by_entry": by_entry,
        "n_configs": len(results),
        "n_eligible": len(eligible),
        "profitable_train": pos_train,
        "profitable_test": pos_test,
        "train_test_split_utc": datetime.fromtimestamp(SPLIT, timezone.utc).isoformat(),
        "chunks": meta["chunks"],
    }, indent=1, default=str))
    print(f"\nfinished in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
