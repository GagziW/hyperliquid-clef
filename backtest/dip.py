"""Buy-the-dip test: go long after sharp one-minute ETH falls, chosen on training data, judged on unseen data.

Usage:  python3 -I backtest/dip.py --months 2024-01..2026-09 --results dip_results.json
Streams Binance ETHUSDT 1s data through memory (extract.py) and reuses the trade simulator in
analyze.py with fall events mirrored into long trades. Only the results file is written.
"""

import argparse
import importlib.util
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extract, bt = load("extract"), load("analyze")

THRESHOLDS = [0.5, 0.7, 1.0, 1.5, 2.0]  # percent fall over 60 s
TPS = [10, 15, 20, 30, 50, 80, 120, None]  # None = no profit target
SLS = [20, 30, 50, 80, 120, 200, None]  # None = no stop
NONE = 1e9
EPISODE_GAP = 600  # signals closer than this belong to one sell-off
HORIZONS = [10, 30, 60, 120, 300]


def name(c):
    thr, mode, tp, sl, hold, mk = c
    return (f"fall {thr}% {mode:9} target={'none' if tp is None else tp:>4} stop={'none' if sl is None else sl:>4} "
            f"hold={hold:3d} exit={'maker' if mk else 'taker'}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", required=True)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    started = time.time()

    extract.THRESHOLDS = THRESHOLDS
    meta, paths = extract.extract(extract.chunks(argparse.Namespace(months=args.months, days=None)), args.workers)
    n = meta["fwd_s"] + 1
    falls = sorted(e for e in meta["events"] if e[1] == -1 and e[4] <= bt.MAX_GAP)

    # ---- 1. Event study, counting each sell-off once ----
    print("\nAVERAGE BOUNCE AFTER A FALL, bp (positive = price moved back up). Each sell-off counted once.")
    print(f"{'signal':>14} {'sell-offs':>9} " + " ".join(f"{h:>6}s" for h in HORIZONS) + "   std err at 300 s")
    study = {}
    for thr in THRESHOLDS:
        last, episodes = -math.inf, []
        for e in (e for e in falls if e[2] == thr):
            if e[0] - last > EPISODE_GAP:
                episodes.append(e)
            last = e[0]
        if len(episodes) < 2:
            continue
        rows = [bt.get_path(paths, e[5], n, -1)[2] for e in episodes]
        means = [sum(-C[h] for C in rows) / len(rows) for h in HORIZONS]
        x = [-C[300] for C in rows]
        se = math.sqrt(sum((v - means[-1]) ** 2 for v in x) / (len(x) - 1) / len(x))
        study[thr] = {"episodes": len(episodes), "mean_bounce_bps": means, "se_300s": se}
        print(f"{'fall >= ' + str(thr) + '%':>14} {len(episodes):9d} " + " ".join(f"{m:+7.2f}" for m in means)
              + f"   {se:6.2f}")
    print("Round-trip cost: about 7 bp (maker in, taker out) or 3 bp (maker both ways)", flush=True)

    # ---- 2. Grid: long after falls, one position at a time ----
    results = []
    for thr in THRESHOLDS:
        evs = [(e[0], *bt.get_path(paths, e[5], n, -1)) for e in falls if e[2] == thr]
        for mode in bt.ENTRIES:
            prepared = bt.prepare(evs, mode)
            for tp in TPS:
                for sl in SLS:
                    for hold in bt.HOLDS:
                        for mk in (False, True):
                            if tp is None and mk:
                                continue
                            trades = bt.simulate(prepared, NONE if tp is None else tp, NONE if sl is None else sl, hold, mk)
                            results.append({
                                "cfg": (thr, mode, tp, sl, hold, mk),
                                "train": bt.stats([t for t in trades if t[0] < bt.SPLIT]),
                                "test": bt.stats([t for t in trades if t[0] >= bt.SPLIT]),
                            })
        print(f"grid: fall {thr}% done ({time.time() - started:.0f}s)", flush=True)

    eligible = [r for r in results if r["train"].get("n", 0) >= 100]
    pos_train = sum(r["train"]["mean_bps"] > 0 for r in eligible)
    pos_test = sum(r["test"].get("n", 0) > 0 and r["test"]["mean_bps"] > 0 for r in eligible)
    print(f"\n{len(results)} long-after-fall configurations; {len(eligible)} with >= 100 training trades")
    print(f"  profitable after costs: {pos_train} in training (2024-01..2025-06), {pos_test} in testing (2025-07..2026-09)")

    top = sorted(eligible, key=lambda r: r["train"]["mean_bps"], reverse=True)[:10]
    print("\nTOP 10 BY TRAINING MEAN, AND HOW THEY DID ON UNSEEN TEST DATA")
    for r in top:
        print(f"  {name(r['cfg'])}\n     train {bt.fmt(r['train'])}\n     test  {bt.fmt(r['test'])}")

    pick = top[0]["cfg"]
    thr, mode, tp, sl, hold, mk = pick
    evs = [(e[0], *bt.get_path(paths, e[5], n, -1)) for e in falls if e[2] == thr]
    trades = bt.simulate(bt.prepare(evs, mode), NONE if tp is None else tp, NONE if sl is None else sl, hold, mk)
    print(f"\nPICKED ON TRAINING DATA ONLY: {name(pick)}")
    print(f"  train: {bt.fmt(top[0]['train'])}\n  test:  {bt.fmt(top[0]['test'])}  exits={top[0]['test'].get('reasons')}")
    halves = {}
    for t in trades:
        d = datetime.fromtimestamp(t[0], timezone.utc)
        halves.setdefault(f"{d.year}-H{1 if d.month <= 6 else 2}", []).append(t)
    for k, v in sorted(halves.items()):
        print(f"  {k}: {bt.fmt(bt.stats(v))}")
    days = {}
    for t in trades:
        day = datetime.fromtimestamp(t[0], timezone.utc).date().isoformat()
        days[day] = days.get(day, 0.0) + t[1]
    best = sorted(days.items(), key=lambda kv: kv[1], reverse=True)[:5]
    total = sum(days.values())
    rest = [t for t in trades if datetime.fromtimestamp(t[0], timezone.utc).date().isoformat() not in dict(best)]
    print(f"  traded on {len(days)} days; total {total:+.0f} bp; best 5 days {sum(v for _, v in best):+.0f} bp "
          f"({', '.join(d for d, _ in best)})")
    print(f"  without those 5 days: {bt.fmt(bt.stats(rest))}")
    rnd = sorted((r[0], *bt.get_path(paths, r[2], n, -1)) for r in meta["randoms"] if r[1] <= bt.MAX_GAP)
    rnd_trades = bt.simulate(bt.prepare(rnd, mode), NONE if tp is None else tp, NONE if sl is None else sl, hold, mk)
    print(f"  same rules, buying at random times instead: {bt.fmt(bt.stats(rnd_trades))}")

    print("\n  stability: test-period mean (bp) for nearby targets x stops, same threshold/entry/hold/exit")
    grid = {(r["cfg"][2], r["cfg"][3]): r for r in results
            if r["cfg"][0] == thr and r["cfg"][1] == mode and r["cfg"][4] == hold and r["cfg"][5] == mk}
    print("   stop\\target " + " ".join(f"{('none' if t is None else t):>7}" for t in TPS))
    for s in SLS:
        cells = []
        for t in TPS:
            r = grid.get((t, s))
            cells.append(f"{r['test']['mean_bps']:+7.2f}" if r and r["test"].get("n", 0) else f"{'-':>7}")
        print(f"   {('none' if s is None else s):>11} " + " ".join(cells))

    args.results.write_text(json.dumps({
        "episode_study": study,
        "n_configs": len(results),
        "n_eligible": len(eligible),
        "profitable_train": pos_train,
        "profitable_test": pos_test,
        "top_train": top,
        "pick": pick,
        "pick_by_half_year": {k: bt.stats(v) for k, v in halves.items()},
        "pick_without_best_5_days": bt.stats(rest),
        "pick_random_entry_baseline": bt.stats(rnd_trades),
    }, indent=1, default=str))
    print(f"\nfinished in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
