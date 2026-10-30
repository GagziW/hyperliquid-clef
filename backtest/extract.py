"""Stream Binance ETHUSDT 1-second klines into memory and extract spike events.

Raw market data is never written to disk: each zip is downloaded into memory, parsed,
and discarded. Only a compact file of event price paths (relative, float32) is saved.
Standard library only.

Usage (small sets to disk; the 33-month run goes through analyze.py --months instead):
    python3 -I backtest/extract.py --days 2026-10-10..2026-10-13 --out /tmp/eth-validate
"""

import argparse
import io
import json
import math
import sys
import time
import urllib.request
import zipfile
from array import array
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = "https://data.binance.vision/data/spot"
THRESHOLDS = [0.3, 0.4, 0.5, 0.7, 1.0]  # percent rise/fall over LOOKBACK seconds
LOOKBACK = 60
FWD = 360  # seconds of path stored from the signal second onward (entry <= 60 s + hold <= 300 s)
TAIL = 500  # seconds carried over from the previous chunk (>= FWD + LOOKBACK + 1)
RANDOM_STRIDE = 4 * 3600  # one random baseline sample per 4 hours
MAX_GAP_S = 5  # events whose window contains a longer run of missing seconds are flagged


def fetch(url, tries=6):
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=180) as response:
                return response.read()
        except Exception as exc:  # noqa: BLE001
            if attempt == tries - 1:
                raise RuntimeError(f"{url}: {exc}") from exc
            time.sleep(3 + 5 * attempt)


def chunks(args):
    out = []
    if args.months:
        first, last = args.months.split("..")
        y, m = map(int, first.split("-"))
        ly, lm = map(int, last.split("-"))
        while (y, m) <= (ly, lm):
            start = datetime(y, m, 1, tzinfo=timezone.utc)
            ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
            end = datetime(ny, nm, 1, tzinfo=timezone.utc)
            label = f"{y:04d}-{m:02d}"
            out.append((label, f"{BASE}/monthly/klines/ETHUSDT/1s/ETHUSDT-1s-{label}.zip", start, end))
            y, m = ny, nm
    if args.days:
        first, last = args.days.split("..")
        day = datetime.fromisoformat(first).replace(tzinfo=timezone.utc)
        stop = datetime.fromisoformat(last).replace(tzinfo=timezone.utc)
        while day <= stop:
            label = day.strftime("%Y-%m-%d")
            url = f"{BASE}/daily/klines/ETHUSDT/1s/ETHUSDT-1s-{label}.zip"
            out.append((label, url, day, day + timedelta(days=1)))
            day += timedelta(days=1)
    return out


def parse(blob, start_s, n):
    nan = math.nan
    hi, lo, cl = array("d", [nan]) * n, array("d", [nan]) * n, array("d", [nan]) * n
    real = bytearray(n)
    rows = 0
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        with archive.open(archive.namelist()[0]) as raw:
            for line in io.TextIOWrapper(raw, encoding="ascii"):
                p = line.split(",", 5)
                if not p[0][:1].isdigit():
                    continue  # header row, if any
                ts = int(p[0])
                if ts > 10**14:
                    ts //= 1000  # spot files from 2025 use microseconds
                i = ts // 1000 - start_s
                if 0 <= i < n:
                    hi[i], lo[i], cl[i] = float(p[2]), float(p[3]), float(p[4])
                    real[i] = 1
                    rows += 1
    return hi, lo, cl, real, rows


def extract(plan, workers=4):
    """Stream every chunk in plan through memory; return (meta, paths) without touching disk."""
    paths = array("f")  # per path: FWD+1 highs, lows, closes in bps relative to the signal close
    path_of = {}
    events, randoms, stats = [], [], []
    tail = None
    min_thr = min(THRESHOLDS)
    started = time.time()

    def store_path(abs_s, i, H, L, C, real):
        if abs_s in path_of:
            return path_of[abs_s]
        c = C[i]
        for series in (H, L, C):
            paths.extend((series[i + k] / c - 1.0) * 1e4 for k in range(FWD + 1))
        run = longest = 0
        for k in range(i - LOOKBACK - 1, i + FWD + 1):
            run = 0 if real[k] else run + 1
            longest = max(longest, run)
        path_of[abs_s] = (len(path_of), longest)
        return path_of[abs_s]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {}
        for idx, (label, url, start, end) in enumerate(plan):
            for j in range(idx, min(idx + workers + 2, len(plan))):
                if j not in pending:
                    pending[j] = pool.submit(fetch, plan[j][1])
            blob = pending.pop(idx).result()
            start_s, n = int(start.timestamp()), int((end - start).total_seconds())
            hi, lo, cl, real, rows = parse(blob, start_s, n)
            del blob

            if tail and tail[0] == start_s:
                H, L, C, R = tail[1] + hi, tail[2] + lo, tail[3] + cl, tail[4] + real
                base = start_s - len(tail[1])
                first = len(tail[1]) - FWD - 1
            else:
                H, L, C, R, base, first = hi, lo, cl, real, start_s, LOOKBACK + 1
            last_close = None
            for i in range(len(C)):
                if C[i] != C[i]:
                    if last_close is not None:
                        H[i] = L[i] = C[i] = last_close
                else:
                    last_close = C[i]
            end_i = len(C) - FWD - 1

            n_events = 0
            jp = None
            for i in range(max(first, LOOKBACK + 1), end_i):
                c, ref = C[i], C[i - LOOKBACK]
                if c != c or ref != ref:
                    jp = None
                    continue
                j = (c / ref - 1.0) * 100.0
                if jp is None:
                    pc, pref = C[i - 1], C[i - 1 - LOOKBACK]
                    jp = (pc / pref - 1.0) * 100.0 if pc == pc and pref == pref else j
                if -min_thr < j < min_thr:
                    jp = j
                    continue
                for thr in THRESHOLDS:
                    if j >= thr > jp:
                        pid, gap = store_path(base + i, i, H, L, C, R)
                        events.append([base + i, 1, thr, round(j, 4), gap, pid])
                        n_events += 1
                    elif j <= -thr < jp:
                        pid, gap = store_path(base + i, i, H, L, C, R)
                        events.append([base + i, -1, thr, round(j, 4), gap, pid])
                        n_events += 1
                jp = j

            k0, k1 = (base + first) // RANDOM_STRIDE, (base + end_i) // RANDOM_STRIDE + 1
            for k in range(k0, k1):
                s = k * RANDOM_STRIDE + (k * 2654435761) % RANDOM_STRIDE
                i = s - base
                if max(first, LOOKBACK + 1) <= i < end_i and C[i] == C[i]:
                    pid, gap = store_path(s, i, H, L, C, R)
                    randoms.append([s, gap, pid])

            missing = n - rows
            stats.append({"chunk": label, "rows": rows, "seconds": n, "missing_seconds": missing})
            print(
                f"{label}: rows={rows} missing={missing} events={n_events} "
                f"paths={len(path_of)} elapsed={time.time() - started:.0f}s",
                flush=True,
            )
            tail = (int(end.timestamp()), H[-TAIL:], L[-TAIL:], C[-TAIL:], R[-TAIL:])

    meta = {
        "source": "Binance spot ETHUSDT 1s klines (data.binance.vision)",
        "thresholds_pct": THRESHOLDS,
        "lookback_s": LOOKBACK,
        "fwd_s": FWD,
        "path_len": 3 * (FWD + 1),
        "events": events,
        "randoms": randoms,
        "chunks": stats,
    }
    print(f"done: {len(events)} events, {len(randoms)} random samples, {len(path_of)} paths", flush=True)
    return meta, paths


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", help="YYYY-MM..YYYY-MM")
    parser.add_argument("--days", help="YYYY-MM-DD..YYYY-MM-DD")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    meta, paths = extract(chunks(args), args.workers)
    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / "paths.f32").open("wb") as f:
        paths.tofile(f)
    (args.out / "events.json").write_text(json.dumps(meta))


if __name__ == "__main__":
    sys.exit(main())
