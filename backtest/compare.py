"""Compare Binance spot 1s data with Hyperliquid's live bot signals and 1m candles (in memory only).

Usage: python3 -I backtest/compare.py /tmp/eth-validate   (a directory made by extract.py --days)
The bot signal times below are the paper trader's entry_requested events for 10-13 Oct 2026.
"""
import io, json, sys, urllib.request, zipfile
from pathlib import Path
from datetime import datetime, timezone
f = lambda s: datetime.fromtimestamp(s, timezone.utc).strftime('%m-%d %H:%M:%S')

# 1) Binance 0.50% up-crossings vs the bot's live entry signals
ev = json.load(open(Path(sys.argv[1]) / 'events.json'))['events']
bin_up = sorted(e[0] for e in ev if e[1] == 1 and e[2] == 0.5)
bot = [1791733220, 1791850205, 1791862361, 1791862368, 1791890567, 1791890578, 1791890602]  # entry_requested, s
print('Binance 0.50% rise signals (UTC):', ', '.join(f(s) for s in bin_up))
for b in bot:
    near = min(bin_up, key=lambda s: abs(s - b)) if bin_up else None
    print(f'  bot signal {f(b)} -> nearest Binance signal {f(near)} ({near - b:+d} s)')

# 2) 1-minute returns: Binance spot vs Hyperliquid perp over the overlap
closes = {}
for day in ('2026-10-10', '2026-10-11', '2026-10-12', '2026-10-13'):
    url = f'https://data.binance.vision/data/spot/daily/klines/ETHUSDT/1s/ETHUSDT-1s-{day}.zip'
    with zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(url, timeout=120).read())) as z:
        for line in io.TextIOWrapper(z.open(z.namelist()[0]), encoding='ascii'):
            p = line.split(',', 5)
            ts = int(p[0]); ts = ts // 1000 if ts > 10**14 else ts
            closes[ts // 60000] = float(p[4])  # last 1s close in each minute = minute close
req = json.dumps({"type": "candleSnapshot", "req": {"coin": "ETH", "interval": "1m", "startTime": 0, "endTime": 1792000000000}}).encode()
hl = json.load(urllib.request.urlopen(urllib.request.Request('https://api.hyperliquid.xyz/info', req, {'Content-Type': 'application/json'}), timeout=60))
hlc = {c['t'] // 60000: float(c['c']) for c in hl}
mins = sorted(set(closes) & set(hlc))
pairs = [((closes[m] / closes[m - 1] - 1) * 100, (hlc[m] / hlc[m - 1] - 1) * 100) for m in mins if m - 1 in closes and m - 1 in hlc]
n = len(pairs); mx = sum(a for a, _ in pairs) / n; my = sum(b for _, b in pairs) / n
cov = sum((a - mx) * (b - my) for a, b in pairs); vx = sum((a - mx) ** 2 for a, _ in pairs); vy = sum((b - my) ** 2 for _, b in pairs)
print(f'\n1-minute returns over {n} overlapping minutes: correlation {cov / (vx * vy) ** 0.5:.3f}')
print(f'  std dev Binance {100*(vx/n)**0.5/100:.4f}%  Hyperliquid {(vy/n)**0.5:.4f}%')
basis = [(hlc[m] / closes[m] - 1) * 1e4 for m in mins]
print(f'  price gap Hyperliquid vs Binance: median {sorted(basis)[n//2]:+.1f} bp, range {min(basis):+.1f} to {max(basis):+.1f} bp')
big = [(m, a, b) for (m, (a, b)) in zip(mins[1:], pairs) if abs(a) >= 0.3 or abs(b) >= 0.3]
print(f'  minutes with a move of 0.3% or more on either venue: {len(big)}')
for m, a, b in big[:15]:
    print(f'    {f(m*60)}  Binance {a:+.2f}%  Hyperliquid {b:+.2f}%')
