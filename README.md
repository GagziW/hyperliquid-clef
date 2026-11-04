# Hyperliquid Paper-Trading Lab

Write your own trading strategy, run it for 24 hours on **live** Hyperliquid ETH market data with
**paper money**, and compare it with the example strategy.

- No account, no wallet and no API key are necessary. The program reads only public market data.
- The program **never sends orders**. All trades are simulated.
- Performance is secondary. The goal is to understand how the program connects to live data, which
  parameters it uses, and how a paper trade works under the hood.

Course: AI in Trading, SIMIS. Contact: Dr. Leon Witt, leon.witt@simis.cn

## 1. Install (Windows, macOS, Linux)

You need about 10 minutes and an internet connection. `uv` installs the correct Python version
for you, so you do not need to install Python yourself.

**Windows** (open *PowerShell*):

```powershell
winget install --id Git.Git -e
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Close PowerShell and open it again, so that it finds `git` and `uv`. If `winget` is not available,
install Git from <https://git-scm.com/download/win>.

**macOS / Linux** (open *Terminal*; macOS asks to install Git the first time that you use it):

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Then, on all systems:**

```sh
git clone https://github.com/GagziW/hyperliquid-paper-trading-lab.git
cd hyperliquid-paper-trading-lab
uv sync --locked
uv run pytest -q
```

If the tests pass (one optional signing test is skipped), the installation is correct. All
commands below are the same on Windows, macOS and Linux.

## 2. Run the example strategy

```sh
uv run python -m pipeline.paper --duration 600
```

This runs all strategies in `run = [...]` in `paper.toml` for 10 minutes. In a second terminal:

```sh
uv run python -m pipeline.paper --status
uv run python -m pipeline.compare
```

The program prints one line at the start of each strategy and one line for each fill and exit.
The example signal is rare: in 10 minutes, possibly no trade occurs. Stop the program with
`Ctrl+C`; it saves the paper account before it stops. While it runs, it keeps the computer awake
(Windows, macOS, Linux). Closing a laptop lid can still put it to sleep.

## 3. What is fixed and what is yours

`paper.toml` has three parts:

| Part | Who changes it | Contents |
| --- | --- | --- |
| `run = [...]` | **you** | The strategies that run. Keep `spike_fade` (the benchmark) and add yours. |
| `[simulation]` | **nobody: FIXED** | Fees, slippage, latency, order size, starting balance, data-quality limits. |
| `[strategies.<name>]` | **you** | The parameters of each strategy. |

The fixed simulation settings are the same for every student:

| Setting | Value | Meaning |
| --- | --- | --- |
| `network` | mainnet | Public live data, no account |
| `starting_balance_usdc` | 1,000 | Paper money |
| `order_usdc` | 20 | Size of each paper order |
| `maker_fee_bps` / `taker_fee_bps` | 1.5 / 4.5 | Hyperliquid base-tier fees |
| `slippage_bps` | 1 | Extra cost on each market order and exit |
| `latency_ms` | 250 | Delay from your decision to the exchange |
| `entry_timeout_s` | 5 | An unfilled post-only entry is cancelled after 5 s |
| `max_book_age_ms` | 1,500 | Older order books are rejected |
| `gap_reset_s` | 5 | A longer pause in the data is a data gap |

If you change a `[simulation]` value, the program prints a warning, `status.json` shows
`"simulation_standard": false`, and `pipeline.compare` marks the result **NOT COMPARABLE**.

## 4. What we compare

Your strategy and the example strategy run **in the same process, in the same 24 hours, on the same
market events, with the same fixed simulation**. For this reason, the only difference is the
strategy. `uv run python -m pipeline.compare` shows them side by side:

| Value | Question |
| --- | --- |
| order books observed, data gaps | Did both strategies see the same data? |
| entries requested / rejected / expired | How often did your signal fire, and how often did the order not fill? |
| closed trades, win rate | How often did you trade, and how often did you win? |
| P&L after fees, mean net per trade (bp) | Did the strategy pay for its costs? |
| max drawdown | What was the worst fall of the paper account? |
| model calls | How many paid AI calls did the strategy make? |

24 hours is a small sample. A day can show a bug, a cost problem or a wrong hypothesis; it cannot
prove a strategy. Funding payments are not modelled.

## 5. Write your own strategy

The engine (`pipeline/paper_engine.py`) does the work that must be the same for everybody: fills,
fees, latency, exits, P&L and the account. A strategy only decides **when** to trade and **in which
direction**. It can be as simple or as complex as you want: rules, statistics, machine learning or
an AI model.

1. Open `pipeline/strategies/my_strategy.py`. It is a commented template; it explains all the data
   that you get and everything that you can return.
2. Write your hypothesis at the top of the file.
3. Put your parameters into the `Params` dataclass and into `[strategies.my_strategy]` in
   `paper.toml`.
4. Write your rule in `on_book` (and `on_trade` if you use the trade stream). Return
   `Entry(side="long" or "short", ...)` to open a position, `Exit()` to close it, or `None`.
5. Write tests in `tests/` (copy the style of `tests/test_paper.py`), then run `uv run pytest -q`.

The example strategy `pipeline/strategies/spike_fade.py` is a complete strategy in about 60 lines.
Read it first. To use another file name, copy the template to `pipeline/strategies/<name>.py` and
put `<name>` into `run = [...]`.

| You can choose | Values |
| --- | --- |
| Direction | `side="long"` or `side="short"` |
| Entry type | `style="maker"`: post-only limit at the best bid/ask (1.5 bp, may not fill) <br> `style="taker"`: market order (fills, 4.5 bp + 1 bp slippage) |
| Exits | `take_profit_pct`, `stop_loss_pct` (or `None`), `max_holding_s` (required, at most 24 h), or return `Exit()` |
| Anything else | Your own state: price history, volatility, trade flow, time of day, model answers ... |

The engine allows one position at a time and ignores an `Entry` while a position or an entry
order is open.

### Use an AI model

`pipeline/strategies/ai_template.py` shows the pattern. The model runs in the slow `background`
task (for example each 30 s); `on_book` only reads the latest answer. If the answer is old or
missing, the strategy does not trade. The template uses the `mock` model by default, so it runs
without a key. Put keys only into environment variables, never into a file in this repository.

## 6. Run for 24 hours and compare

```sh
uv run python -m pipeline.paper --duration 86400
```

Results are in `runs/paper/<strategy>/`:

| File | Contents |
| --- | --- |
| `status.json` | Latest status, refreshed at least every 5 seconds |
| `<config-hash>/summary.json` | Account and strategy summary |
| `<config-hash>/trades.csv` | One line for each closed trade (side, prices, fees, net bp, exit reason, your `note`) |
| `<config-hash>/state.json` | Checkpoint of the paper account; a restart continues from it |
| `<config-hash>/events-YYYY-MM-DD.jsonl` | Each signal, order, fill, exit and data gap |

A new set of parameters gets a new `<config-hash>` directory and a new paper account, so results
never mix. Then:

```sh
uv run python -m pipeline.compare
```

## 7. Deliverables

1. **Code**: your strategy file, your `paper.toml` and your tests.
2. **Run data**: the `runs/paper/` folders of your strategy and of `spike_fade` from the same run.
3. **Visualization**: one page that shows how your strategy works: the data that it uses, the
   signal, the entry, the exits and the parameters.
4. **Explanation**: explain your strategy in front of the class, including how the program gets the
   live data, which parameters are fixed and which are yours, and how a paper trade fills.

## 8. How the paper trader works

```text
Hyperliquid WebSocket (public): l2Book (5 levels, fast) + trades
  -> pipeline/market.py      Feed: parse, validate, reconnect with backoff
  -> pipeline/paper.py       one queue, freshness checks, data gaps, logs, all strategies
  -> pipeline/strategies/    YOUR decision: Entry / Exit / None
  -> pipeline/paper_engine.py FIXED simulation: orders, fills, exits, fees, P&L, checkpoint
```

**Maker entry (post-only).** A short rests at the best ask, a long at the best bid. A touched price
is **not** a fill. The first book after the simulated latency must still allow a passive order; an
order that would cross is rejected. The order fills only on a later trade strictly through the
limit (a buyer-initiated trade above a short's limit, a seller-initiated trade below a long's
limit), before expiry, with enough volume for the full paper order. This conservative
approximation does not reconstruct queue priority, partial fills or market impact.

**Taker entry (market order).** After the latency, the order fills against the displayed depth of
the first fresh book, plus slippage and the taker fee. If the depth is too small, it is rejected.

**Exits.** The engine closes a position at the take-profit, the stop-loss, the time limit, an
exposure above 1x equity, a data gap, or an `Exit()` from the strategy. Exits are market orders
on the next fresh book after the latency, against the displayed depth, plus slippage and the taker
fee. Profit and stop triggers use the executable price (ask depth for a short, bid depth for a
long), not the mid price. A stop can fill worse than its trigger price.

**Data quality.** Books older than 1.5 s are rejected. Disconnects, stale books and pauses longer
than 5 s are data gaps: the engine cancels an unfilled entry, requests an exit for an open
position, and calls `reset()` on the strategy so that it can clear its history. No price is
invented for the time without data.

**Restarts.** The checkpoint `state.json` is authoritative. Restarting the same configuration
restores cash and positions, cancels an unfilled entry and requests a risk exit for a position
whose price path was not observed. Only one process can own an output directory.

**Funding is not modelled.** P&L after fees includes simulated slippage but is not all-in net
trading profit. Trades crossing hourly funding boundaries are counted. These results must not be
presented as achievable live performance.

## 9. The example strategy and research findings

`spike_fade` sells short after the mid price rises 0.50% or more within 60 s, with a 0.15%
take-profit, a 0.50% stop-loss and a 120 s time limit. It is an **example**, not a recommendation:

- 12 live paper trades in 20 days lost 6.6 bp per trade; a 33-month backtest on Binance one-second
  data lost 8.0 bp per trade over 5,368 trades. None of 9,000 parameter variants was profitable.
- After sharp one-minute rises ETH tends to continue rather than revert; after sharp falls it bounces.

Full write-up, data and ideas for other strategies: [RESEARCH.md](RESEARCH.md). Backtest code is in
`backtest/`; exported paper data of the example is in `data/paper-trading/`.

### Instructor: macOS background service

`scripts/paper-service.sh start|stop|status|uninstall` installs the paper trader as a user
LaunchAgent (`com.prediction-markets.eth-paper`) that restarts after failure and loads at login.
`scripts/macmini-paper.sh` runs the same commands on a remote Mac over SSH and `fetch` copies the
results into `runs/macmini-paper/`. Service output is in `runs/service/`.

## 10. Verification

```sh
uv run pytest -q
uv run ruff check pipeline tests
uv run ruff format --check pipeline tests
```

---

# Part 2: The AI decision probe (dry run)

The original infrastructure probe is separate from the paper trader. It sends live market features
to a model (Clef, Clef Flash, Jev or a mock), checks the answer, and builds an **unsigned** order
preview that is never submitted. `ai_template.py` reuses its `Model` class.

```text
Hyperliquid ETH book + trades over WebSocket
  -> rolling features
  -> Clef / Clef Flash / Jev / mock
  -> deterministic freshness, price and size checks
  -> unsigned post-only order PREVIEW + local event log
```

The live feed continues updating while inference runs. Only one inference runs at
a time; there is no queue of old model requests. A decision is checked against the
latest book, and reconnects invalidate decisions made from the previous connection.
The book subscription explicitly requests the fast five-level feed. Use
`--slow-book` to compare it with the slower 20-level subscription.

## First Run

From this directory, with Python 3.11+ and `uv` installed:

```sh
uv sync --locked --no-dev
uv run --no-dev python -m pipeline doctor
uv run --no-dev python -m pipeline run --provider mock --samples 10 --warmup 10
```

The default mock always chooses HOLD. To exercise actual order-preview formatting
against real market data, without calling a model or placing an order:

```sh
uv run --no-dev python -m pipeline run --provider mock --demo-action BUY --samples 10
uv run --no-dev python -m pipeline run --provider mock --demo-action SELL --samples 10
```

`--network testnet` selects Hyperliquid testnet. The default is read-only mainnet
market data, which is more representative than testnet data for latency experiments.

Every run creates a unique directory inside `runs/` containing `events.jsonl` and
`summary.json`. The events include received market messages, feature snapshots,
model outputs, candidate order payloads and rejection reasons. They do not include
authorization headers, private keys or signatures. These logs are local experiments,
not a complete archival feed: there can be gaps during disconnects and restarts.

## Credentials

Inject secrets into the process through the 1Password developer environment.
The application deliberately does not load `.env` files. `.env.example` documents
variable names, not a place to store credentials. Never paste wallet secrets into
chat or commit them to this project.

| Capability | Process variables |
| --- | --- |
| Public Hyperliquid feed | None |
| Clef / Clef Flash | `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_AUTH_TOKEN` |
| Jev | `TYPESAFE_API_KEY` |
| Read-only account/fee inspection | `HYPERLIQUID_ACCOUNT_ADDRESS` |
| Optional local signing diagnostic | `HYPERLIQUID_AGENT_PRIVATE_KEY` |

`CLOUDFLARE_API_TOKEN` is accepted as an alternative token variable. Use a
Cloudflare API token authorized for Workers AI on the specified account.
Use a dedicated Hyperliquid API-agent wallet rather than exposing a funded
master-wallet key. Its approval must be separately verified before any future
execution implementation is enabled.

The initial install needs only HTTP and WebSocket packages. For the optional local
signing check, install `uv sync --locked --no-dev --extra signing` first; subsequent
commands must also include `--extra signing` to retain that extra.

`doctor` checks public HTTP and WebSocket connectivity, probes both model services
when credentials exist, and optionally inspects the account. With an injected
signer, it signs an **already-expired action locally** and verifies recovery using
the official SDK. Nothing is transmitted to the exchange. Local signing does not
prove that the account has approved that agent, is funded, or can place an order.

The diagnostic exits successfully if public data works; consult each individual
check for model/account/signing readiness. Missing secrets remain explicitly
unverified. In the initial development session the 1Password MCP server was not
available, so vault-backed credentials could not be inspected or injected.

After credentials are available:

```sh
uv run --no-dev python -m pipeline doctor
uv run --no-dev python -m pipeline run --provider clef --samples 20 --warmup 35
uv run --no-dev python -m pipeline run --provider clef-flash --samples 20 --warmup 35
uv run --no-dev python -m pipeline run --provider jev --samples 20 --warmup 35
```

Missing credentials and model errors never fall back silently to mock decisions.
Jev defaults to `jev-latest`; set `TYPESAFE_MODEL` to an explicitly versioned model
for reproducible comparisons. The resolved model and reported usage are recorded.
Hosted model calls can incur charges.

## Experiment Defaults

These are engineering-test settings, not optimized strategy parameters:

| Setting | Default |
| --- | --- |
| Instrument | ETH perpetual, USDC collateral |
| Decision interval | 5 seconds |
| Candidate direction horizon | 60 seconds |
| Requested order notional | 20 USDC; hard cap 25 USDC per preview |
| Maximum locally observed/exchange-estimated book age | 1,500 ms |
| Maximum decision/input age | 2,000 ms |
| Maximum mid-price change during inference | 2 basis points |
| Maximum spread | 5 basis points |
| Minimum selected-option probability | 0.80 |
| Order style | Post-only at the current best bid/ask, rounded conservatively |

Features are spread, five-level depth imbalance, observed taker-volume imbalance
over 60 seconds, and observed 5/30-second mid-price changes. Missing history stays
unknown rather than being represented as zero. This tiny feature set is a plumbing
test, not evidence of predictive power. The 0.80 threshold is not an 80% chance of
profit: it filters a model's preference among the supplied choices.

Prices and sizes follow exchange metadata and
[Hyperliquid's precision rules](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size).
The generated action uses
[ALO/post-only](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint),
but acceptance and subsequent fills are not tested by a preview. A SELL candidate
could open a short; this prototype does not manage positions or exits.

## No Leverage

The intended future limit is **gross exposure at most account equity (1x)**,
including the worst-case effect of outstanding orders. That is different from
merely setting the exchange's leverage field to 1. A future executor must check
fresh account state and enforce the exposure cap before each submission, including
other positions and repeated fills. This prototype changes no leverage settings
and does not claim to enforce account-level exposure because it submits no orders.
Perpetuals still involve funding, collateral and liquidation risk at 1x.

## What Latency Numbers Mean

The summary separates feature computation, model HTTP round trip, order preparation,
total local pipeline time, local book age and WebSocket book-delivery intervals.
`doctor` separates the first metadata request from two warmed requests. Mock mode
reports model latency as unavailable, not as zero. Submission acknowledgements and
fills are also unavailable because nothing is submitted.

Exchange-age estimates compare wall clocks and include both transit time and clock
skew; they are not measured one-way network latency. Keep the host clock synchronized.
WebSocket delivery interval is update cadence, not exchange execution latency.
Small-sample p95 values are descriptive, not reliable service-level estimates.

Run real model benchmarks from the intended deployment location, at different times,
before selecting Clef, Clef Flash or Jev. The 2-second decision deadline rejects
late results; raising it increases permitted staleness as well as completion rate.
Rate-limit/capacity errors back off without replaying old states. Authentication
errors stop the run.

As an engineering starting hypothesis, use a hosted model for slower decisions and
keep pricing, risk and cancellation deterministic. Do not assume a home connection
plus remote inference can win a speed race against specialized market makers.
Moving the process to a VPS should follow measured latency improvements, not guesswork.

## Profitability Is a Separate Test

At the published base perpetual fee tier, maker fees are 1.5 basis points per fill
and taker fees 4.5 basis points. A maker/maker round trip therefore costs about
3 basis points before funding, inference costs and adverse selection; account
discounts can differ. See the
[fee schedule](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees).

A limit order touching the best bid/ask does not imply a fill. Later evaluation
must account for queue position, partial fills, cancellations, missed fills and
the fact that passive fills can precede an unfavorable price move. Compare the
model to simple non-AI baselines on held-out data using net returns, not just
directional accuracy. The downloaded Polymarket dataset is not Hyperliquid order
book history and is not used in this prototype.

Before real-money trading, build a funded-testnet order/cancel round trip, explicit
agent-authorization checks, account/position reconciliation, exposure limits,
order-expiry handling and a kill switch. Then validate fills and net strategy
performance. None of those live-execution capabilities are implied by this probe.

## Verification and Sources

```sh
uv run --no-dev --group test pytest -q
uv run --no-dev --group lint ruff check pipeline tests
uv run --no-dev --group lint ruff format --check pipeline tests
```

Tests cover malformed model decisions, authentication-error redaction, price/size
rounding, staleness, moved books, reconnects, trade snapshot deduplication and model
request/response contracts, plus the rules-only paper scenarios above.
The SDK signing test is skipped unless the optional
signing extra is installed; include `--extra signing` in the test command to run it.

Model integrations follow the primary documentation:
[Cloudflare Clef](https://developers.cloudflare.com/workers-ai/models/clef/) and
[TypeSafe System One API](https://docs.typesafe.ai/api).
Market subscriptions follow the
[Hyperliquid WebSocket API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions).

## Initial Smoke Check: 2026-10-10

Measured from the development machine, not the intended production deployment:

| Check | Observed result |
| --- | --- |
| Public HTTP metadata | Pass; first request 3,133 ms, two warmed requests 1,074 / 1,723 ms |
| Fast WebSocket first book | Pass; 2,423 ms including connection/subscription setup |
| Fast book delivery interval | Median 534 ms, p95 960 ms; only 22 intervals |
| Mock BUY pipeline | 20 previews, zero errors, zero submitted orders |
| Local feature + preview pipeline | Median 0.147 ms, p95 0.236 ms; no real inference or submission |
| Clef / Jev / account / signer | Credentials unavailable in the process; vault access unavailable |
| Missing-Clef-credentials run | Failed explicitly before opening the feed, without a mock fallback |
| Tests | 42 passed; optional SDK signing test skipped |
| Lint and formatting | Passed using locally cached Ruff 0.14.14 |

The earlier slow-book run observed roughly 5.3-second median delivery intervals,
which prompted the explicit fast subscription. These short runs demonstrate
connectivity and formatting, not sustained reliability, achievable trading
latency or profitability. Real inference and exchange acknowledgement timings
remain unmeasured.

The corresponding records are in
`runs/20261010T055116Z-doctor-614b4b39/` and
`runs/20261010T055125Z-run-4677d950/`.
