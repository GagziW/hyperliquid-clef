# ETH Perpetual Infrastructure Probe

This project contains a **rules-only paper trader** and the original dry-run
infrastructure probe. Neither submits orders. The probe tests connectivity,
credentials, market-data capture, typed model decisions, order formatting and
latency. The paper trader below simulates a short-side spike-fade strategy.
There is no exchange-submission implementation or live-trading switch, and no
demonstrated profitable strategy.

## Research Findings (30 Oct 2026)

Full write-up, data and proposed next steps: [RESEARCH.md](RESEARCH.md).

- The current short-only spike-fade rule loses money: 12 live paper trades in 20 days at −6.6 bp
  per trade, and a 33-month backtest on Binance one-second data at −8.0 bp per trade over 5,368
  trades. None of 9,000 parameter variants was profitable in or out of sample.
- After sharp one-minute rises ETH tends to continue rather than revert; after sharp falls it bounces.
- Buying after falls of 1.5% or more (hold 5 minutes) was positive in two unseen periods
  (+34 bp per trade over 165 trades, 2023 to Sep 2026), but the sample is small and the worst trade lost 10.4%.

Backtest code is in `backtest/`; exported paper data is in `data/paper-trading/`.

## Rules-Only Paper Trader

The paper trader does not call Clef or any other model, inspect private wallets,
or require API keys. It uses public mainnet ETH perpetual books and trades.

```sh
uv sync --locked --no-dev
uv run --no-dev python -m pipeline.paper --config paper.toml
```

Edit `paper.toml` to configure the strategy. Percent values are in percentage
points: `0.50` means 0.50%, not 50%. Initial values are unoptimized test settings:

| Parameter | Initial Value |
| --- | --- |
| Upward jump over the lookback window | 0.50% |
| Lookback window | 60 seconds |
| Take-profit decline from filled entry | 0.15% |
| Stop-loss rise from filled entry | 0.50% |
| Maximum holding time before requesting exit | 120 seconds |
| Cooldown after closing | 30 seconds |
| Starting simulated cash | 1,000 USDC |
| Requested simulated short | 20 USDC; hard cap 25 |
| Simulated order latency | 250 milliseconds |
| Unfilled entry lifetime | 5 seconds |
| Maker / taker fees | 1.5 / 4.5 basis points |
| Extra exit slippage | 1 basis point |

The four principal settings also have command-line overrides:

```sh
uv run --no-dev python -m pipeline.paper \
  --jump-threshold-pct 0.8 --lookback-s 60 \
  --take-profit-pct 0.2 --stop-loss-pct 0.6 --max-holding-s 90 \
  --duration 600
```

Without `--duration`, it runs until stopped. A short is considered only when the
rolling mid-price return first crosses the jump threshold, after a complete
lookback has been observed. The strategy does not repeatedly short while the same
signal remains above threshold. There is one position at a time, no averaging
down, and an entry equity cap with a paper risk exit if gross exposure exceeds 1x.

Entry is a fixed post-only sell limit at the observed best ask. A touched price
is **not** counted as a fill. The first book timestamped after simulated latency
must still permit a passive sell; an already-crossing limit is rejected. Until
that confirmation there are no fills, so intervening possible fills are missed
rather than invented. The simulator then requires a subsequent buyer-initiated
trade strictly above that limit, after confirmation and before expiry,
with enough reported volume for the complete paper order. This conservative
trade-through approximation does not reconstruct queue priority, partial fills
or the market impact of inserting our order. It is not a live-fill guarantee.

Exits are simulated buys against available ask depth on the next fresh book after
the configured latency, with extra slippage and taker fees. Profit and stop triggers
use executable ask depth rather than mid price. Stop and time-limit exits can be
late or worse than their trigger prices. No exit is fabricated using stale data
or insufficient displayed depth. Disconnects cancel unfilled paper entries,
reset signal history, and request an exit for an existing short; filling that
exit still requires fresh data.

### Results

`runs/paper/status.json` is refreshed at least every five seconds while healthy.
It reports the actual hostname, configuration, paper balance, closed P&L after
fees, estimated equity, observed drawdown, pending orders and any open short.
Separate parameter sets get separate experiment subdirectories; changing settings
starts a new simulated account rather than mixing results.

Inside each experiment directory:

| File | Contents |
| --- | --- |
| `trades.csv` | Closed paper trades, prices, fees, holding time and exit reason |
| `summary.json` | Latest account and strategy summary |
| `state.json` | Atomic account checkpoint, including the closed-trade ledger |
| `events-YYYY-MM-DD.jsonl` | Daily UTC signal, fill, exit, connection and status events |

The checkpoint is authoritative. CSV is regenerated, so restarts do not append
duplicate trades. Restarting the same configuration restores cash and positions,
cancels an unfilled entry, rewarms history, and requests a risk exit for a short
whose price path was not observed during downtime. Only one process can own an
output directory. On a normal stop, open shorts are reported and checkpointed,
not silently closed at an invented price.

**Funding is not modeled.** P&L after fees includes simulated slippage but is not
all-in net trading profit. Trades crossing hourly funding boundaries are flagged.
Observed drawdown can miss movements during data gaps. The event log is an audit
of the paper strategy, not a complete raw-market-data archive or historical
backtest. These results must not be presented as achievable live performance.

### Mac Mini Operation

The configured destination is
`/Users/leonwitt/codeMacMini/prediction_markets/hyperliquid-clef` on SSH host
`macmini-gagzi`. From this local project:

```sh
sh scripts/macmini-paper.sh status
sh scripts/macmini-paper.sh fetch
sh scripts/macmini-paper.sh stop
sh scripts/macmini-paper.sh start
```

`fetch` copies results into local `runs/macmini-paper/`. To change parameters,
stop the service, update the Mac mini's `paper.toml`, and start again. Prior
experiments remain intact. A new parameter set has its own paper cash balance.

On the Mac mini itself, use `sh scripts/paper-service.sh start|stop|status|uninstall`.
The service is a user LaunchAgent named `com.prediction-markets.eth-paper`.
It runs in the background, retries after failure, and loads at the next user
login. This is **not** a system daemon that starts before login after reboot.
`stop` unloads it for the current session; `uninstall` also removes the startup
registration while preserving all results. No global power settings are changed.
Service output is in `runs/service/stdout.log` and `stderr.log`.

### Paper Deployment Check: 2026-10-10

The rules-only service was installed and started on `Leons-Mac-mini.local`
(Apple silicon, Python 3.12). Public ETH books and trades connected successfully,
and the full 60-second signal window warmed up. A stop/start check restored the
paper account and preserved its logs. The final deployment reported no errors,
no model calls, no exchange submissions and no triggered trades during the
initial check; this establishes connectivity, not profitability.

The full test suite passed 77 tests on both the development machine and Mac mini;
the optional SDK signing test was skipped. Paper tests include threshold
crossings, delayed passive-entry validation, trade-through fills, profit/stop/time
exits, shallow depth, feed failures, checkpoint recovery and duplicate-process
protection. Lint and shell syntax checks also passed.

The original probe below is separate from this rules-only paper process.

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
