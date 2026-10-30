# Research Notes: Short-Horizon ETH Strategies on Hyperliquid

Status: 30 October 2026. All results are simulated (paper trading and backtests). No real orders
have been sent, funding is not modelled, and nothing here demonstrates achievable live performance.

## 1. Summary

| Question | Answer so far | Evidence |
| --- | --- | --- |
| Does the current rule (short after a 0.5% rise in 60 s) make money? | **No** | Live paper: 12 trades, −6.6 bp/trade. Backtest: 5,368 trades over 33 months, −8.0 bp/trade, negative every year (t ≈ −22) |
| Can its parameters be tuned into profit? | **No** | 9,000 variants: none profitable in training (Jan 2024–Jun 2025) or testing (Jul 2025–Sep 2026) |
| Do sharp one-minute rises reverse? | **No; they drift further up** | Event study on 13,713 rises ≥ 0.5% |
| Do sharp one-minute falls reverse? | **Yes, increasingly with fall size** | +41.6 bp average bounce within 5 min after ≥ 1.5% falls (152 sell-offs, s.e. 9.5 bp) |
| Is "buy the crash" a viable strategy? | **Promising, not proven** | +34 bp/trade over 165 trades (2023–Sep 2026), positive in two unseen periods; small sample, selected from 7,875 variants, worst trade −10.4% |

## 2. Repository Contents

| Path | Contents |
| --- | --- |
| `pipeline/` | Live market-data probe, model-decision hooks, order previews (never submitted) and the rules-only paper trader |
| `tests/` | Unit tests for the probe and paper trader |
| `scripts/paper-service.sh`, `scripts/macmini-paper.sh` | Run the paper trader as a macOS background service |
| `scripts/paper_summary.py` | Exports the compact paper-trading data below from the raw logs |
| `data/paper-trading/` | Live paper results: `trades.csv`, `daily.csv`, `summary.json` |
| `backtest/` | Historical backtests on Binance one-second data (standard library only; data streamed, never stored) |
| `backtest/results/` | Full text output of the three backtests quoted in this document |

## 3. Data

### 3.1 Live (Hyperliquid mainnet, public feed)

The paper trader has run continuously since 10 October 2026 on a Mac mini, using the public
five-level ETH order book and trade stream. Raw event logs (520 MB) are not committed;
`data/paper-trading/` holds the closed trades, a per-day summary and the account summary.

| Metric (10–30 Oct 2026, 20.2 days) | Value |
| --- | --- |
| Order-book updates processed | 3,209,324 |
| Entry signals | 46 |
| Rejected (price already through the order) / expired unfilled | 17 / 16 |
| Trades closed | 12 (6 take-profit, 4 time limit, 2 data-gap exits) |
| Win rate | 58.3% |
| Net result | −$0.158 on $1,000 (−6.6 bp per $20 trade) |
| Time unable to signal (history resets) | 7.5%; worst day 24 Oct, 37% |

### 3.2 Historical (Binance spot ETHUSDT, one-second klines)

Hyperliquid's API serves only about 3.5 days of candles, so the backtests use the free Binance
archive at `data.binance.vision`: 1,004 days from January 2024 to September 2026 (86.7 million
seconds, none missing), plus 2023 for a second out-of-sample check. Each monthly file is streamed
through memory and discarded.

**Validation against the live Hyperliquid data (10–13 Oct 2026):**

- All 7 live entry signals appear in Binance data within 4 seconds.
- One-minute returns correlate at 0.978 over 4,709 overlapping minutes, with equal volatility (0.058%).
- Replaying the bot's rules on Binance data reproduced both live trades: −17.5 vs −17.7 bp and +8.5 vs +8.7 bp.
- Binance fills are somewhat more generous: the replay filled 3 signals the live bot did not.

## 4. Strategy 1: Spike Fade (the current bot)

**Rule.** When the mid price rises ≥ 0.50% over 60 s, post a sell limit at the best ask (maker,
1.5 bp), cancel after 5 s. Exit at a 0.15% fall (target), a 0.50% rise (stop) or after 120 s, as a
taker (4.5 bp + 1 bp slippage). One position at a time, 30 s cooldown, $20 per trade.

**Payoff arithmetic.** With round-trip cost $c \approx 7$ bp, a target of $G = 15$ bp and a stop of
$L = 50$ bp, a win nets $G - c = 8$ bp and a loss costs $L + c = 57$ bp. Ignoring time exits, the
break-even win rate is

$$p^* = \frac{L + c}{(G - c) + (L + c)} = \frac{57}{65} \approx 88\%.$$

**Backtest (Binance, Jan 2024–Sep 2026).**

| Period | Trades | Win rate | Mean net per trade | t |
| --- | --- | --- | --- | --- |
| Training, Jan 2024–Jun 2025 | 3,173 | 64.3% | −7.98 bp | −17.0 |
| Testing, Jul 2025–Sep 2026 | 2,195 | 62.0% | −8.05 bp | −13.8 |
| 2024 / 2025 / 2026 | 1,537 / 2,574 / 1,257 | 62.8 / 64.5 / 61.9% | −8.26 / −7.51 / −8.73 bp | |
| Live paper, Oct 2026 | 12 | 58.3% | −6.6 bp | |

The live result matches the backtest. Over 9,000 variants (thresholds 0.3–1.0%, five entry styles,
targets 10–80 bp, stops 20–120 bp, holds 60–300 s, maker or taker exits, short-only or both
directions), **none was profitable in either period**. The best training variant (0.7% trigger,
sell 0.2% above the market, 10 bp target, 120 bp stop, maker exit) won 87% of its trades and still
lost 3.7 bp per trade, then 5.0 bp in testing.

**Event study: what follows a rise?** Average move back toward the pre-spike price, in bp
(positive = reversal). Every threshold crossing is counted, so neighbouring events overlap.

| Rise in 60 s | Events | 5 s | 30 s | 60 s | 120 s | 300 s |
| --- | --- | --- | --- | --- | --- | --- |
| ≥ 0.3% | 58,702 | −0.30 | −0.54 | −0.43 | −0.64 | −0.66 |
| ≥ 0.5% | 13,713 | −0.15 | −0.34 | −0.36 | −0.80 | −1.99 |
| ≥ 0.7% | 4,873 | −0.26 | −0.84 | −1.70 | −3.39 | −7.94 |
| ≥ 1.0% | 1,541 | +0.33 | −1.16 | +1.65 | −6.51 | −16.30 |
| Random times | 6,024 | −0.04 | −0.12 | −0.21 | −0.16 | −0.26 |

**Why it fails.** (1) Rises continue rather than revert, so the premise is wrong for this period.
(2) The payoff needs an ~88% win rate. (3) Passive entries suffer adverse selection: the order fills
only when a buyer pays through it, which is when the rise is continuing.

## 5. Strategy 2: Crash Bounce (long after sharp falls)

**Event study, each sell-off counted once** (signals less than 10 minutes apart are merged).

| Fall in 60 s | Sell-offs | 10 s | 30 s | 60 s | 120 s | 300 s | s.e. at 300 s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ≥ 0.5% | 3,069 | +0.75 | +1.46 | +1.72 | +1.99 | +2.78 | 0.99 |
| ≥ 0.7% | 1,193 | +2.04 | +3.01 | +3.78 | +3.01 | +5.59 | 2.01 |
| ≥ 1.0% | 429 | +3.20 | +8.13 | +7.09 | +5.82 | +10.42 | 4.89 |
| ≥ 1.5% | 152 | +5.41 | +14.44 | +14.73 | +23.87 | +41.60 | 9.45 |
| ≥ 2.0% | 72 | +9.06 | +26.62 | +37.37 | +66.03 | +79.90 | 16.46 |

Rises and falls behave asymmetrically: small moves of either sign are noise, large falls bounce,
large rises continue.

**Selected rule** (best of 7,875 long-only variants on training data only): after a ≥ 1.5% fall in
60 s, rest a buy limit 0.20% below that price for 30 s; if filled, hold 5 minutes and sell at market.
No target, no stop. Costs as above.

| Period | Used for selection? | Trades | Win rate | Mean net per trade | t |
| --- | --- | --- | --- | --- | --- |
| 2023 | No | 29 | 72.4% | +21.2 bp | 0.90 |
| Jan 2024–Jun 2025 | Yes | 100 | 68.0% | +31.3 bp | 1.87 |
| Jul 2025–Sep 2026 | No | 36 | 77.8% | +52.5 bp | 2.45 |
| All, 2023–Sep 2026 | | 165 | 70.9% | +34.1 bp | 2.88 |

- Every half-year from 2024-H1 to 2026-H2 was positive; without its 5 best days it still averaged +26.2 bp.
- The same rules applied at random times lost 3.4 bp per trade.
- Downside: worst trade −10.4% (5 Aug 2024), then −5.0%, −4.2%, −3.9%, −3.7%; 16 of 165 trades lost ≥ 1%; worst cumulative drawdown 1,075 bp; longest losing streak 3.
- Size: at $20 per trade, +$11.26 over 165 trades (about 44 trades per year).

**Stability.** Test-period mean (bp) for neighbouring targets and stops, same trigger, entry and
hold. Tight exits lose; the effect needs room to play out.

| Stop \ Target | 10 | 20 | 50 | 80 | 120 | none |
| --- | --- | --- | --- | --- | --- | --- |
| 20 | −5.6 | −4.6 | −0.8 | +7.3 | +7.9 | −1.3 |
| 50 | −4.0 | −3.8 | −0.9 | +9.8 | +13.3 | −2.3 |
| 80 | −2.2 | −1.8 | +6.2 | +18.0 | +21.9 | +10.9 |
| 120 | +1.8 | +1.1 | +12.8 | +28.8 | +38.9 | +6.5 |
| 200 | −2.0 | −2.0 | +17.6 | +38.7 | +58.3 | +30.0 |
| none | −2.8 | −3.5 | +17.1 | +33.2 | +53.3 | +52.5 |

**Caveats.** Only 65 trades fall in unseen periods; the rule is the best of thousands; trades
cluster on a few crash days; crash-time spreads and slippage are likely worse than the 1 bp assumed;
Hyperliquid may behave differently from Binance in exactly these moments (for example liquidation
cascades).

## 6. Methodology and Limitations

- **Fill model.** Passive entries fill only on a later trade strictly through the limit (a touch is not a fill), after 250 ms latency, with post-only rejection; exits pay the taker fee plus 1 bp. Historical fills use one-second high/low/close bars; a stop and target in the same second count as the stop.
- **Venue proxy.** Binance spot, not Hyperliquid perpetuals; validated above, but fills on Binance are more generous.
- **Costs.** Hyperliquid base-tier fees; no funding; no fee discounts.
- **Dependence.** Overlapping events inflate event-study precision; the strategy simulations enforce one position at a time, but trades still cluster on volatile days.
- **Selection.** 16,875 configurations were evaluated across both studies. Reported out-of-sample results guard against, but do not remove, selection bias.

## 7. Improving the Mathematical Analysis

### 7.1 Expected value with all exit types
Generalise the break-even formula to the observed exit mix. With exit-type probabilities $q_i$ and
mean net outcomes $m_i$ (target, stop, time, data gap):

$$E[\text{net}] = \sum_i q_i\, m_i, \qquad \text{profit requires } E[\text{net}] > 0.$$

Estimating $q_i$ and $m_i$ separately shows which lever matters (for the spike fade, time exits
drift negative because rises continue).

### 7.2 Statistical power and sample size
To detect a mean edge $\mu$ with per-trade standard deviation $\sigma$ at significance $\alpha$ and
power $1-\beta$:

$$n \approx \left(\frac{(z_{1-\alpha/2} + z_{1-\beta})\,\sigma}{\mu}\right)^2.$$

For the crash bounce ($\mu \approx 34$ bp, $\sigma \approx 152$ bp, 5% level, 80% power):
$n \approx (2.80 \times 152/34)^2 \approx 157$ trades, about 3.6 years at 44 trades per year. Live
confirmation on ETH alone is slow; more instruments are needed.

### 7.3 Multiple testing and backtest overfitting
- **Deflated Sharpe ratio** (Bailey & López de Prado, 2014): adjust the best Sharpe ratio for the number of trials and non-normal returns.
- **Probability of backtest overfitting** via combinatorially symmetric cross-validation (Bailey et al., 2017).
- **White's Reality Check** (White, 2000) and **Hansen's SPA test** (Hansen, 2005): test whether the best of many rules beats a benchmark after data snooping.
- Treat t > 3 as the minimum bar for a discovery from a large search (Harvey, Liu & Zhu, 2016).
- Pre-register the rule, sample size and pass/fail criteria before live testing.

### 7.4 Dependence and clustering
- Compute standard errors clustered by day or by sell-off.
- Use the stationary block bootstrap (Politis & Romano, 1994) for confidence intervals on mean P&L and drawdown.
- Report day-level as well as trade-level statistics.

### 7.5 Validation design
Replace the single train/test split with walk-forward analysis (rolling re-selection, then
evaluation on the next window) and purged, embargoed cross-validation (López de Prado, 2018) so
overlapping events cannot leak between folds. Keep one final period untouched until the end.

### 7.6 Volatility-normalised signals and regimes
Fixed percentage thresholds mix calm and turbulent periods. Define the signal as a standardised
move $z_t = r_{t-60,t} / \hat\sigma_t$, with $\hat\sigma_t$ from an EWMA of recent one-second
returns, and condition results on volatility quantiles or a regime model.

### 7.7 Event-study regressions
Model the forward return directly:

$$r_{t \to t+h} = \alpha + \beta\,|\text{move}_t| + \gamma^\top X_t + \varepsilon_t,$$

with $X_t$ including BTC's simultaneous move (market-wide vs ETH-specific), traded volume, taker
buy/sell imbalance, order-book depth, Hyperliquid liquidation volume, funding rate and time of day.
Quantile regression shows how the tails, not just the mean, depend on these variables.

### 7.8 Execution and fill modelling
- Measure **markouts** (price change 1, 5 and 30 s after each fill) to quantify adverse selection (Glosten & Milgrom, 1985).
- Model fill probability as a function of distance from the touch, volatility and queue position.
- Calibrate slippage as a function of volatility, especially in crashes, using small real trades.

### 7.9 Risk and position sizing
- Report Expected Shortfall (CVaR) of per-trade P&L and Monte Carlo drawdown distributions from bootstrapped trade sequences.
- The Kelly fraction $f^* \approx \mu/\sigma^2$ gives about 15× leverage for the crash bounce, which illustrates why plug-in Kelly fails with estimation error and fat tails (a −10% five-minute trade). Use a small fraction of Kelly at most, on top of the existing 1× gross-exposure cap.

### 7.10 Costs and carry
Add Hyperliquid's hourly funding, fee tiers and realistic latency to every simulation, so results
are all-in net returns.

## 8. More Strategies to Test

| # | Strategy | Hypothesis | Latency sensitivity | First test |
| --- | --- | --- | --- | --- |
| 1 | Crash bounce, refined | Forced selling (liquidations) overshoots and reverts | Low–medium | Condition on liquidation volume, BTC co-move and fall size; scale entries |
| 2 | Breakout momentum | Large rises continue (Section 4 event study) | Medium | Episode-counted event study of ≥ 0.7–1.0% rises; long with time exits |
| 3 | Intraday time-series momentum | Trends persist over hours (Moskowitz, Ooi & Pedersen, 2012) | Low | Hourly signals; costs are small relative to moves |
| 4 | ETH–BTC pairs / statistical arbitrage | The residual of ETH on BTC mean-reverts (Gatev et al., 2006; Avellaneda & Lee, 2010) | Low–medium | Rolling hedge ratio, z-score entries, half-life estimate |
| 5 | Funding carry and basis | High funding predicts mean reversion; spot–perp basis can be harvested | Low | Hyperliquid funding history vs subsequent returns |
| 6 | Order-flow imbalance filter | Order-flow imbalance predicts short-term price changes (Cont, Kukanov & Stoikov, 2014) | High | Use as a filter on strategies 1–2 rather than standalone |
| 7 | Cross-venue lead–lag | Binance moves first; Hyperliquid follows within seconds | Very high | Cross-correlation at sub-second lags; likely competed away |
| 8 | Market making with inventory control | Earn the spread while managing inventory (Avellaneda & Stoikov, 2008) | Very high | Only with queue modelling; base-tier maker fee (1.5 bp) leaves little room |
| 9 | Model-based decisions (Clef/Jev hooks in `pipeline/`) | A model adds information beyond simple rules | Medium | Only after a rules baseline beats costs; same out-of-sample protocol |

## 9. Roadmap

1. Add a long-side crash-bounce mode to the paper trader and run it alongside the current rule, mainly to compare Hyperliquid crash-time fills with the backtest.
2. Re-run both backtests with day-clustered standard errors, block-bootstrap intervals, the deflated Sharpe ratio and the probability of backtest overfitting.
3. Extend to more instruments (BTC, SOL and others) and to 2022–2023 to increase the number of independent crash events.
4. Add funding and fee tiers to all simulations.
5. Fix the history reset in `pipeline/paper.py`: during short feed pauses, stop trading but keep the 60 s history.
6. Consider small real trades only after pre-registered criteria are met in both backtest and paper trading.

## 10. Reproducing the Results

```sh
python3 -I backtest/analyze.py --months 2024-01..2026-09 --results results.json    # spike fade, about 7 min
python3 -I backtest/dip.py --months 2024-01..2026-09 --results dip_results.json    # crash bounce grid
python3 -I backtest/dip_check.py --months 2023-01..2026-09                         # 2023 check and downside
python3 -I scripts/paper_summary.py runs/paper/<experiment-id> data/paper-trading  # refresh live data
```

## References

- Avellaneda, M., & Lee, J.-H. (2010). Statistical arbitrage in the US equities market. *Quantitative Finance*, 10(7), 761–782.
- Avellaneda, M., & Stoikov, S. (2008). High-frequency trading in a limit order book. *Quantitative Finance*, 8(3), 217–224.
- Bailey, D. H., Borwein, J., López de Prado, M., & Zhu, Q. J. (2017). The probability of backtest overfitting. *Journal of Computational Finance*, 20(4), 39–69.
- Bailey, D. H., & López de Prado, M. (2014). The deflated Sharpe ratio: Correcting for selection bias, backtest overfitting, and non-normality. *Journal of Portfolio Management*, 40(5), 94–107.
- Cont, R., Kukanov, A., & Stoikov, S. (2014). The price impact of order book events. *Journal of Financial Econometrics*, 12(1), 47–88.
- Gatev, E., Goetzmann, W. N., & Rouwenhorst, K. G. (2006). Pairs trading: Performance of a relative-value arbitrage rule. *Review of Financial Studies*, 19(3), 797–827.
- Glosten, L. R., & Milgrom, P. R. (1985). Bid, ask and transaction prices in a specialist market with heterogeneously informed traders. *Journal of Financial Economics*, 14(1), 71–100.
- Hansen, P. R. (2005). A test for superior predictive ability. *Journal of Business & Economic Statistics*, 23(4), 365–380.
- Harvey, C. R., Liu, Y., & Zhu, H. (2016). … and the cross-section of expected returns. *Review of Financial Studies*, 29(1), 5–68.
- López de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley.
- Moskowitz, T. J., Ooi, Y. H., & Pedersen, L. H. (2012). Time series momentum. *Journal of Financial Economics*, 104(2), 228–250.
- Politis, D. N., & Romano, J. P. (1994). The stationary bootstrap. *Journal of the American Statistical Association*, 89(428), 1303–1313.
- White, H. (2000). A reality check for data snooping. *Econometrica*, 68(5), 1097–1126.
