import argparse
import asyncio
import contextlib
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import httpx

from pipeline.config import Settings
from pipeline.decision import Decision, Model
from pipeline.market import Feed, Hyperliquid, InfrastructureError
from pipeline.orders import prepare_order, round_price, wire_decimal


def percentiles(values):
    values = sorted(values)
    if not values:
        return {"n": 0, "p50": None, "p95": None, "max": None}

    def quantile(p):
        # Nearest-rank summary; small samples should not imply a reliable p95 estimate.
        return round(values[max(0, math.ceil(p * len(values)) - 1)], 3)

    return {"n": len(values), "p50": quantile(0.5), "p95": quantile(0.95), "max": round(values[-1], 3)}


class Journal:
    def __init__(self, root: Path, name: str):
        self.path = root / (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + name + "-" + uuid4().hex[:8]
        )
        self.path.mkdir(parents=True, exist_ok=False)
        self.file = (self.path / "events.jsonl").open("w", buffering=1)

    def emit(self, event):
        self.file.write(
            json.dumps({"recorded_utc": datetime.now(timezone.utc).isoformat(), **event}, allow_nan=False)
            + "\n"
        )

    def finish(self, summary):
        self.file.close()
        (self.path / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
        print(json.dumps({**summary, "output_directory": str(self.path.resolve())}, indent=2))


async def cancel_task(task):
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def doctor(settings, output):
    journal = Journal(output, "doctor")
    checks = {}
    async with httpx.AsyncClient(timeout=10, limits=httpx.Limits(max_connections=4)) as client:
        hl = Hyperliquid(client, settings)
        asset = None
        context = None
        timings = []
        try:
            for _ in range(3):
                start = time.monotonic()
                asset, context = await hl.eth_meta()
                timings.append((time.monotonic() - start) * 1000)
            checks["hyperliquid_http"] = {
                "status": "pass",
                "cold_rtt_ms": round(timings[0], 3),
                "warm_rtt_ms": percentiles(timings[1:]),
                "eth_asset_id": asset.asset_id,
                "size_decimals": asset.size_decimals,
                "funding_rate_per_hour": context.get("funding"),
            }
        except InfrastructureError as exc:
            checks["hyperliquid_http"] = {"status": "failed", "error": str(exc)}

        feed = Feed(settings, journal.emit)
        feed_task = asyncio.create_task(feed.run())
        book = None
        start = time.monotonic()
        try:
            book = await feed.wait_book()
            checks["hyperliquid_websocket"] = {
                "status": "pass",
                "book_subscription": "fast" if settings.fast_book else "slow",
                "first_book_ms": round((time.monotonic() - start) * 1000, 3),
                "bid": str(book.bid),
                "ask": str(book.ask),
                "spread_bps": book.spread_bps,
                "estimated_exchange_age_ms": round(book.estimated_exchange_age_ms(), 3),
                "clock_skew_caveat": "Exchange age uses wall clocks and includes transit and clock skew.",
            }
            if (
                book.estimated_exchange_age_ms() > settings.max_book_age_ms
                or book.estimated_exchange_age_ms() < -250
            ):
                checks["hyperliquid_websocket"]["status"] = "failed"
                checks["hyperliquid_websocket"]["error"] = "stale_exchange_book_or_clock_skew"
        except InfrastructureError as exc:
            checks["hyperliquid_websocket"] = {"status": "failed", "error": str(exc)}

        for provider in ("clef", "jev"):
            model = Model(client, provider, timeout_s=10)
            if missing := model.missing():
                checks[provider] = {"status": "missing_credentials", "missing": missing}
                continue
            try:
                started = time.monotonic()
                result = await model.decide(
                    {"observed_history_s": 0, "purpose": "credential connectivity check; choose HOLD"}
                )
                checks[provider] = {
                    "status": "pass",
                    "rtt_ms": round((time.monotonic() - started) * 1000, 3),
                    "resolved_model": result.model,
                    "action": result.action,
                    "usage": result.usage,
                }
            except InfrastructureError as exc:
                checks[provider] = {"status": "failed", "error": str(exc)}

        address = os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS", "")
        if not address:
            checks["hyperliquid_account"] = {
                "status": "missing_credentials",
                "missing": ["HYPERLIQUID_ACCOUNT_ADDRESS"],
            }
        elif not re.fullmatch(r"0x[a-fA-F0-9]{40}", address):
            checks["hyperliquid_account"] = {"status": "failed", "error": "Invalid account address format"}
        else:
            try:
                state = await hl.info({"type": "clearinghouseState", "user": address})
                orders = await hl.info({"type": "openOrders", "user": address})
                fees = await hl.info({"type": "userFees", "user": address})
                equity = Decimal(state["marginSummary"]["accountValue"])
                notional = Decimal(state["marginSummary"]["totalNtlPos"])
                eth_positions = [
                    p["position"] for p in state["assetPositions"] if p["position"]["coin"] == "ETH"
                ]
                checks["hyperliquid_account"] = {
                    "status": "pass",
                    "funded": equity > 0,
                    "account_value_usdc": str(equity),
                    "current_gross_position_notional_usdc": str(notional),
                    "open_orders": len(orders),
                    "current_gross_leverage": str(notional / equity) if equity > 0 else None,
                    "eth_leverage_settings": [p.get("leverage") for p in eth_positions],
                    "maker_fee_rate": fees.get("userAddRate"),
                    "taker_fee_rate": fees.get("userCrossRate"),
                }
            except (InfrastructureError, KeyError, TypeError, ArithmeticError) as exc:
                checks["hyperliquid_account"] = {"status": "failed", "error_type": type(exc).__name__}

        key = os.environ.get("HYPERLIQUID_AGENT_PRIVATE_KEY", "")
        if not key:
            checks["hyperliquid_signer"] = {
                "status": "missing_credentials",
                "missing": ["HYPERLIQUID_AGENT_PRIVATE_KEY"],
            }
        elif not asset or not book:
            checks["hyperliquid_signer"] = {"status": "unverified", "reason": "No valid market metadata/book"}
        else:
            try:
                from eth_account import Account
                from hyperliquid.utils.signing import recover_agent_or_user_from_l1_action, sign_l1_action

                wallet = Account.from_key(key)
                px = round_price(book.bid, asset.size_decimals, True)
                action = {
                    "type": "order",
                    "orders": [
                        {
                            "a": asset.asset_id,
                            "b": True,
                            "p": wire_decimal(px),
                            "s": "0.01",
                            "r": False,
                            "t": {"limit": {"tif": "Alo"}},
                        }
                    ],
                    "grouping": "na",
                }
                nonce = int(time.time() * 1000)
                # An expired LOCAL action verifies serialization/signing without producing a usable order.
                expiry = nonce - 10000
                signature = sign_l1_action(wallet, action, None, nonce, expiry, settings.network == "mainnet")
                recovered = recover_agent_or_user_from_l1_action(
                    action, signature, None, nonce, expiry, settings.network == "mainnet"
                )
                if recovered.lower() != wallet.address.lower():
                    raise ValueError("Signature recovery mismatch")
                checks["hyperliquid_signer"] = {
                    "status": "pass",
                    "local_sign_and_recover": True,
                    "account_is_signer": bool(address) and wallet.address.lower() == address.lower(),
                    "agent_approval": "unverified; local signing does not confirm exchange authorization",
                }
            except ImportError:
                checks["hyperliquid_signer"] = {
                    "status": "missing_dependency",
                    "install": "uv sync --extra signing",
                    "reason": "Optional signing SDK is not installed",
                }
            except Exception as exc:
                checks["hyperliquid_signer"] = {"status": "failed", "error_type": type(exc).__name__}
        await cancel_task(feed_task)

    summary = {
        "mode": "dry-run",
        "network": settings.network,
        "checks": checks,
        "public_data_ready": all(
            checks[k]["status"] == "pass" for k in ("hyperliquid_http", "hyperliquid_websocket")
        ),
        "clef_ready": checks["clef"]["status"] == "pass",
        "jev_ready": checks["jev"]["status"] == "pass",
        "trading_enabled": False,
        "agent_approval_verified": False,
        "credentials_source": "process environment; vault contents not inspected",
    }
    journal.emit({"event": "doctor", **summary})
    journal.finish(summary)
    return 0 if summary["public_data_ready"] else 1


async def run(settings, args):
    journal = Journal(args.output, "run")
    latencies = {
        name: []
        for name in (
            "features_ms",
            "model_rtt_ms",
            "order_prepare_ms",
            "pipeline_ms",
            "latest_book_age_ms",
            "initial_book_age_at_decision_ms",
        )
    }
    counts = {"preview": 0, "hold": 0, "blocked": 0, "error": 0}
    fatal = None
    feed = None
    completed = 0
    async with httpx.AsyncClient(timeout=10, limits=httpx.Limits(max_connections=4)) as client:
        model = Model(client, settings.provider, timeout_s=settings.max_decision_age_ms / 1000)
        if missing := model.missing():
            fatal = "Missing environment variables: " + ", ".join(missing)
        else:
            try:
                asset, _ = await Hyperliquid(client, settings).eth_meta()
                feed = Feed(settings, journal.emit)
                feed_task = asyncio.create_task(feed.run())
                try:
                    await feed.wait_book()
                    await asyncio.sleep(args.warmup)
                    next_start = time.monotonic()
                    for index in range(args.samples):
                        await asyncio.sleep(max(0, next_start - time.monotonic()))
                        started = time.monotonic()
                        # No backlog: an inference takes at most the current slot; missed slots are dropped.
                        next_start = started + settings.interval_s
                        model_start = None
                        try:
                            book = await feed.wait_book(timeout_s=5)
                            if (
                                book.estimated_exchange_age_ms() > settings.max_book_age_ms
                                or book.estimated_exchange_age_ms() < -250
                            ):
                                raise InfrastructureError("stale_exchange_book_or_clock_skew")
                            feature_start = time.monotonic()
                            state = feed.state(book)
                            features_ms = (time.monotonic() - feature_start) * 1000
                            model_start = time.monotonic()
                            if settings.provider == "mock" and args.demo_action != "HOLD":
                                probabilities = {
                                    k: float(k == args.demo_action) for k in ("BUY", "SELL", "HOLD")
                                }
                                decision = Decision(
                                    args.demo_action, probabilities, 1.0, "mock-order-demo", {}
                                )
                            else:
                                decision = await model.decide(state)
                            model_ms = (time.monotonic() - model_start) * 1000
                            current = feed.book
                            prepare_start = time.monotonic()
                            order = prepare_order(asset, book, current, decision, settings, model_start)
                            prepare_ms = (time.monotonic() - prepare_start) * 1000
                            times = {
                                "features_ms": features_ms,
                                "model_rtt_ms": model_ms if settings.provider != "mock" else None,
                                "order_prepare_ms": prepare_ms,
                                "pipeline_ms": (time.monotonic() - started) * 1000,
                                "latest_book_age_ms": current.age_ms() if current else None,
                                "initial_book_age_at_decision_ms": book.age_ms(),
                            }
                            for name, value in times.items():
                                if value is not None:
                                    latencies[name].append(value)
                            counts[order["status"]] += 1
                            event = {
                                "event": "decision",
                                "sample": index + 1,
                                "provider": settings.provider,
                                "state": state,
                                "decision": decision.record(),
                                "order": order,
                                "timings": times,
                                "initial_exchange_ms": book.exchange_ms,
                                "latest_exchange_ms": current.exchange_ms if current else None,
                            }
                            journal.emit(event)
                            print(
                                json.dumps(
                                    {
                                        "sample": index + 1,
                                        "action": decision.action,
                                        "status": order["status"],
                                        "model_rtt_ms": round(model_ms, 2)
                                        if settings.provider != "mock"
                                        else None,
                                        "reasons": order["reasons"],
                                    }
                                )
                            )
                        except InfrastructureError as exc:
                            counts["error"] += 1
                            event = {"event": "decision_error", "sample": index + 1, "error": str(exc)}
                            if model_start is not None:
                                event["failed_model_ms"] = (time.monotonic() - model_start) * 1000
                            journal.emit(event)
                            print(json.dumps(event))
                            if "HTTP 429" in str(exc) or "HTTP 529" in str(exc):
                                next_start = time.monotonic() + max(settings.interval_s, 30)
                            elif "HTTP 401" in str(exc) or "HTTP 403" in str(exc):
                                fatal = str(exc)
                                break
                        completed += 1
                finally:
                    await cancel_task(feed_task)
            except InfrastructureError as exc:
                fatal = str(exc)
    summary = {
        "mode": "dry-run",
        "network": settings.network,
        "provider": settings.provider,
        "book_subscription": "fast" if settings.fast_book else "slow",
        "samples_completed": completed,
        "counts": counts,
        "fatal_error": fatal,
        "latencies_ms": {k: percentiles(v) for k, v in latencies.items()},
        "book_delivery_intervals_ms": percentiles(feed.book_intervals_ms if feed else []),
        "websocket_messages": feed.message_count if feed else 0,
        "exchange_submission_rtt_ms": None,
        "orders_submitted": 0,
        "notice": "No fill simulation or profitability estimate. Model timing is unavailable with mock.",
        "target_leverage": 1,
        "exchange_leverage_setting_modified": False,
    }
    journal.finish(summary)
    return 1 if fatal or counts["error"] else 0


def main():
    parser = argparse.ArgumentParser(
        description="ETH perpetual dry-run infrastructure probe; never submits orders"
    )
    parser.add_argument("command", choices=["doctor", "run"])
    parser.add_argument("--network", choices=["mainnet", "testnet"], default="mainnet")
    parser.add_argument("--provider", choices=["mock", "clef", "clef-flash", "jev"], default="mock")
    parser.add_argument(
        "--slow-book", action="store_true", help="Use the slower 20-level feed for comparison"
    )
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--decision-timeout-ms", type=float, default=2000)
    parser.add_argument("--book-age-ms", type=float, default=1500)
    parser.add_argument("--max-mid-move-bps", type=float, default=2)
    parser.add_argument("--order-usdc", type=Decimal, default=Decimal("20"))
    parser.add_argument("--demo-action", choices=["BUY", "SELL", "HOLD"], default="HOLD")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parent.parent / "runs")
    args = parser.parse_args()
    if not 1 <= args.samples <= 1000 or not math.isfinite(args.warmup) or args.warmup < 0:
        parser.error("samples must be 1..1000 and warmup must be finite and nonnegative")
    if args.provider != "mock" and args.demo_action != "HOLD":
        parser.error("demo-action is available only with provider mock")
    try:
        settings = Settings(
            network=args.network,
            provider=args.provider,
            fast_book=not args.slow_book,
            interval_s=args.interval,
            max_decision_age_ms=args.decision_timeout_ms,
            max_book_age_ms=args.book_age_ms,
            max_mid_move_bps=args.max_mid_move_bps,
            order_usdc=args.order_usdc,
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        code = asyncio.run(doctor(settings, args.output) if args.command == "doctor" else run(settings, args))
    except KeyboardInterrupt:
        code = 130
    raise SystemExit(code)
