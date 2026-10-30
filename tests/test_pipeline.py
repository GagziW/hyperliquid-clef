import asyncio
import copy
import json
import time
from dataclasses import replace
from decimal import Decimal

import httpx
import pytest

from pipeline.config import Settings
from pipeline.decision import Decision, Model, parse_decision
from pipeline.market import Asset, Book, Feed, Hyperliquid, InfrastructureError
from pipeline.orders import prepare_order, round_price


def book(bid="2495.7", ask="2495.8"):
    return Book.parse(
        {
            "coin": "ETH",
            "time": int(time.time() * 1000),
            "levels": [
                [{"px": bid, "sz": "1"}],
                [{"px": ask, "sz": "2"}],
            ],
        }
    )


def decision(action="BUY", probability=1.0):
    return Decision(
        action,
        {
            "BUY": probability if action == "BUY" else 0,
            "SELL": probability if action == "SELL" else 0,
            "HOLD": 1 - probability if action != "HOLD" else 1,
        },
        0.9,
        "test",
        {},
    )


def response():
    return {
        "model": "clef",
        "answers": {
            "action": {
                "type": "choice",
                "choice": "BUY",
                "confidence": 0.9,
                "probabilities": {"BUY": 0.9, "SELL": 0.05, "HOLD": 0.05},
            }
        },
        "usage": {"input_tokens": 100},
    }


@pytest.mark.parametrize("action", ["BUY", "SELL"])
def test_preview_is_post_only_and_respects_notional_and_precision(action):
    b = book()
    result = prepare_order(Asset(1, 4), b, b, decision(action), Settings(), time.monotonic())
    order = result["action"]["orders"][0]
    assert order["t"] == {"limit": {"tif": "Alo"}}
    assert order["a"] == 1
    assert Decimal("10") <= Decimal(order["p"]) * Decimal(order["s"]) <= Decimal("20")
    assert len(order["s"].split(".")[1]) <= 4
    assert len(order["c"]) == 34
    assert result["submission_enabled"] is False


@pytest.mark.parametrize(
    "scenario,reason",
    [
        ("missing", "feed_disconnected"),
        ("local_age", "stale_local_book"),
        ("exchange_age", "stale_exchange_book_or_clock_skew"),
        ("future", "stale_exchange_book_or_clock_skew"),
        ("generation", "feed_reconnected_during_inference"),
        ("input_age", "stale_decision_input"),
        ("moved", "book_moved_during_inference"),
        ("wide", "spread_too_wide"),
    ],
)
def test_unsafe_candidate_has_no_order(scenario, reason):
    initial = book()
    current = initial
    if scenario == "missing":
        current = None
    elif scenario == "local_age":
        current = replace(initial, received_mono=time.monotonic() - 4)
    elif scenario == "exchange_age":
        current = replace(initial, exchange_ms=initial.exchange_ms - 5000)
    elif scenario == "future":
        current = replace(initial, exchange_ms=initial.exchange_ms + 5000)
    elif scenario == "generation":
        current = replace(initial, generation=1)
    elif scenario == "input_age":
        initial = replace(initial, received_mono=time.monotonic() - 4)
    elif scenario == "moved":
        current = book("2500.0", "2500.1")
    elif scenario == "wide":
        current = book("2490", "2500")
    result = prepare_order(Asset(1, 4), initial, current, decision(), Settings(), time.monotonic())
    assert result["action"] is None
    assert reason in result["reasons"]


def test_slow_inference_blocks_even_with_a_recent_initial_book():
    b = book()
    result = prepare_order(Asset(1, 4), b, b, decision(), Settings(), time.monotonic() - 3)
    assert "stale_decision_input" in result["reasons"]


def test_exchange_age_of_original_input_is_part_of_decision_deadline():
    current = book()
    initial = replace(current, exchange_ms=current.exchange_ms - 3000)
    result = prepare_order(Asset(1, 4), initial, current, decision(), Settings(), time.monotonic())
    assert result["action"] is None
    assert "stale_decision_input" in result["reasons"]


def test_low_probability_and_hold_never_produce_orders():
    b = book()
    for d in (decision(probability=0.6), decision("HOLD")):
        assert prepare_order(Asset(1, 4), b, b, d, Settings(), time.monotonic())["action"] is None


@pytest.mark.parametrize(
    "price,decimals,is_buy,expected",
    [
        ("2495.78", 4, True, "2495.7"),
        ("2495.78", 4, False, "2495.8"),
        ("123456", 4, True, "123456"),
        ("0.01234", 1, True, "0.01234"),
        ("0.012345", 1, True, "0.01234"),
    ],
)
def test_price_rounding_obeys_exchange_rules(price, decimals, is_buy, expected):
    assert round_price(Decimal(price), decimals, is_buy) == Decimal(expected)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -0.1, 1.1, True])
def test_invalid_model_probabilities_cannot_reach_execution(bad):
    body = response()
    body["answers"]["action"]["probabilities"]["BUY"] = bad
    with pytest.raises(InfrastructureError):
        parse_decision(body)


def test_response_needs_expected_options_sum_and_winning_choice():
    variants = []
    for mutate in (
        lambda a: a.update(choice="withdraw"),
        lambda a: a.update(choice="SELL"),
        lambda a: a.update(probabilities={"BUY": 1}),
        lambda a: a.update(probabilities={"BUY": 1, "SELL": 1, "HOLD": 0}),
        lambda a: a.update(confidence=float("nan")),
        lambda a: a.update(probabilities=["BUY", "SELL", "HOLD"]),
    ):
        body = copy.deepcopy(response())
        mutate(body["answers"]["action"])
        variants.append(body)
    for body in variants:
        with pytest.raises(InfrastructureError):
            parse_decision(body)


@pytest.mark.parametrize("provider", ["clef", "jev"])
def test_model_wire_contract_and_envelope(monkeypatch, provider):
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "a" * 32)
    monkeypatch.setenv("CLOUDFLARE_AUTH_TOKEN", "test-not-a-real-credential")
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-not-a-real-credential")

    async def exercise():
        def handler(req):
            data = json.loads(req.content)
            assert data["questions"]["action"]["type"] == "choice"
            assert set(data["questions"]["action"]["criteria"]) == {"BUY", "SELL", "HOLD"}
            assert "test-not-a-real-credential" not in req.content.decode()
            if provider == "clef":
                assert req.url.path.endswith("/ai/run/@cf/cloudflare/clef")
                payload = {"success": True, "result": response()}
            else:
                assert str(req.url) == "https://api.typesafe.ai/v1/systemone"
                assert data["model"] == "jev-latest"
                payload = response()
            return httpx.Response(200, json=payload)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            assert (await Model(client, provider).decide({"mid": 2500})).action == "BUY"

    asyncio.run(exercise())


def test_auth_error_contains_no_key_or_response_body(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "sensitive-test-key")

    async def exercise():
        transport = httpx.MockTransport(lambda _: httpx.Response(401, text="sensitive-test-key"))
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(InfrastructureError, match="jev HTTP 401") as exc:
                await Model(client, "jev").decide({})
            assert "sensitive" not in str(exc.value)

    asyncio.run(exercise())


def test_total_model_deadline_stops_slow_transport(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "sensitive-test-key")

    async def exercise():
        async def handler(_):
            await asyncio.sleep(0.2)
            return httpx.Response(200, json=response())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(InfrastructureError, match="TimeoutError"):
                await Model(client, "jev", timeout_s=0.01).decide({})

    asyncio.run(exercise())


@pytest.mark.parametrize("body", [[], None, {"success": True, "result": []}])
def test_invalid_cloudflare_envelope_fails_safely(monkeypatch, body):
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "a" * 32)
    monkeypatch.setenv("CLOUDFLARE_AUTH_TOKEN", "sensitive-test-key")

    async def exercise():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body))
        ) as client:
            with pytest.raises(InfrastructureError):
                await Model(client, "clef").decide({})

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "body", [None, {}, [{"universe": []}], [{"universe": [{"name": "ETH", "szDecimals": 1.5}]}, [{}]]]
)
def test_malformed_metadata_fails_safely(body):
    async def exercise():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body))
        ) as client:
            with pytest.raises(InfrastructureError):
                await Hyperliquid(client, Settings()).eth_meta()

    asyncio.run(exercise())


@pytest.mark.parametrize("value", [float("nan"), -1, True, "100"])
def test_invalid_token_counts_do_not_break_logging(value):
    body = response()
    body["usage"]["input_tokens"] = value
    with pytest.raises(InfrastructureError):
        parse_decision(body)


def test_trade_snapshots_do_not_double_count_and_exclude_future_trades():
    feed = Feed(Settings(), lambda _: None)
    b = book()
    trades = [
        {"tid": 1, "coin": "ETH", "time": b.exchange_ms - 1, "side": "B", "sz": "1"},
        {"tid": 2, "coin": "ETH", "time": b.exchange_ms + 5000, "side": "A", "sz": "3"},
    ]
    feed.ingest({"channel": "trades", "data": trades})
    feed.ingest({"channel": "trades", "data": trades})
    state = feed.state(b)
    assert state["observed_trade_count_60s"] == 1
    assert state["observed_taker_buy_fraction_60s"] == 1
    assert state["mid_return_30s_bps"] is None


def test_same_order_pair_can_trade_again_in_another_block():
    feed = Feed(Settings(), lambda _: None)
    b = book()
    feed.ingest(
        {
            "channel": "trades",
            "data": [
                {"tid": 1, "coin": "ETH", "time": b.exchange_ms - 10, "side": "B", "sz": "1"},
                {"tid": 1, "coin": "ETH", "time": b.exchange_ms - 5, "side": "A", "sz": "1"},
            ],
        }
    )
    assert feed.state(b)["observed_trade_count_60s"] == 2


def test_feed_requests_fast_book_and_allows_explicit_slow_comparison():
    assert Feed(Settings(), lambda _: None).subscriptions()[0] == {
        "type": "l2Book",
        "coin": "ETH",
        "fast": True,
    }
    assert Feed(Settings(fast_book=False), lambda _: None).subscriptions()[0]["fast"] is False


def test_official_sdk_can_sign_and_recover_preview_locally():
    pytest.importorskip(
        "hyperliquid.utils.signing", reason="Install the optional signing extra for this test"
    )
    from eth_account import Account
    from hyperliquid.utils.signing import recover_agent_or_user_from_l1_action, sign_l1_action

    wallet = Account.create()
    b = book()
    action = prepare_order(Asset(1, 4), b, b, decision(), Settings(), time.monotonic())["action"]
    for mainnet in (True, False):
        signature = sign_l1_action(wallet, action, None, 1, 0, mainnet)
        recovered = recover_agent_or_user_from_l1_action(action, signature, None, 1, 0, mainnet)
        assert recovered == wallet.address
