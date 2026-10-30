import time
import uuid
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from pipeline.config import Settings
from pipeline.decision import Decision
from pipeline.market import Asset, Book


def wire_decimal(value: Decimal):
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def round_price(price: Decimal, size_decimals: int, is_buy: bool):
    # Integer prices are always valid; fractional prices obey both exchange limits.
    quantum = Decimal(10) ** max(price.adjusted() - 4, -(6 - size_decimals))
    rounding = ROUND_FLOOR if is_buy else ROUND_CEILING
    if price == price.to_integral_value():
        return price
    return (price / quantum).to_integral_value(rounding=rounding) * quantum


def prepare_order(
    asset: Asset,
    initial: Book,
    current: Book | None,
    decision: Decision,
    settings: Settings,
    decision_started: float,
):
    reasons = []
    if current is None:
        return {"status": "blocked", "reasons": ["feed_disconnected"], "action": None}
    if initial.generation != current.generation:
        reasons.append("feed_reconnected_during_inference")
    if current.age_ms() > settings.max_book_age_ms:
        reasons.append("stale_local_book")
    exchange_age = current.estimated_exchange_age_ms()
    if exchange_age > settings.max_book_age_ms or exchange_age < -250:
        reasons.append("stale_exchange_book_or_clock_skew")
    if (
        max(
            initial.age_ms(),
            initial.estimated_exchange_age_ms(),
            (time.monotonic() - decision_started) * 1000,
        )
        > settings.max_decision_age_ms
    ):
        reasons.append("stale_decision_input")
    if current.spread_bps > settings.max_spread_bps:
        reasons.append("spread_too_wide")
    move_bps = float(abs(current.mid / initial.mid - 1) * 10000)
    if move_bps > settings.max_mid_move_bps:
        reasons.append("book_moved_during_inference")
    if decision.action == "HOLD":
        return {"status": "hold", "reasons": reasons or ["model_hold"], "action": None}
    if decision.probabilities[decision.action] < settings.min_probability:
        reasons.append("low_option_probability")
    if reasons:
        return {"status": "blocked", "reasons": reasons, "action": None}
    is_buy = decision.action == "BUY"
    price = round_price(current.bid if is_buy else current.ask, asset.size_decimals, is_buy)
    if price <= 0:
        return {"status": "blocked", "reasons": ["invalid_rounded_price"], "action": None}
    size = (settings.order_usdc / price).quantize(Decimal(10) ** -asset.size_decimals, rounding=ROUND_FLOOR)
    notional = price * size
    if size <= 0 or not Decimal("10") <= notional <= settings.max_order_usdc:
        return {"status": "blocked", "reasons": ["invalid_order_notional_after_rounding"], "action": None}
    if (is_buy and price >= current.ask) or (not is_buy and price <= current.bid):
        return {"status": "blocked", "reasons": ["would_cross_book"], "action": None}
    action = {
        "type": "order",
        "orders": [
            {
                "a": asset.asset_id,
                "b": is_buy,
                "p": wire_decimal(price),
                "s": wire_decimal(size),
                "r": False,
                "t": {"limit": {"tif": "Alo"}},
                "c": "0x" + uuid.uuid4().hex,
            }
        ],
        "grouping": "na",
    }
    return {
        "status": "preview",
        "reasons": [],
        "action": action,
        "notional_usdc": str(notional),
        "submission_enabled": False,
        "requires_before_submission": [
            "fresh_account_state",
            "effective_leverage_at_most_1x",
            "outstanding_order_exposure",
            "agent_approval",
            "validated_strategy",
        ],
    }
