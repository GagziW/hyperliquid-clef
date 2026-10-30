from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Settings:
    network: str = "mainnet"
    provider: str = "mock"
    fast_book: bool = True
    interval_s: float = 5.0
    max_book_age_ms: float = 1500.0
    max_decision_age_ms: float = 2000.0
    max_mid_move_bps: float = 2.0
    max_spread_bps: float = 5.0
    min_probability: float = 0.8
    order_usdc: Decimal = Decimal("20")
    max_order_usdc: Decimal = Decimal("25")

    def __post_init__(self):
        if self.network not in {"mainnet", "testnet"}:
            raise ValueError("Unsupported network")
        if self.provider not in {"mock", "clef", "clef-flash", "jev"}:
            raise ValueError("Unsupported provider")
        import math

        for value in (
            self.interval_s,
            self.max_book_age_ms,
            self.max_decision_age_ms,
            self.max_mid_move_bps,
            self.max_spread_bps,
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Timing and price limits must be finite and positive")
        if not 0 < self.min_probability <= 1:
            raise ValueError("Probability threshold must be in (0, 1]")
        if not self.order_usdc.is_finite() or not self.max_order_usdc.is_finite():
            raise ValueError("Order limits must be finite")
        if not Decimal("10") <= self.order_usdc <= self.max_order_usdc:
            raise ValueError("Order notional must be at least 10 USDC and within the cap")

    @property
    def http_url(self):
        domain = "api.hyperliquid.xyz" if self.network == "mainnet" else "api.hyperliquid-testnet.xyz"
        return f"https://{domain}"

    @property
    def ws_url(self):
        return self.http_url.replace("https://", "wss://") + "/ws"
