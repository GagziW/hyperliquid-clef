import asyncio
import math
import os
from dataclasses import asdict, dataclass

import httpx

from pipeline.market import InfrastructureError


@dataclass(frozen=True)
class Decision:
    action: str
    probabilities: dict[str, float]
    confidence: float
    model: str
    usage: dict

    def record(self):
        return asdict(self)


def parse_decision(data: dict) -> Decision:
    try:
        if not isinstance(data, dict):
            raise ValueError
        answer = data["answers"]["action"]
        probabilities = answer["probabilities"]
        if (
            answer["type"] != "choice"
            or not isinstance(probabilities, dict)
            or set(probabilities) != {"BUY", "SELL", "HOLD"}
        ):
            raise ValueError
        if any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 0 <= v <= 1
            for v in probabilities.values()
        ):
            raise ValueError
        if abs(sum(probabilities.values()) - 1) > 0.001:
            raise ValueError
        action = answer["choice"]
        confidence = answer["confidence"]
        if action not in probabilities or probabilities[action] < max(probabilities.values()):
            raise ValueError
        if (
            isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not math.isfinite(confidence)
            or not 0 <= confidence <= 1
        ):
            raise ValueError
        model, usage = data["model"], data.get("usage", {})
        if not isinstance(model, str) or not model or not isinstance(usage, dict):
            raise ValueError
        tokens = {key: usage[key] for key in ("input_tokens", "output_tokens") if key in usage}
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in tokens.values()):
            raise ValueError
        return Decision(action, probabilities, confidence, model, tokens)
    except (KeyError, TypeError, ValueError):
        raise InfrastructureError("Invalid model choice response") from None


def question():
    return {
        "action": {
            "type": "choice",
            "instructions": (
                "Select one candidate ETH perpetual action using only the observed state. "
                "The experiment has no validated predictive edge. Missing history is unknown, not zero. "
                "Prefer HOLD if there is insufficient evidence. Code controls price, size, and execution. "
                "Your option probabilities are judgments, not calibrated probabilities of trade profitability."
            ),
            "criteria": {
                "BUY": "Evidence favors a small passive long candidate over the next 60 seconds.",
                "SELL": "Evidence favors a small passive short candidate over the next 60 seconds.",
                "HOLD": "Insufficient directional evidence, incomplete history, or uncertain conditions.",
            },
        }
    }


class Model:
    def __init__(self, client: httpx.AsyncClient, provider: str, timeout_s: float = 2):
        self.client, self.provider, self.timeout_s = client, provider, timeout_s

    def missing(self):
        if self.provider in {"clef", "clef-flash"}:
            missing = []
            if not os.environ.get("CLOUDFLARE_ACCOUNT_ID"):
                missing.append("CLOUDFLARE_ACCOUNT_ID")
            if not (os.environ.get("CLOUDFLARE_AUTH_TOKEN") or os.environ.get("CLOUDFLARE_API_TOKEN")):
                missing.append("CLOUDFLARE_AUTH_TOKEN (or CLOUDFLARE_API_TOKEN)")
            return missing
        return (
            ["TYPESAFE_API_KEY"] if self.provider == "jev" and not os.environ.get("TYPESAFE_API_KEY") else []
        )

    async def decide(self, state):
        if self.provider == "mock":
            return Decision("HOLD", {"BUY": 0.0, "SELL": 0.0, "HOLD": 1.0}, 1.0, "mock-hold", {})
        if missing := self.missing():
            raise InfrastructureError("Missing environment variables: " + ", ".join(missing))
        if self.provider in {"clef", "clef-flash"}:
            account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
            import re

            if not re.fullmatch(r"[a-fA-F0-9]{32}", account):
                raise InfrastructureError("Invalid CLOUDFLARE_ACCOUNT_ID format")
            url = f"https://api.cloudflare.com/client/v4/accounts/{account}/ai/run/@cf/cloudflare/{self.provider}"
            key = os.environ.get("CLOUDFLARE_AUTH_TOKEN") or os.environ["CLOUDFLARE_API_TOKEN"]
            model = self.provider
        else:
            url, key = "https://api.typesafe.ai/v1/systemone", os.environ["TYPESAFE_API_KEY"]
            model = os.environ.get("TYPESAFE_MODEL", "jev-latest")
        try:
            # HTTP timeouts bound individual phases; this also bounds the full inference round trip.
            async with asyncio.timeout(self.timeout_s):
                response = await self.client.post(
                    url,
                    headers={"Authorization": f"Bearer {key}"},
                    json={"model": model, "state": state, "questions": question()},
                    timeout=self.timeout_s,
                )
            if response.status_code != 200:
                raise InfrastructureError(f"{self.provider} HTTP {response.status_code}")
            data = response.json()
            if self.provider in {"clef", "clef-flash"}:
                if not isinstance(data, dict) or data.get("success") is not True:
                    raise InfrastructureError("Cloudflare inference unsuccessful")
                data = data["result"]
            return parse_decision(data)
        except (httpx.HTTPError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            raise InfrastructureError(f"{self.provider} inference {type(exc).__name__}") from None
