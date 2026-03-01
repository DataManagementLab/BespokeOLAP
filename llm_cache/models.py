import logging
from typing import Tuple

MODELS = {
    "gpt-5.1": {
        "input": 1.25 / 1_000_000,  # USD pro Token
        "cached_input": 0.125 / 1_000_000,  # USD pro Token
        "output": 10.00 / 1_000_000,  # USD pro Token
        "context_window": 400_000,
    },
    "gpt-5.1-codex": {
        "input": 1.25 / 1_000_000,  # USD pro Token
        "cached_input": 0.125 / 1_000_000,  # USD pro Token
        "output": 10.00 / 1_000_000,  # USD pro Token
        "context_window": 400_000,
    },
    "gpt-5.1-codex-max": {
        "input": 1.25 / 1_000_000,  # USD pro Token
        "cached_input": 0.125 / 1_000_000,  # USD pro Token
        "output": 10.00 / 1_000_000,  # USD pro Token
        "context_window": 400_000,
    },
    "gpt-5.2-codex": {
        "input": 1.75 / 1_000_000,  # USD pro Token
        "cached_input": 0.17 / 1_000_000,  # USD pro Token
        "output": 14.00 / 1_000_000,  # USD pro Token
        "context_window": 400_000,
    },
    "anthropic/claude-opus-4-20250514": {
        "input": 15.00 / 1_000_000,
        "cached_input": 1.50 / 1_000_000,
        "output": 75.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4-1-20250514": {
        "input": 15.00 / 1_000_000,
        "cached_input": 1.50 / 1_000_000,
        "output": 75.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4-5-20250514": {
        "input": 5.00 / 1_000_000,
        "cached_input": 0.50 / 1_000_000,
        "output": 25.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4-6-20250514": {
        "input": 5.00 / 1_000_000,
        "cached_input": 0.50 / 1_000_000,
        "output": 25.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4": {
        "input": 15.00 / 1_000_000,
        "cached_input": 1.50 / 1_000_000,
        "output": 75.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4-1": {
        "input": 15.00 / 1_000_000,
        "cached_input": 1.50 / 1_000_000,
        "output": 75.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4-5": {
        "input": 5.00 / 1_000_000,
        "cached_input": 0.50 / 1_000_000,
        "output": 25.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-opus-4-6": {
        "input": 5.00 / 1_000_000,
        "cached_input": 0.50 / 1_000_000,
        "output": 25.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-sonnet-4-20250514": {
        "input": 3.00 / 1_000_000,
        "cached_input": 0.30 / 1_000_000,
        "output": 15.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-sonnet-4-5-20250514": {
        "input": 3.00 / 1_000_000,
        "cached_input": 0.30 / 1_000_000,
        "output": 15.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-sonnet-4-20250514-5": {
        "input": 3.00 / 1_000_000,
        "cached_input": 0.30 / 1_000_000,
        "output": 15.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-sonnet-4": {
        "input": 3.00 / 1_000_000,
        "cached_input": 0.30 / 1_000_000,
        "output": 15.00 / 1_000_000,
        "context_window": 200_000,
    },
    "anthropic/claude-sonnet-4-5": {
        "input": 3.00 / 1_000_000,
        "cached_input": 0.30 / 1_000_000,
        "output": 15.00 / 1_000_000,
        "context_window": 200_000,
    },
}

logger = logging.getLogger(__name__)


def request_cost_usd(
    model, input_tokens: int, cached_tokens: int, output_tokens: int
) -> float:
    prices = MODELS[str(model)]

    billable_input_tokens = input_tokens - cached_tokens

    return (
        billable_input_tokens * prices["input"]
        + cached_tokens * prices["cached_input"]
        + output_tokens * prices["output"]
    )


def context_window_usage(model, used_tokens: int) -> Tuple[str, float]:
    window_size = MODELS[str(model)]["context_window"]

    used_pct = (used_tokens / window_size) * 100
    left_pct = 100 - used_pct

    def fmt_k(n: int) -> str:
        return f"{n / 1000:.1f}K" if n >= 1000 else str(n)

    return (
        f"{left_pct:.0f}% left ({fmt_k(used_tokens)} used / {fmt_k(window_size)})"
    ), used_pct / 100
