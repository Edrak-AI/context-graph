"""Unit tests for app.utils.edrak_spend_gate (stdlib only; runnable as plain python3 or via pytest).

    cd backend/python && python3 tests/unit/utils/test_edrak_spend_gate.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from app.utils import edrak_spend_gate as gate  # noqa: E402

USAGE_URL = "http://edrak-chat.internal/api/internal/cgraph-usage"
DENIAL = {
    "allowed": False,
    "reason": "org_daily_limit",
    "scope": "org",
    "errorKey": "edrak_spend_daily_limit_reached_org",
    "spendMicroUsd": 123456,
    "limitMicroUsd": 100000,
}


class FakeTransport:
    """Scripted (status, body) responses; an Exception instance in the script is raised."""

    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, str], str]] = []

    async def __call__(self, url: str, params: dict[str, str], token: str) -> tuple[int, Any]:
        self.calls.append((url, params, token))
        item = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(item, Exception):
            raise item
        return item


async def _token() -> str | None:
    return "tok"


async def _no_token() -> str | None:
    return None


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def run(coro: Any) -> Any:
    return asyncio.run(coro)


class SpendGateTests(unittest.TestCase):
    def setUp(self) -> None:
        gate.clear_cache()
        gate._last_fail_open_log = 0.0
        os.environ[gate.USAGE_URL_ENV] = USAGE_URL
        os.environ.pop(gate.SPEND_GATE_URL_ENV, None)

    def tearDown(self) -> None:
        os.environ.pop(gate.USAGE_URL_ENV, None)
        os.environ.pop(gate.SPEND_GATE_URL_ENV, None)

    # --- URL derivation -----------------------------------------------------------------------

    def test_gate_url_derived_from_usage_url(self) -> None:
        self.assertEqual(gate.gate_url(), "http://edrak-chat.internal/api/internal/spend-gate")

    def test_gate_url_from_base_only_usage_url(self) -> None:
        os.environ[gate.USAGE_URL_ENV] = "https://dev.edrak.com/"
        self.assertEqual(gate.gate_url(), "https://dev.edrak.com/api/internal/spend-gate")

    def test_explicit_gate_url_wins(self) -> None:
        os.environ[gate.SPEND_GATE_URL_ENV] = "http://other/api/internal/spend-gate"
        self.assertEqual(gate.gate_url(), "http://other/api/internal/spend-gate")

    def test_inert_without_env(self) -> None:
        os.environ.pop(gate.USAGE_URL_ENV)
        self.assertFalse(gate.is_enabled())
        transport = FakeTransport((200, DENIAL))
        result = run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_token))
        self.assertTrue(result.allowed)
        self.assertEqual(result.source, "disabled")
        self.assertEqual(transport.calls, [])

    # --- parsing --------------------------------------------------------------------------------

    def test_parse_allowed(self) -> None:
        r = gate.parse_response(200, {"allowed": True, "reason": None, "orgOwnerId": "x"})
        self.assertTrue(r.allowed)
        self.assertEqual(r.source, "edrak")
        self.assertIsNone(r.reason)

    def test_parse_denial(self) -> None:
        r = gate.parse_response(200, DENIAL)
        self.assertFalse(r.allowed)
        self.assertEqual(r.reason, "org_daily_limit")
        self.assertEqual(r.error_key, "edrak_spend_daily_limit_reached_org")
        self.assertEqual(r.scope, "org")
        self.assertEqual(r.spend_micro_usd, 123456)
        self.assertEqual(r.limit_micro_usd, 100000)
        self.assertEqual(
            r.http_detail(),
            {
                "error": "spend_limit",
                "reason": "org_daily_limit",
                "errorKey": "edrak_spend_daily_limit_reached_org",
                "message": "AI spend limit reached; contact your organisation admin.",
            },
        )

    def test_parse_unknown_user_fails_open(self) -> None:
        r = gate.parse_response(200, {"allowed": True, "reason": "unknown_user"})
        self.assertTrue(r.allowed)
        self.assertEqual(r.source, "fail_open")

    def test_parse_non_2xx_and_garbage_fail_open(self) -> None:
        for status, body in ((500, {"error": "boom"}), (401, {"error": "unauthorized"}), (200, None), (200, "text"), (200, [])):
            r = gate.parse_response(status, body)
            self.assertTrue(r.allowed, (status, body))
            self.assertEqual(r.source, "fail_open", (status, body))

    def test_parse_allowed_false_only_when_explicit_false(self) -> None:
        # `allowed: 0` / missing / null are not an explicit denial
        for body in ({"allowed": 0}, {"allowed": None}, {}):
            self.assertTrue(gate.parse_response(200, body).allowed, body)

    # --- decisions through the entry point -----------------------------------------------------

    def test_denial_fails_closed_and_is_not_cached(self) -> None:
        transport = FakeTransport((200, DENIAL))
        r1 = run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_token))
        r2 = run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_token))
        self.assertFalse(r1.allowed)
        self.assertFalse(r2.allowed)
        self.assertEqual(len(transport.calls), 2)
        url, params, token = transport.calls[0]
        self.assertEqual(url, "http://edrak-chat.internal/api/internal/spend-gate")
        self.assertEqual(params, {"cgraphUserId": "u1"})
        self.assertEqual(token, "tok")

    def test_transport_exception_fails_open_and_is_not_cached(self) -> None:
        transport = FakeTransport(ConnectionError("down"))
        r = run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_token))
        self.assertTrue(r.allowed)
        self.assertEqual(r.source, "fail_open")
        self.assertEqual(r.reason, "transport_error")
        run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_token))
        self.assertEqual(len(transport.calls), 2)

    def test_5xx_fails_open(self) -> None:
        transport = FakeTransport((503, None))
        r = run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_token))
        self.assertTrue(r.allowed)
        self.assertEqual(r.source, "fail_open")

    def test_missing_token_fails_open_without_calling(self) -> None:
        transport = FakeTransport((200, DENIAL))
        r = run(gate.check_spend_allowed("u1", "o1", transport=transport, token_provider=_no_token))
        self.assertTrue(r.allowed)
        self.assertEqual(r.reason, "no_token")
        self.assertEqual(transport.calls, [])

    def test_missing_user_fails_open_without_calling(self) -> None:
        transport = FakeTransport((200, DENIAL))
        r = run(gate.check_spend_allowed(None, "o1", transport=transport, token_provider=_token))
        self.assertTrue(r.allowed)
        self.assertEqual(transport.calls, [])

    # --- cache ----------------------------------------------------------------------------------

    def test_allow_is_cached_per_user_for_ttl(self) -> None:
        clock = FakeClock()
        transport = FakeTransport((200, {"allowed": True, "reason": None}))
        kw = dict(transport=transport, token_provider=_token, clock=clock)
        r1 = run(gate.check_spend_allowed("u1", "o1", **kw))
        r2 = run(gate.check_spend_allowed("u1", "o1", **kw))
        self.assertEqual((r1.source, r2.source), ("edrak", "cache"))
        self.assertEqual(len(transport.calls), 1)
        # another user is not served from u1's entry
        run(gate.check_spend_allowed("u2", "o1", **kw))
        self.assertEqual(len(transport.calls), 2)
        # just before expiry: still cached; at expiry: refetched
        clock.t += gate.CACHE_TTL_S - 0.001
        run(gate.check_spend_allowed("u1", "o1", **kw))
        self.assertEqual(len(transport.calls), 2)
        clock.t += 0.001
        r3 = run(gate.check_spend_allowed("u1", "o1", **kw))
        self.assertEqual(r3.source, "edrak")
        self.assertEqual(len(transport.calls), 3)

    def test_cached_allow_then_denial_after_expiry(self) -> None:
        clock = FakeClock()
        transport = FakeTransport((200, {"allowed": True}), (200, DENIAL))
        kw = dict(transport=transport, token_provider=_token, clock=clock)
        self.assertTrue(run(gate.check_spend_allowed("u1", "o1", **kw)).allowed)
        clock.t += gate.CACHE_TTL_S
        self.assertFalse(run(gate.check_spend_allowed("u1", "o1", **kw)).allowed)

    def test_fail_open_results_are_not_cached(self) -> None:
        clock = FakeClock()
        transport = FakeTransport((200, {"allowed": True, "reason": "unknown_user"}))
        kw = dict(transport=transport, token_provider=_token, clock=clock)
        run(gate.check_spend_allowed("u1", "o1", **kw))
        run(gate.check_spend_allowed("u1", "o1", **kw))
        self.assertEqual(len(transport.calls), 2)

    def test_fail_open_logs_once_per_minute(self) -> None:
        clock = FakeClock()
        transport = FakeTransport(ConnectionError("down"))
        kw = dict(transport=transport, token_provider=_token, clock=clock)
        with self.assertLogs(gate.logger, level="DEBUG") as cm:
            run(gate.check_spend_allowed("u1", "o1", **kw))
            clock.t += 10
            run(gate.check_spend_allowed("u1", "o1", **kw))
            clock.t += gate.FAIL_OPEN_LOG_INTERVAL_S
            run(gate.check_spend_allowed("u1", "o1", **kw))
        levels = [r.levelname for r in cm.records]
        self.assertEqual(levels, ["WARNING", "DEBUG", "WARNING"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
