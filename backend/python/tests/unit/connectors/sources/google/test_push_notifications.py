"""Google push helpers: pure functions, Drive channel / Gmail watch managers on fake transports, reverse index."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from app.connectors.core.base.webhooks.subscription_store import SubscriptionStore
from app.connectors.sources.google.common.push_notifications import (
    DRIVE_CHANNEL_LIFETIME,
    DriveChannel,
    DriveChannelManager,
    DriveWatchTarget,
    GmailReverseIndex,
    GmailWatch,
    GmailWatchManager,
    GooglePushError,
    drive_channel_body,
    drive_receiver_url,
    gmail_reverse_index_key,
    gmail_watch_body,
    is_expiring_ms,
    kv_store_of,
    load_drive_webhook_settings,
    parse_epoch_ms,
    plan_drive_channels,
    plan_gmail_watches,
    pubsub_topic,
    push_error_from,
    remove_gmail_watches,
    sync_drive_channels,
    sync_gmail_watches,
    to_epoch_ms,
    webhook_channel_token,
)

if TYPE_CHECKING:
    import pytest

NOW = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
LOGGER = logging.getLogger("test-google-push")
ADDRESS = "https://dev.edrak.com/api/webhooks/google/drive/conn-1"
TOPIC = "projects/edrak-chat-nonprod/topics/cgraph-gmail"


def ms(value: datetime) -> int:
    return to_epoch_ms(value)


class FakeSyncPoint:
    def __init__(self) -> None:
        self.points: dict[str, dict[str, Any]] = {}

    async def read_sync_point(self, key: str) -> dict[str, Any]:
        return dict(self.points.get(key, {}))

    async def update_sync_point(self, key: str, data: dict[str, Any], encrypt_fields: list[str] | None = None) -> None:
        self.points[key] = data


class FakeKv:
    def __init__(self) -> None:
        self.keys: dict[str, Any] = {}

    async def get_key(self, key: str) -> object:
        return self.keys.get(key)

    async def create_key(self, key: str, value: object, overwrite: bool = True, ttl: int | None = None) -> bool:
        if not overwrite and key in self.keys:
            return False
        self.keys[key] = value
        return True

    async def delete_key(self, key: str) -> bool:
        return self.keys.pop(key, None) is not None


class FakeDrive:
    """``changes.watch`` / ``channels.stop`` bookkeeping; ``fail_watch`` raises on every watch."""

    def __init__(self) -> None:
        self.channels: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, ...]] = []
        self.fail_watch: Exception | None = None
        self.fail_stop: set[str] = set()
        self._seq = 0

    async def watch(self, user_email: str, page_token: str, body: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("watch", user_email, page_token))
        if self.fail_watch is not None:
            raise self.fail_watch
        self._seq += 1
        row = {"kind": "api#channel", "id": body["id"], "resourceId": f"res-{self._seq}", "expiration": body["expiration"], "user": user_email}
        self.channels[body["id"]] = row
        return dict(row)

    async def stop(self, user_email: str, channel_id: str, resource_id: str) -> None:
        self.calls.append(("stop", user_email, channel_id))
        if channel_id in self.fail_stop:
            raise GooglePushError(500, "backend error")
        if channel_id not in self.channels:
            raise GooglePushError(404, "gone")
        assert self.channels[channel_id]["resourceId"] == resource_id
        del self.channels[channel_id]


class FakeGmail:
    def __init__(self, expiration: datetime | None = None) -> None:
        self.watches: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str]] = []
        self.fail_watch: Exception | None = None
        self.expiration = expiration or NOW + timedelta(days=7)

    async def watch(self, user_email: str, body: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("watch", user_email))
        if self.fail_watch is not None:
            raise self.fail_watch
        self.watches[user_email] = body
        return {"historyId": "778899", "expiration": str(ms(self.expiration))}

    async def stop(self, user_email: str) -> None:
        self.calls.append(("stop", user_email))
        self.watches.pop(user_email, None)


def drive_manager(api: FakeDrive, point: FakeSyncPoint, now: datetime = NOW, logger: logging.Logger | MagicMock = LOGGER) -> DriveChannelManager:
    return DriveChannelManager(api, "conn-1", ADDRESS, "tok", logger, store=SubscriptionStore(point), now=lambda: now)


def gmail_manager(
    api: FakeGmail, point: FakeSyncPoint, kv: FakeKv, now: datetime = NOW, connector_id: str = "conn-1", logger: logging.Logger | MagicMock = LOGGER
) -> GmailWatchManager:
    return GmailWatchManager(
        api, connector_id, TOPIC, logger, store=SubscriptionStore(point), reverse_index=GmailReverseIndex(kv), now=lambda: now
    )


def rows(point: FakeSyncPoint) -> list[dict[str, Any]]:
    stored = point.points["webhooks"]["webhooks"]
    assert isinstance(stored, str)  # one JSON string: Neo4j rejects a list of maps
    return json.loads(stored)


class TestPureHelpers:
    def test_channel_token_is_truncated_hmac_with_google_prefix(self) -> None:
        expected = hmac.new(b"secret", b"google-webhook:conn-1", hashlib.sha256).hexdigest()[:40]
        assert webhook_channel_token("secret", "conn-1") == expected
        assert len(webhook_channel_token("secret", "conn-1")) == 40
        assert webhook_channel_token("secret", "conn-1") != webhook_channel_token("secret", "conn-2")

    def test_receiver_url_and_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert drive_receiver_url("https://dev.edrak.com/", "c 1") == "https://dev.edrak.com/api/webhooks/google/drive/c%201"
        monkeypatch.delenv("GOOGLE_PUBSUB_TOPIC", raising=False)
        assert pubsub_topic() == ""
        monkeypatch.setenv("GOOGLE_PUBSUB_TOPIC", f" {TOPIC} ")
        assert pubsub_topic() == TOPIC

    def test_reverse_index_key_is_lower_cased(self) -> None:
        assert gmail_reverse_index_key(" Ali@Edrak.COM ") == "cgraph:watch:gmail:ali@edrak.com"

    def test_epoch_helpers(self) -> None:
        assert parse_epoch_ms("1760000000000") == 1760000000000
        assert parse_epoch_ms(1760000000000) == 1760000000000
        assert parse_epoch_ms("garbage") is None and parse_epoch_ms(None) is None and parse_epoch_ms(True) is None
        assert is_expiring_ms(ms(NOW + timedelta(hours=1)), timedelta(hours=2), NOW)
        assert not is_expiring_ms(ms(NOW + timedelta(hours=3)), timedelta(hours=2), NOW)
        assert is_expiring_ms(None, timedelta(hours=2), NOW)

    def test_request_bodies(self) -> None:
        body = drive_channel_body(ADDRESS, "tok", ms(NOW), channel_id="abc")
        assert body == {"id": "abc", "type": "web_hook", "address": ADDRESS, "token": "tok", "expiration": str(ms(NOW))}
        assert len(drive_channel_body(ADDRESS, "tok", ms(NOW))["id"]) == 36
        assert gmail_watch_body(TOPIC) == {"topicName": TOPIC, "labelIds": ["INBOX", "SENT"], "labelFilterBehavior": "INCLUDE"}

    def test_push_error_from_duck_typed_http_error(self) -> None:
        class HttpErrorLike(Exception):
            def __init__(self) -> None:
                super().__init__("<HttpResponse 401>")
                self.resp = MagicMock(status=401)
                self.content = b'{"error": {"errors": [{"reason": "push.webhookUrlUnauthorized"}]}}'

        error = push_error_from(HttpErrorLike())
        assert error.status_code == 401 and error.is_webhook_unauthorized
        assert push_error_from(RuntimeError("x")).status_code == 0
        assert push_error_from(GooglePushError(404, "gone")).status_code == 404

    def test_load_settings_requires_origin_and_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class Config:
            def __init__(self, keys: object) -> None:
                self.keys = keys

            async def get_config(self, key: str) -> object:
                return self.keys

        monkeypatch.delenv("FRONTEND_PUBLIC_URL", raising=False)
        assert asyncio.run(load_drive_webhook_settings(Config({"scopedJwtSecret": "s"}), "c1", LOGGER)) is None
        monkeypatch.setenv("FRONTEND_PUBLIC_URL", "https://dev.edrak.com/")
        assert asyncio.run(load_drive_webhook_settings(Config({}), "c1", LOGGER)) is None
        settings = asyncio.run(load_drive_webhook_settings(Config({"scopedJwtSecret": "s"}), "c1", LOGGER))
        assert settings is not None
        assert settings.address == "https://dev.edrak.com/api/webhooks/google/drive/c1"
        assert settings.token == webhook_channel_token("s", "c1")

    def test_kv_store_of_config_service(self) -> None:
        kv = FakeKv()
        assert kv_store_of(MagicMock(store=kv)) is kv
        assert kv_store_of(object()) is None


class TestPlanning:
    def test_drive_plan(self) -> None:
        live = DriveChannel("a", "r-a", ms(NOW + timedelta(hours=10)), "a@x.com")
        expiring = DriveChannel("b", "r-b", ms(NOW + timedelta(minutes=30)), "b@x.com")
        pending = DriveChannel("c", "r-c", ms(NOW + timedelta(hours=5)), "c@x.com", stop_pending=True)
        orphan_live = DriveChannel("d", "r-d", ms(NOW + timedelta(hours=5)), "d@x.com")
        orphan_dead = DriveChannel("e", "r-e", ms(NOW - timedelta(hours=5)), "e@x.com")
        targets = [DriveWatchTarget("A@x.com", "p1"), DriveWatchTarget("b@x.com", "p2"), DriveWatchTarget("new@x.com", "p3")]
        plan = plan_drive_channels([live, expiring, pending, orphan_live, orphan_dead], targets, NOW)
        assert [t.user_email for t in plan.create] == ["new@x.com"]
        assert [(c.id, t.page_token) for c, t in plan.renew] == [("b", "p2")]
        assert plan.stop == [pending]
        assert {c.id for c in plan.keep} == {"a", "d"}

    def test_gmail_plan(self) -> None:
        fresh = GmailWatch("a@x.com", "1", ms(NOW + timedelta(days=5)))
        stale = GmailWatch("b@x.com", "2", ms(NOW + timedelta(hours=10)))
        orphan_dead = GmailWatch("c@x.com", "3", ms(NOW - timedelta(days=1)))
        orphan_live = GmailWatch("d@x.com", "4", ms(NOW + timedelta(days=2)))
        plan = plan_gmail_watches([fresh, stale, orphan_dead, orphan_live], ["A@x.com", "b@x.com", "new@x.com", ""], NOW)
        assert plan.refresh == ["b@x.com", "new@x.com"]
        assert {w.email_address for w in plan.keep} == {"a@x.com", "d@x.com"}
        assert plan.drop == [orphan_dead]

    def test_channel_row_round_trip(self) -> None:
        channel = DriveChannel("id", "res", 123, "U@x.com", stop_pending=True)
        loaded = DriveChannel.from_dict(channel.to_dict())
        assert loaded == DriveChannel("id", "res", 123, "u@x.com", stop_pending=True)
        assert DriveChannel.from_dict({"id": "x"}) is None
        assert GmailWatch.from_dict({"emailAddress": "A@x.com", "historyId": 5, "expiration": "7"}) == GmailWatch("a@x.com", "5", 7)


class TestDriveChannelManager:
    def test_creates_one_channel_per_user_and_persists(self) -> None:
        api, point = FakeDrive(), FakeSyncPoint()
        targets = [DriveWatchTarget("a@x.com", "p-a"), DriveWatchTarget("b@x.com", "p-b")]
        channels = asyncio.run(drive_manager(api, point).ensure(targets))
        assert [c[:3] for c in api.calls] == [("watch", "a@x.com", "p-a"), ("watch", "b@x.com", "p-b")]
        assert {c.user_email for c in channels} == {"a@x.com", "b@x.com"}
        stored = rows(point)
        assert {row["userEmail"] for row in stored} == {"a@x.com", "b@x.com"}
        assert all(row["resourceId"].startswith("res-") and len(row["id"]) == 36 for row in stored)
        assert all(row["expiration"] == ms(NOW + DRIVE_CHANNEL_LIFETIME) for row in stored)
        # a second run with enough lifetime left does nothing
        api.calls.clear()
        asyncio.run(drive_manager(api, point).ensure(targets))
        assert api.calls == []

    def test_renews_when_under_two_hours_new_first_then_stop_old(self) -> None:
        api, point = FakeDrive(), FakeSyncPoint()
        asyncio.run(drive_manager(api, point).ensure([DriveWatchTarget("a@x.com", "p1")]))
        old_id = rows(point)[0]["id"]
        later = NOW + DRIVE_CHANNEL_LIFETIME - timedelta(minutes=90)
        api.calls.clear()
        asyncio.run(drive_manager(api, point, now=later).ensure([DriveWatchTarget("a@x.com", "p2")]))
        assert api.calls[0][:3] == ("watch", "a@x.com", "p2")
        assert api.calls[1] == ("stop", "a@x.com", old_id)
        assert [row["id"] for row in rows(point)] != [old_id] and len(rows(point)) == 1
        assert list(api.channels) == [rows(point)[0]["id"]]

    def test_failed_stop_keeps_old_channel_pending_and_retries(self) -> None:
        api, point = FakeDrive(), FakeSyncPoint()
        asyncio.run(drive_manager(api, point).ensure([DriveWatchTarget("a@x.com", "p1")]))
        old_id = rows(point)[0]["id"]
        api.fail_stop.add(old_id)
        later = NOW + DRIVE_CHANNEL_LIFETIME - timedelta(minutes=30)
        logger = MagicMock()
        asyncio.run(drive_manager(api, point, now=later, logger=logger).ensure([DriveWatchTarget("a@x.com", "p2")]))
        pending = [row for row in rows(point) if row.get("stopPending")]
        assert [row["id"] for row in pending] == [old_id]
        assert len(rows(point)) == 2
        assert logger.warning.call_count == 1  # one warning per run
        # next run: the stop succeeds and the pending row disappears
        api.fail_stop.clear()
        asyncio.run(drive_manager(api, point, now=later + timedelta(minutes=5)).ensure([DriveWatchTarget("a@x.com", "p2")]))
        assert len(rows(point)) == 1 and not rows(point)[0].get("stopPending")

    def test_webhook_url_unauthorized_logs_once_and_skips(self) -> None:
        api, point = FakeDrive(), FakeSyncPoint()
        api.fail_watch = GooglePushError(401, '{"reason": "push.webhookUrlUnauthorized"}')
        logger = MagicMock()
        channels = asyncio.run(drive_manager(api, point, logger=logger).ensure([DriveWatchTarget("a@x.com", "p"), DriveWatchTarget("b@x.com", "p")]))
        assert channels == [] and rows(point) == []
        assert len(api.calls) == 1  # the second user is not even attempted
        assert logger.warning.call_count == 1
        message = logger.warning.call_args[0][0] % logger.warning.call_args[0][1:]
        assert "push.webhookUrlUnauthorized" in message and "Search Console" in message and ADDRESS in message

    def test_other_failures_are_one_warning_and_keep_expiring_channel(self) -> None:
        api, point = FakeDrive(), FakeSyncPoint()
        asyncio.run(drive_manager(api, point).ensure([DriveWatchTarget("a@x.com", "p1")]))
        api.fail_watch = GooglePushError(500, "backend error")
        logger = MagicMock()
        later = NOW + DRIVE_CHANNEL_LIFETIME - timedelta(minutes=30)
        channels = asyncio.run(drive_manager(api, point, now=later, logger=logger).ensure([DriveWatchTarget("a@x.com", "p2"), DriveWatchTarget("b@x.com", "p")]))
        assert [c.user_email for c in channels] == ["a@x.com"]  # expiring channel kept rather than dropped
        assert logger.warning.call_count == 1
        assert "2 operation(s) failed" in logger.warning.call_args[0][0] % logger.warning.call_args[0][1:]

    def test_remove_all_stops_everything(self) -> None:
        api, point = FakeDrive(), FakeSyncPoint()
        asyncio.run(drive_manager(api, point).ensure([DriveWatchTarget("a@x.com", "p"), DriveWatchTarget("b@x.com", "p")]))
        api.channels.pop(rows(point)[1]["id"])  # already gone on Google's side → 404 is fine
        asyncio.run(drive_manager(api, point).remove_all())
        assert api.channels == {} and rows(point) == []

    def test_sync_hook_never_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class Config:
            async def get_config(self, key: str) -> object:
                return {"scopedJwtSecret": "s"}

        class Broken:
            async def watch(self, *args: object) -> dict[str, Any]:
                raise RuntimeError("impersonation failed")

            async def stop(self, *args: object) -> None:
                raise RuntimeError("impersonation failed")

        point = FakeSyncPoint()
        monkeypatch.setenv("FRONTEND_PUBLIC_URL", "https://dev.edrak.com")
        logger = MagicMock()
        asyncio.run(sync_drive_channels(config_service=Config(), connector_id="c1", sync_point=point, transport=Broken(), targets=[DriveWatchTarget("a@x.com", "p")], logger=logger))
        assert rows(point) == [] and logger.warning.call_count == 1
        monkeypatch.delenv("FRONTEND_PUBLIC_URL")
        api = FakeDrive()
        asyncio.run(sync_drive_channels(config_service=Config(), connector_id="c1", sync_point=point, transport=api, targets=[DriveWatchTarget("a@x.com", "p")], logger=logger))
        assert api.calls == []


class TestGmailWatchManager:
    def test_watch_persists_and_indexes(self) -> None:
        api, point, kv = FakeGmail(), FakeSyncPoint(), FakeKv()
        watches = asyncio.run(gmail_manager(api, point, kv).ensure(["Ali@x.com"]))
        assert api.calls == [("watch", "ali@x.com")]
        assert api.watches["ali@x.com"] == gmail_watch_body(TOPIC)
        assert [w.to_dict() for w in watches] == [{"emailAddress": "ali@x.com", "historyId": "778899", "expiration": ms(NOW + timedelta(days=7))}]
        assert rows(point) == [w.to_dict() for w in watches]
        assert kv.keys == {"cgraph:watch:gmail:ali@x.com": ["conn-1"]}
        # > 24 h left: not re-called; < 24 h left: refreshed
        api.calls.clear()
        asyncio.run(gmail_manager(api, point, kv, now=NOW + timedelta(days=5)).ensure(["ali@x.com"]))
        assert api.calls == []
        asyncio.run(gmail_manager(api, point, kv, now=NOW + timedelta(days=6, hours=1)).ensure(["ali@x.com"]))
        assert api.calls == [("watch", "ali@x.com")]

    def test_reverse_index_shared_between_connectors(self) -> None:
        api, kv = FakeGmail(), FakeKv()
        p1, p2 = FakeSyncPoint(), FakeSyncPoint()
        asyncio.run(gmail_manager(api, p1, kv, connector_id="conn-1").ensure(["a@x.com"]))
        asyncio.run(gmail_manager(api, p2, kv, connector_id="conn-2").ensure(["a@x.com"]))
        assert kv.keys["cgraph:watch:gmail:a@x.com"] == ["conn-1", "conn-2"]
        assert asyncio.run(GmailReverseIndex(kv).lookup("A@x.com")) == ["conn-1", "conn-2"]
        asyncio.run(gmail_manager(api, p1, kv, connector_id="conn-1").remove_all())
        assert kv.keys["cgraph:watch:gmail:a@x.com"] == ["conn-2"]
        assert api.calls[-1] == ("stop", "a@x.com") and rows(p1) == []
        asyncio.run(gmail_manager(api, p2, kv, connector_id="conn-2").remove_all())
        assert kv.keys == {}

    def test_reverse_index_tolerates_json_strings(self) -> None:
        kv = FakeKv()
        kv.keys["cgraph:watch:gmail:a@x.com"] = '["conn-9"]'
        index = GmailReverseIndex(kv)
        assert asyncio.run(index.lookup("a@x.com")) == ["conn-9"]
        asyncio.run(index.add("a@x.com", "conn-9"))  # idempotent
        assert kv.keys["cgraph:watch:gmail:a@x.com"] == '["conn-9"]'
        asyncio.run(index.remove("a@x.com", "missing"))
        assert kv.keys["cgraph:watch:gmail:a@x.com"] == '["conn-9"]'

    def test_watch_failure_is_one_warning_and_keeps_live_watch(self) -> None:
        api, point, kv = FakeGmail(), FakeSyncPoint(), FakeKv()
        asyncio.run(gmail_manager(api, point, kv).ensure(["a@x.com"]))
        api.fail_watch = GooglePushError(403, "insufficient scope")
        logger = MagicMock()
        watches = asyncio.run(gmail_manager(api, point, kv, now=NOW + timedelta(days=6, hours=2), logger=logger).ensure(["a@x.com", "b@x.com"]))
        assert [w.email_address for w in watches] == ["a@x.com"]
        assert logger.warning.call_count == 1
        assert kv.keys == {"cgraph:watch:gmail:a@x.com": ["conn-1"]}

    def test_sync_hook_skips_without_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api, point = FakeGmail(), FakeSyncPoint()
        config = MagicMock(store=FakeKv())
        logger = MagicMock()
        monkeypatch.delenv("GOOGLE_PUBSUB_TOPIC", raising=False)
        asyncio.run(sync_gmail_watches(config_service=config, connector_id="c1", sync_point=point, transport=api, emails=["a@x.com"], logger=logger))
        assert api.calls == [] and point.points == {}
        assert logger.info.call_count == 1 and "GOOGLE_PUBSUB_TOPIC" in logger.info.call_args[0][0] % logger.info.call_args[0][1:]
        monkeypatch.setenv("GOOGLE_PUBSUB_TOPIC", TOPIC)
        asyncio.run(sync_gmail_watches(config_service=config, connector_id="c1", sync_point=point, transport=api, emails=["a@x.com"], logger=logger))
        assert api.calls == [("watch", "a@x.com")]
        assert config.store.keys == {"cgraph:watch:gmail:a@x.com": ["c1"]}
        asyncio.run(remove_gmail_watches(config_service=config, connector_id="c1", sync_point=point, transport=api, logger=logger))
        assert api.calls[-1] == ("stop", "a@x.com") and config.store.keys == {} and rows(point) == []
