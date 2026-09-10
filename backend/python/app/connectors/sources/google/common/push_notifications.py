"""Google push notifications ("Layer 2" event-driven sync) — shared helpers.

Mirrors ``microsoft/common/change_notifications.py`` for the four Google connectors.
CGraph is internal-only, so Google never talks to it directly:

* **Drive** — one ``changes.watch`` channel per (connector instance, user) pointing at
  edrak-ai ``{FRONTEND_PUBLIC_URL}/api/webhooks/google/drive/{connectorId}``. The channel
  ``token`` is the shared secret edrak-ai verifies; edrak-ai forwards to the per-connector
  ``/notify`` route with ``source: "google-drive"``.
* **Gmail** — one ``users.watch`` per mailbox publishing to the Cloud Pub/Sub topic in
  ``GOOGLE_PUBSUB_TOPIC``. Pub/Sub messages carry only ``{emailAddress, historyId}``, so a
  reverse index ``cgraph:watch:gmail:<email>`` → ``[connectorId, ...]`` in the KV store lets
  ``/notify-by-resource`` find the connectors to wake.

Everything here degrades to polling: registration failures are logged (one warning per
run, a dedicated one for ``push.webhookUrlUnauthorized``) and never fail a sync.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import quote

from app.config.constants.service import config_node_constants
from app.connectors.core.base.webhooks.subscription_store import (
    SubscriptionStore,
    SyncPointLike,
)
from app.connectors.sources.microsoft.common.change_notifications import (
    ConfigServiceLike,
    public_origin,
    utcnow,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable
    from logging import Logger

CHANNEL_TOKEN_PREFIX = "google-webhook:"
CHANNEL_TOKEN_LENGTH = 40
PUBSUB_TOPIC_ENV = "GOOGLE_PUBSUB_TOPIC"

# Drive caps a changes.watch channel at one day (developers.google.com/workspace/drive/api/guides/push,
# "Renew notification channels"); 23 h leaves room for a late scheduler tick before renewal.
DRIVE_CHANNEL_LIFETIME = timedelta(hours=23)
DRIVE_RENEW_WITHIN = timedelta(hours=2)
# Gmail sets the watch expiry itself (7 days, developers.google.com/workspace/gmail/api/guides/push,
# "Renewing mailbox watch"); re-calling users.watch is idempotent and refreshes it.
GMAIL_WATCH_LIFETIME = timedelta(days=7)
GMAIL_RENEW_WITHIN = timedelta(hours=24)
GMAIL_WATCH_LABEL_IDS = ("INBOX", "SENT")
GMAIL_REVERSE_INDEX_PREFIX = "cgraph:watch:gmail:"
WEBHOOK_URL_UNAUTHORIZED = "push.webhookUrlUnauthorized"


def webhook_channel_token(scoped_jwt_secret: str, connector_id: str) -> str:
    """``hex(HMAC-SHA256(key=scopedJwtSecret, msg="google-webhook:" + connectorId))[:40]``."""
    digest = hmac.new(
        scoped_jwt_secret.encode("utf-8"),
        f"{CHANNEL_TOKEN_PREFIX}{connector_id}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:CHANNEL_TOKEN_LENGTH]


def drive_receiver_url(origin: str, connector_id: str) -> str:
    return f"{origin.strip().rstrip('/')}/api/webhooks/google/drive/{quote(connector_id, safe='')}"


def pubsub_topic() -> str:
    return os.getenv(PUBSUB_TOPIC_ENV, "").strip()


def gmail_reverse_index_key(email_address: str) -> str:
    return f"{GMAIL_REVERSE_INDEX_PREFIX}{email_address.strip().lower()}"


def to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def parse_epoch_ms(value: object) -> int | None:
    """Google returns ``expiration`` as an int64 string of ms; stored rows hold an int."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def is_expiring_ms(expiration: object, within: timedelta, now: datetime) -> bool:
    """True when ``expiration`` is missing, unparsable or inside the renewal window."""
    parsed = parse_epoch_ms(expiration)
    if parsed is None:
        return True
    return parsed <= to_epoch_ms(now + within)


def is_expired_ms(expiration: object, now: datetime) -> bool:
    parsed = parse_epoch_ms(expiration)
    return parsed is not None and parsed <= to_epoch_ms(now)


class GooglePushError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message

    @property
    def is_webhook_unauthorized(self) -> bool:
        return WEBHOOK_URL_UNAUTHORIZED in self.message


def push_error_from(error: Exception) -> GooglePushError:
    """Normalise a googleapiclient ``HttpError`` (duck-typed, no import) or any exception."""
    if isinstance(error, GooglePushError):
        return error
    status = 0
    resp = getattr(error, "resp", None)
    try:
        status = int(getattr(resp, "status", 0) or 0)
    except (TypeError, ValueError):
        status = 0
    message = str(error)
    content = getattr(error, "content", None)
    if isinstance(content, bytes):
        message = f"{message} {content.decode('utf-8', 'replace')}"
    elif isinstance(content, str):
        message = f"{message} {content}"
    return GooglePushError(status, message[:500])


async def scoped_jwt_secret(config_service: ConfigServiceLike) -> str | None:
    secret_keys = await config_service.get_config(config_node_constants.SECRET_KEYS.value)
    secret = (secret_keys or {}).get("scopedJwtSecret") if isinstance(secret_keys, dict) else None
    return str(secret) if secret else None


@dataclass(frozen=True)
class DriveWebhookSettings:
    address: str
    token: str


async def load_drive_webhook_settings(
    config_service: ConfigServiceLike, connector_id: str, logger: Logger
) -> DriveWebhookSettings | None:
    origin = public_origin()
    if not origin:
        logger.info("FRONTEND_PUBLIC_URL is not set; Drive push notifications stay disabled for %s", connector_id)
        return None
    secret = await scoped_jwt_secret(config_service)
    if not secret:
        logger.warning("scopedJwtSecret is not configured; Drive push notifications stay disabled for %s", connector_id)
        return None
    return DriveWebhookSettings(
        address=drive_receiver_url(origin, connector_id),
        token=webhook_channel_token(secret, connector_id),
    )


def kv_store_of(config_service: object) -> KeyValueStoreLike | None:
    """The KV store behind ``ConfigurationService`` — the one the notify route reads."""
    store = getattr(config_service, "store", None)
    return store if hasattr(store, "get_key") else None


# --------------------------------------------------------------------------- Drive


@dataclass
class DriveChannel:
    id: str
    resource_id: str
    expiration: int | None
    user_email: str
    stop_pending: bool = False  # superseded channel whose channels.stop has not succeeded yet

    def to_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "id": self.id,
            "resourceId": self.resource_id,
            "expiration": self.expiration,
            "userEmail": self.user_email,
        }
        if self.stop_pending:
            row["stopPending"] = True
        return row

    @classmethod
    def from_dict(cls, data: object) -> DriveChannel | None:
        if not isinstance(data, dict) or not data.get("id") or not data.get("userEmail"):
            return None
        return cls(
            id=str(data["id"]),
            resource_id=str(data.get("resourceId") or ""),
            expiration=parse_epoch_ms(data.get("expiration")),
            user_email=str(data["userEmail"]).strip().lower(),
            stop_pending=data.get("stopPending") is True,
        )


@dataclass(frozen=True)
class DriveWatchTarget:
    user_email: str
    page_token: str


@dataclass
class DrivePlan:
    create: list[DriveWatchTarget] = field(default_factory=list)
    renew: list[tuple[DriveChannel, DriveWatchTarget]] = field(default_factory=list)
    stop: list[DriveChannel] = field(default_factory=list)
    keep: list[DriveChannel] = field(default_factory=list)


def plan_drive_channels(
    channels: Iterable[DriveChannel],
    targets: Iterable[DriveWatchTarget],
    now: datetime,
    renew_within: timedelta = DRIVE_RENEW_WITHIN,
) -> DrivePlan:
    """Decide per user: create (no live channel), renew (expiring), keep, or stop (pending)."""
    plan = DrivePlan()
    by_email: dict[str, DriveChannel] = {}
    for channel in channels:
        if channel.stop_pending:
            plan.stop.append(channel)
        else:
            by_email[channel.user_email] = channel
    wanted = {target.user_email.strip().lower(): target for target in targets}
    for email, target in wanted.items():
        channel = by_email.pop(email, None)
        if channel is None:
            plan.create.append(target)
        elif is_expiring_ms(channel.expiration, renew_within, now):
            plan.renew.append((channel, target))
        else:
            plan.keep.append(channel)
    # Users this run did not cover: their channels stay until they lapse (no page token to renew with).
    plan.keep.extend(ch for ch in by_email.values() if not is_expired_ms(ch.expiration, now))
    return plan


def drive_channel_body(address: str, token: str, expiration_ms: int, channel_id: str | None = None) -> dict[str, Any]:
    return {
        "id": channel_id or str(uuid.uuid4()),
        "type": "web_hook",
        "address": address,
        "token": token,
        "expiration": str(expiration_ms),
    }


class DriveWatchTransport(Protocol):
    """``changes.watch`` / ``channels.stop`` as the given user (impersonated for team connectors)."""

    async def watch(self, user_email: str, page_token: str, body: dict[str, Any]) -> dict[str, Any]: ...

    async def stop(self, user_email: str, channel_id: str, resource_id: str) -> None: ...


class DriveChannelManager:
    """Keeps one connector's Drive push channels alive, one per user.

    Renewal creates the replacement channel first and only then stops the old one; if
    the stop fails the old row is kept with ``stopPending`` and retried next run so
    edrak-ai never receives notifications from a channel we no longer know.
    """

    def __init__(
        self,
        transport: DriveWatchTransport,
        connector_id: str,
        address: str,
        token: str,
        logger: Logger,
        store: SubscriptionStore | None = None,
        now: Callable[[], datetime] = utcnow,
        lifetime: timedelta = DRIVE_CHANNEL_LIFETIME,
        renew_within: timedelta = DRIVE_RENEW_WITHIN,
    ) -> None:
        self._transport = transport
        self.connector_id = connector_id
        self.address = address
        self.token = token
        self.logger = logger
        self._store = store
        self._now = now
        self._lifetime = lifetime
        self._renew_within = renew_within
        self._channels: list[DriveChannel] | None = None
        self.unauthorized = False
        self._failures: list[str] = []

    async def channels(self) -> list[DriveChannel]:
        if self._channels is None:
            rows = await self._store.load_rows() if self._store is not None else []
            self._channels = [ch for ch in (DriveChannel.from_dict(row) for row in rows) if ch is not None]
        return self._channels

    async def _save(self) -> None:
        if self._store is not None and self._channels is not None:
            await self._store.save_rows([ch.to_dict() for ch in self._channels])

    def _note_failure(self, what: str, error: Exception) -> None:
        self._failures.append(f"{what}: {str(error)[:200]}")

    def _log_unauthorized(self, error: GooglePushError) -> None:
        if self.unauthorized:
            return
        self.unauthorized = True
        self.logger.warning(
            "Google Drive refused the push channel for %s (%s): the notification domain of %s is not verified "
            "for the GCP project that owns this connector's OAuth client / service account. Verify it in Google "
            "Search Console and add it to the OAuth consent screen's authorised domains; polling remains the "
            "fallback. (%s)",
            self.connector_id, WEBHOOK_URL_UNAUTHORIZED, self.address, error.message[:200],
        )

    async def _create(self, target: DriveWatchTarget) -> DriveChannel | None:
        if self.unauthorized:
            return None
        expires = to_epoch_ms(self._now() + self._lifetime)
        body = drive_channel_body(self.address, self.token, expires)
        try:
            created = await self._transport.watch(target.user_email, target.page_token, body)
        except Exception as e:
            error = push_error_from(e)
            if error.is_webhook_unauthorized:
                self._log_unauthorized(error)
            else:
                self._note_failure(f"watch {target.user_email}", error)
            return None
        channel_id = str(created.get("id") or body["id"])
        resource_id = created.get("resourceId")
        if not resource_id:
            self._note_failure(f"watch {target.user_email}", GooglePushError(0, "response has no resourceId"))
        return DriveChannel(
            id=channel_id,
            resource_id=str(resource_id or ""),
            expiration=parse_epoch_ms(created.get("expiration")) or expires,
            user_email=target.user_email.strip().lower(),
        )

    async def _stop(self, channel: DriveChannel) -> bool:
        try:
            await self._transport.stop(channel.user_email, channel.id, channel.resource_id)
        except Exception as e:
            error = push_error_from(e)
            if error.status_code == 404:
                return True
            self._note_failure(f"stop {channel.id}", error)
            return False
        return True

    async def ensure(self, targets: Iterable[DriveWatchTarget]) -> list[DriveChannel]:
        """Create/renew channels for the users this run covered; never raises."""
        plan = plan_drive_channels(await self.channels(), targets, self._now(), self._renew_within)
        result: list[DriveChannel] = list(plan.keep)
        created = renewed = 0
        result.extend([channel for channel in plan.stop if not await self._stop(channel)])
        for target in plan.create:
            channel = await self._create(target)
            if channel is not None:
                result.append(channel)
                created += 1
        for old, target in plan.renew:
            channel = await self._create(target)
            if channel is None:
                result.append(old)  # keep the expiring one; better late notifications than none
                continue
            result.append(channel)
            renewed += 1
            if not is_expired_ms(old.expiration, self._now()) and not await self._stop(old):
                old.stop_pending = True
                result.append(old)
        self._channels = result
        await self._save()
        if created or renewed:
            self.logger.info(
                "Drive push channels for %s: created %d, renewed %d, live %d",
                self.connector_id, created, renewed, len([c for c in result if not c.stop_pending]),
            )
        self._flush_failures()
        return result

    async def remove_all(self) -> None:
        remaining: list[DriveChannel] = []
        for channel in await self.channels():
            if is_expired_ms(channel.expiration, self._now()):
                continue
            if not await self._stop(channel):
                remaining.append(channel)
        self._channels = remaining
        await self._save()
        self._flush_failures()

    def _flush_failures(self) -> None:
        if not self._failures:
            return
        self.logger.warning(
            "Drive push channel upkeep for %s: %d operation(s) failed; first: %s",
            self.connector_id, len(self._failures), self._failures[0],
        )
        self._failures = []


# --------------------------------------------------------------------------- Gmail


@dataclass
class GmailWatch:
    email_address: str
    history_id: str | None
    expiration: int | None

    def to_dict(self) -> dict[str, Any]:
        return {"emailAddress": self.email_address, "historyId": self.history_id, "expiration": self.expiration}

    @classmethod
    def from_dict(cls, data: object) -> GmailWatch | None:
        if not isinstance(data, dict) or not data.get("emailAddress"):
            return None
        history_id = data.get("historyId")
        return cls(
            email_address=str(data["emailAddress"]).strip().lower(),
            history_id=str(history_id) if history_id not in (None, "") else None,
            expiration=parse_epoch_ms(data.get("expiration")),
        )


@dataclass
class GmailPlan:
    refresh: list[str] = field(default_factory=list)
    keep: list[GmailWatch] = field(default_factory=list)
    drop: list[GmailWatch] = field(default_factory=list)


def plan_gmail_watches(
    watches: Iterable[GmailWatch],
    emails: Iterable[str],
    now: datetime,
    renew_within: timedelta = GMAIL_RENEW_WITHIN,
) -> GmailPlan:
    plan = GmailPlan()
    by_email = {watch.email_address: watch for watch in watches}
    for email in {e.strip().lower() for e in emails if e and e.strip()}:
        watch = by_email.pop(email, None)
        if watch is None or is_expiring_ms(watch.expiration, renew_within, now):
            plan.refresh.append(email)
        else:
            plan.keep.append(watch)
    stale = [watch for watch in by_email.values() if is_expired_ms(watch.expiration, now)]
    plan.drop.extend(stale)
    plan.keep.extend(watch for watch in by_email.values() if watch not in stale)
    plan.refresh.sort()
    return plan


def gmail_watch_body(topic: str) -> dict[str, Any]:
    return {"topicName": topic, "labelIds": list(GMAIL_WATCH_LABEL_IDS), "labelFilterBehavior": "INCLUDE"}


class GmailWatchTransport(Protocol):
    """``users.watch`` / ``users.stop`` on the given mailbox (``userId=me`` as that user)."""

    async def watch(self, user_email: str, body: dict[str, Any]) -> dict[str, Any]: ...

    async def stop(self, user_email: str) -> None: ...


class KeyValueStoreLike(Protocol):
    async def get_key(self, key: str) -> object: ...

    async def create_key(self, key: str, value: object, overwrite: bool = True, ttl: int | None = None) -> bool: ...  # noqa: FBT001, FBT002

    async def delete_key(self, key: str) -> bool: ...


def _as_id_list(value: object) -> list[str]:
    if isinstance(value, (bytes, str)):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    if not isinstance(value, list):
        return []
    return [str(v) for v in value if isinstance(v, (str, int)) and str(v)]


class GmailReverseIndex:
    """``cgraph:watch:gmail:<email>`` → JSON list of connector ids (no TTL).

    Read-modify-write without a lock: two connectors watching the same mailbox at
    the same instant could drop each other's id; the next run's ``ensure`` re-adds it.
    """

    def __init__(self, kv_store: KeyValueStoreLike) -> None:
        self._kv = kv_store

    async def lookup(self, email_address: str) -> list[str]:
        return _as_id_list(await self._kv.get_key(gmail_reverse_index_key(email_address)))

    async def add(self, email_address: str, connector_id: str) -> None:
        current = await self.lookup(email_address)
        if connector_id in current:
            return
        await self._kv.create_key(gmail_reverse_index_key(email_address), [*current, connector_id], overwrite=True)

    async def remove(self, email_address: str, connector_id: str) -> None:
        current = await self.lookup(email_address)
        if connector_id not in current:
            return
        remaining = [cid for cid in current if cid != connector_id]
        key = gmail_reverse_index_key(email_address)
        if remaining:
            await self._kv.create_key(key, remaining, overwrite=True)
        else:
            await self._kv.delete_key(key)


class GmailWatchManager:
    """Keeps one connector's Gmail watches (and their reverse-index entries) current."""

    def __init__(
        self,
        transport: GmailWatchTransport,
        connector_id: str,
        topic: str,
        logger: Logger,
        store: SubscriptionStore | None = None,
        reverse_index: GmailReverseIndex | None = None,
        now: Callable[[], datetime] = utcnow,
        renew_within: timedelta = GMAIL_RENEW_WITHIN,
    ) -> None:
        self._transport = transport
        self.connector_id = connector_id
        self.topic = topic
        self.logger = logger
        self._store = store
        self._index = reverse_index
        self._now = now
        self._renew_within = renew_within
        self._watches: list[GmailWatch] | None = None
        self._failures: list[str] = []

    async def watches(self) -> list[GmailWatch]:
        if self._watches is None:
            rows = await self._store.load_rows() if self._store is not None else []
            self._watches = [w for w in (GmailWatch.from_dict(row) for row in rows) if w is not None]
        return self._watches

    async def _save(self) -> None:
        if self._store is not None and self._watches is not None:
            await self._store.save_rows([w.to_dict() for w in self._watches])

    async def _index_add(self, email: str) -> None:
        if self._index is None:
            return
        try:
            await self._index.add(email, self.connector_id)
        except Exception as e:
            self._failures.append(f"index add {email}: {str(e)[:200]}")

    async def _index_remove(self, email: str) -> None:
        if self._index is None:
            return
        try:
            await self._index.remove(email, self.connector_id)
        except Exception as e:
            self._failures.append(f"index remove {email}: {str(e)[:200]}")

    async def ensure(self, emails: Iterable[str]) -> list[GmailWatch]:
        """(Re)issue ``users.watch`` for mailboxes without a watch or with < 24 h left; never raises."""
        plan = plan_gmail_watches(await self.watches(), emails, self._now(), self._renew_within)
        result = list(plan.keep)
        for watch in plan.drop:
            await self._index_remove(watch.email_address)
        refreshed = 0
        for email in plan.refresh:
            try:
                response = await self._transport.watch(email, gmail_watch_body(self.topic))
            except Exception as e:
                self._failures.append(f"watch {email}: {str(push_error_from(e))[:200]}")
                previous = next((w for w in await self.watches() if w.email_address == email), None)
                if previous is not None and not is_expired_ms(previous.expiration, self._now()):
                    result.append(previous)
                continue
            history_id = response.get("historyId")
            result.append(
                GmailWatch(
                    email_address=email,
                    history_id=str(history_id) if history_id not in (None, "") else None,
                    expiration=parse_epoch_ms(response.get("expiration")),
                )
            )
            refreshed += 1
            await self._index_add(email)
        self._watches = result
        await self._save()
        if refreshed:
            self.logger.info("Gmail watches for %s: refreshed %d, live %d", self.connector_id, refreshed, len(result))
        self._flush_failures()
        return result

    async def remove_all(self) -> None:
        for watch in await self.watches():
            if not is_expired_ms(watch.expiration, self._now()):
                try:
                    await self._transport.stop(watch.email_address)
                except Exception as e:
                    error = push_error_from(e)
                    if error.status_code != 404:
                        self._failures.append(f"stop {watch.email_address}: {str(error)[:200]}")
            await self._index_remove(watch.email_address)
        self._watches = []
        await self._save()
        self._flush_failures()

    def _flush_failures(self) -> None:
        if not self._failures:
            return
        self.logger.warning(
            "Gmail watch upkeep for %s: %d operation(s) failed; first: %s",
            self.connector_id, len(self._failures), self._failures[0],
        )
        self._failures = []


# --------------------------------------------------------------------------- datasource transports


class DriveDataSourceLike(Protocol):
    async def changes_watch(self, pageToken: str, **kwargs: object) -> dict[str, Any]: ...

    async def channels_stop(self, **kwargs: object) -> dict[str, Any]: ...


class GmailDataSourceLike(Protocol):
    async def users_watch(self, userId: str, **kwargs: object) -> dict[str, Any]: ...

    async def users_stop(self, userId: str, **kwargs: object) -> dict[str, Any]: ...


class DataSourceDriveTransport:
    """Drive transport over ``GoogleDriveDataSource``; ``datasource_for(email)`` picks the identity."""

    def __init__(self, datasource_for: Callable[[str], Awaitable[DriveDataSourceLike]]) -> None:
        self._datasource_for = datasource_for

    async def watch(self, user_email: str, page_token: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            datasource = await self._datasource_for(user_email)
            return await datasource.changes_watch(
                pageToken=page_token, body=body, supportsAllDrives=True, includeItemsFromAllDrives=True
            )
        except Exception as e:
            raise push_error_from(e) from e

    async def stop(self, user_email: str, channel_id: str, resource_id: str) -> None:
        try:
            datasource = await self._datasource_for(user_email)
            await datasource.channels_stop(body={"id": channel_id, "resourceId": resource_id})
        except Exception as e:
            raise push_error_from(e) from e


class DataSourceGmailTransport:
    def __init__(self, datasource_for: Callable[[str], Awaitable[GmailDataSourceLike]]) -> None:
        self._datasource_for = datasource_for

    async def watch(self, user_email: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            datasource = await self._datasource_for(user_email)
            return await datasource.users_watch(userId="me", body=body)
        except Exception as e:
            raise push_error_from(e) from e

    async def stop(self, user_email: str) -> None:
        try:
            datasource = await self._datasource_for(user_email)
            await datasource.users_stop(userId="me")
        except Exception as e:
            raise push_error_from(e) from e


# --------------------------------------------------------------------------- connector hooks


async def sync_drive_channels(
    *,
    config_service: ConfigServiceLike,
    connector_id: str,
    sync_point: SyncPointLike,
    transport: DriveWatchTransport,
    targets: Iterable[DriveWatchTarget],
    logger: Logger,
) -> None:
    """Post-run hook for the Drive connectors; never raises."""
    targets = list(targets)
    try:
        settings = await load_drive_webhook_settings(config_service, connector_id, logger)
        if settings is None:
            return
        manager = DriveChannelManager(
            transport, connector_id, settings.address, settings.token, logger, store=SubscriptionStore(sync_point)
        )
        await manager.ensure(targets)
    except Exception as e:
        logger.warning("Drive push channel upkeep failed for %s: %s", connector_id, e)


async def remove_drive_channels(
    *,
    connector_id: str,
    sync_point: SyncPointLike,
    transport: DriveWatchTransport,
    logger: Logger,
) -> None:
    """Best-effort teardown on connector delete/disable; never raises."""
    try:
        manager = DriveChannelManager(transport, connector_id, "", "", logger, store=SubscriptionStore(sync_point))
        await manager.remove_all()
    except Exception as e:
        logger.warning("Could not remove Drive push channels for %s: %s", connector_id, e)


async def sync_gmail_watches(
    *,
    config_service: ConfigServiceLike,
    connector_id: str,
    sync_point: SyncPointLike,
    transport: GmailWatchTransport,
    emails: Iterable[str],
    logger: Logger,
) -> None:
    """Post-run hook for the Gmail connectors; skipped (one info log) without ``GOOGLE_PUBSUB_TOPIC``."""
    emails = list(emails)
    try:
        topic = pubsub_topic()
        if not topic:
            logger.info("%s is not set; Gmail push notifications stay disabled for %s", PUBSUB_TOPIC_ENV, connector_id)
            return
        kv_store = kv_store_of(config_service)
        manager = GmailWatchManager(
            transport,
            connector_id,
            topic,
            logger,
            store=SubscriptionStore(sync_point),
            reverse_index=GmailReverseIndex(kv_store) if kv_store is not None else None,
        )
        await manager.ensure(emails)
    except Exception as e:
        logger.warning("Gmail watch upkeep failed for %s: %s", connector_id, e)


async def remove_gmail_watches(
    *,
    config_service: ConfigServiceLike,
    connector_id: str,
    sync_point: SyncPointLike,
    transport: GmailWatchTransport,
    logger: Logger,
) -> None:
    """Best-effort ``users.stop`` + reverse-index cleanup on connector delete/disable; never raises."""
    try:
        kv_store = kv_store_of(config_service)
        manager = GmailWatchManager(
            transport,
            connector_id,
            pubsub_topic(),
            logger,
            store=SubscriptionStore(sync_point),
            reverse_index=GmailReverseIndex(kv_store) if kv_store is not None else None,
        )
        await manager.remove_all()
    except Exception as e:
        logger.warning("Could not remove Gmail watches for %s: %s", connector_id, e)
