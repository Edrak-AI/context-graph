"""Pure helpers for the Microsoft Teams connector (Microsoft Graph).

Everything here is **standard-library only** and free of I/O so message
normalisation, thread rendering, permission derivation and delta-cursor handling
can be unit-tested without the connector's runtime dependencies
(``azure-identity``, ``httpx``, ``msgraph``, pydantic models).  ``connector.py``
turns the plain dataclasses produced here into ``MessageRecord`` /
``Permission`` objects — the same split as ``..dynamics365.mapping``.

What becomes a record
=====================

* One record per **channel message thread**: the root message plus all of its
  replies rendered as markdown (oldest first, newest reply last).  Record group
  = the channel (``"<Team> › <Channel>"``), ``weburl`` = the root message's
  ``webUrl``.
* Optionally (``include_chats`` filter) one record per **1:1 / group chat**
  holding a rolling window of the last ``chat_lookback_days`` days.  Record
  group = ``"Teams chats"``.
* One child ``FileRecord`` per **file shared in a message** (``reference``
  attachments — SharePoint / OneDrive links).  The link is resolved to its
  ``driveItem`` through ``GET /shares/{encoded-url}/driveItem``
  (``shared_drive_item_url``) and the record mirrors the OneDrive connector:
  external id ``file:<driveId>/<itemId>``, the driveItem's mime type / size /
  hashes / ``webUrl``, parent = the thread or chat record, bytes streamed from
  ``@microsoft.graph.downloadUrl`` at indexing time.  Adaptive cards and quoted
  messages (``messageReference``) are never records.
* Optionally (``include_inline_images`` filter) one child ``FileRecord`` per
  **picture pasted into a message** (Teams "hosted content", the
  ``<img src=".../hostedContents/{id}/$value">`` tags in the HTML body).  Off by
  default because every image goes through OCR.

Permission model (Teams membership -> CGraph permission edges)
=============================================================

Teams has no per-message ACL: whoever can see the channel can read every
message in it, and search has no notion of "owner" for a chat message.  So
every edge is READER and derived from membership only:

+------------------------------------------+---------------------------------------------+--------+
| Teams source                             | CGraph principal                            | Role   |
+==========================================+=============================================+========+
| message in a *standard* channel          | GROUP ``team:<teamId>``                     | READER |
|                                          | (members of ``/teams/{id}/members``)        |        |
| message in a *private* or *shared*       | GROUP ``channel-members:<teamId>/<chanId>`` | READER |
| channel                                  | (members of ``/teams/{id}/channels/{cid}/   |        |
|                                          | members``)                                  |        |
| message in a 1:1 / group chat            | USER per chat participant (by email)        | READER |
| file / image attached to a message       | exactly the grants of its parent record     | READER |
|                                          | (the SharePoint ACL is *not* consulted)     |        |
| message author                           | nothing beyond the membership edge          | —      |
| team owners (``roles: ["owner"]``)       | same as members (no WRITER/OWNER edges)     | READER |
+------------------------------------------+---------------------------------------------+--------+

Standard channels inherit the team group; private/shared channels get their own
``AppUserGroup``.  When a private channel's membership cannot be read the
channel is skipped rather than falling back to the team group (fail closed).

Incremental sync model
======================

* Channel ``/messages/delta`` (deltaLink persisted per channel) yields **root
  messages only** — new, edited, reacted-to and soft-deleted roots.  A reply
  does not change its root's ``lastModifiedDateTime``, so after the delta pass
  the connector pages ``/messages?$expand=replies``, which Graph sorts by the
  last-modified time of the *entire reply chain*, and rebuilds every thread with
  activity after the previous run (``classify_listing_items``).  The same
  listing is the fallback for channels without delta support (the plain listing
  supports no ``$filter``).
* Chats: ``/chats/{id}/messages?$filter=lastModifiedDateTime gt <cursor>``;
  edits, reactions and soft deletes all bump ``lastModifiedDateTime`` so the
  rolling record is re-rendered; a window with no indexable message left
  deletes the record and its files.

Microsoft Graph permissions
===========================

``REQUIRED_APPLICATION_PERMISSIONS`` must be granted (admin consent) to the app
registration; ``CHAT_APPLICATION_PERMISSIONS`` only when ``include_chats`` is
on.  ``ChannelMessage.Read.All`` and ``Chat.Read.All`` are **protected APIs**:
Microsoft must approve the tenant/app through the "Microsoft Teams protected
APIs" request form before app-only calls stop returning 403
(https://learn.microsoft.com/graph/teams-protected-apis).  The connector logs
that and skips the channel/chat instead of failing the whole sync.
``FILE_APPLICATION_PERMISSIONS`` (``Files.Read.All``) resolves shared files;
without it attachments are skipped (logged once) and messages still index.
Personal scope needs delegated ``Files.Read`` for the same reason.
"""

from __future__ import annotations

import base64
import mimetypes
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from html.parser import HTMLParser
from typing import Any
from urllib.parse import quote, urlsplit

# ---------------------------------------------------------------------------
# Graph constants
# ---------------------------------------------------------------------------

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0/"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

# Graph caps $top at 50 for channel messages, replies and chat messages.
CHANNEL_MESSAGES_PAGE_SIZE = 50
CHATS_PAGE_SIZE = 50

MEMBERSHIP_STANDARD = "standard"
MEMBERSHIP_PRIVATE = "private"
MEMBERSHIP_SHARED = "shared"

MESSAGE_TYPE_MESSAGE = "message"  # other values: systemEventMessage, chatEvent, typing, unknownFutureValue

ATTACHMENT_REFERENCE = "reference"                # SharePoint / OneDrive file link
ATTACHMENT_MESSAGE_REFERENCE = "messageReference"  # quoted message
ATTACHMENT_CARD_PREFIX = "application/vnd.microsoft.card."

# Pictures pasted into a message are served by Graph itself; the metadata
# endpoint never reports a content type, and Teams stores them as PNG.
HOSTED_CONTENTS_SEGMENT = "/hostedContents/"
HOSTED_CONTENT_VALUE_SUFFIX = "/$value"
HOSTED_IMAGE_MIME_TYPE = "image/png"
HOSTED_IMAGE_EXTENSION = "png"
GRAPH_HOSTS = ("graph.microsoft.com",)

FILE_APPLICATION_PERMISSIONS: tuple[str, ...] = ("Files.Read.All",)  # /shares/{token}/driveItem
REQUIRED_APPLICATION_PERMISSIONS: tuple[str, ...] = (
    "Team.ReadBasic.All",
    "Channel.ReadBasic.All",
    "ChannelMessage.Read.All",
    "TeamMember.Read.All",
    "ChannelMember.Read.All",  # private / shared channel rosters
    "User.Read.All",
    "GroupMember.Read.All",    # fallback roster via /groups/{id}/members
    *FILE_APPLICATION_PERMISSIONS,
)
CHAT_APPLICATION_PERMISSIONS: tuple[str, ...] = ("Chat.Read.All",)
PROTECTED_API_PERMISSIONS: tuple[str, ...] = ("ChannelMessage.Read.All", "Chat.Read.All")
# Personal scope (delegated OAuth, the signed-in user's own chats only). Not protected APIs.
# Files.Read resolves files shared in those chats (/shares/{token}/driveItem).
PERSONAL_DELEGATED_PERMISSIONS: tuple[str, ...] = ("Chat.Read", "Files.Read", "User.Read", "offline_access")

# Filter keys (sync filters)
TEAMS_FILTER_KEY = "teams"
INCLUDE_PRIVATE_CHANNELS_FILTER_KEY = "include_private_channels"
INCLUDE_CHATS_FILTER_KEY = "include_chats"
INCLUDE_INLINE_IMAGES_FILTER_KEY = "include_inline_images"
CHAT_LOOKBACK_DAYS_FILTER_KEY = "chat_lookback_days"
DEFAULT_CHAT_LOOKBACK_DAYS = 30
MAX_CHAT_LOOKBACK_DAYS = 3650

# External-id prefixes keep GROUP ids and record ids unambiguous in the graph.
TEAM_GROUP_PREFIX = "team:"
CHANNEL_MEMBERS_GROUP_PREFIX = "channel-members:"
CHANNEL_RECORD_GROUP_PREFIX = "channel:"
THREAD_ID_PREFIX = "thread"
CHAT_ID_PREFIX = "chat"
FILE_ID_PREFIX = "file"      # file:<driveId>/<itemId>  (shared SharePoint / OneDrive file)
HOSTED_ID_PREFIX = "hosted"  # hosted:<graph path of the hostedContents item>
CHATS_RECORD_GROUP_ID = "teams-chats"
CHATS_RECORD_GROUP_NAME = "Teams chats"

CHANNEL_SYNC_POINT_PREFIX = "channel"
CHATS_SYNC_POINT_KEY = "chats"

_TITLE_MAX_CHARS = 80


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


def parse_graph_timestamp(value: object) -> int | None:
    """Graph returns ISO-8601 UTC with up to 7 fractional digits
    (``2024-05-01T10:15:30.1234567Z``); return epoch milliseconds."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    # datetime.fromisoformat accepts at most 6 fractional digits
    if "." in text:
        head, _, tail = text.partition(".")
        digits = ""
        rest = tail
        for i, ch in enumerate(tail):
            if not ch.isdigit():
                digits, rest = tail[:i], tail[i:]
                break
        else:
            digits, rest = tail, ""
        text = f"{head}.{(digits[:6] or '0')}{rest}"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_graph(epoch_ms: int) -> str:
    """ISO literal accepted by Graph ``$filter=lastModifiedDateTime gt ...``."""
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_timestamp(epoch_ms: int | None) -> str:
    if epoch_ms is None:
        return "n/a"
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def lookback_start_ms(now_ms: int, days: int) -> int:
    return now_ms - int(timedelta(days=days).total_seconds() * 1000)


def resolve_chat_lookback_days(value: object) -> int:
    """Clamp the ``chat_lookback_days`` filter to ``1..MAX_CHAT_LOOKBACK_DAYS``;
    anything unparsable falls back to the default."""
    try:
        days = int(float(value))
    except (TypeError, ValueError):
        return DEFAULT_CHAT_LOOKBACK_DAYS
    if days <= 0:
        return DEFAULT_CHAT_LOOKBACK_DAYS
    return min(days, MAX_CHAT_LOOKBACK_DAYS)


# ---------------------------------------------------------------------------
# External ids
# ---------------------------------------------------------------------------


def team_group_external_id(team_id: str) -> str:
    return f"{TEAM_GROUP_PREFIX}{team_id}"


def channel_members_group_external_id(team_id: str, channel_id: str) -> str:
    return f"{CHANNEL_MEMBERS_GROUP_PREFIX}{team_id}/{channel_id}"


def channel_record_group_external_id(team_id: str, channel_id: str) -> str:
    return f"{CHANNEL_RECORD_GROUP_PREFIX}{team_id}/{channel_id}"


def thread_external_id(team_id: str, channel_id: str, message_id: str) -> str:
    return f"{THREAD_ID_PREFIX}:{team_id}/{channel_id}/{message_id}"


def chat_external_id(chat_id: str) -> str:
    return f"{CHAT_ID_PREFIX}:{chat_id}"


def file_external_id(drive_id: str, item_id: str) -> str:
    """Shared file identity = the driveItem, so the same file posted twice is one record."""
    return f"{FILE_ID_PREFIX}:{drive_id}/{item_id}"


def hosted_external_id(graph_path: str) -> str:
    return f"{HOSTED_ID_PREFIX}:{graph_path}"


def split_external_id(external_id: str) -> tuple[str, tuple[str, ...]]:
    """Inverse of the ``*_external_id`` builders.

    ``thread:<team>/<channel>/<message>`` -> ``("thread", (team, channel, message))``
    ``chat:<chatId>``                     -> ``("chat", (chatId,))``
    ``file:<driveId>/<itemId>``           -> ``("file", (driveId, itemId))``
    ``hosted:<graph path>``               -> ``("hosted", (graph path,))``
    """
    kind, sep, rest = external_id.partition(":")
    if not sep or not rest:
        raise ValueError(f"Not a Microsoft Teams external id: {external_id!r}")
    if kind == THREAD_ID_PREFIX:
        parts = tuple(rest.split("/", 2))
        if len(parts) != 3 or not all(parts):
            raise ValueError(f"Malformed Teams thread id: {external_id!r}")
        return kind, parts
    if kind == CHAT_ID_PREFIX:
        return kind, (rest,)
    if kind == FILE_ID_PREFIX:
        parts = tuple(rest.split("/", 1))
        if len(parts) != 2 or not all(parts):
            raise ValueError(f"Malformed Teams file id: {external_id!r}")
        return kind, parts
    if kind == HOSTED_ID_PREFIX:
        return kind, (rest,)
    raise ValueError(f"Unknown Microsoft Teams external id kind: {kind!r}")


def channel_sync_point_key(team_id: str, channel_id: str) -> str:
    return f"{CHANNEL_SYNC_POINT_PREFIX}/{team_id}/{channel_id}"


# ---------------------------------------------------------------------------
# Graph URLs (relative to GRAPH_BASE_URL unless absolute)
# ---------------------------------------------------------------------------


def channel_messages_url(team_id: str, channel_id: str, *, expand_replies: bool = False) -> str:
    """Root-message listing, sorted by Graph on the last-modified time of the
    whole reply chain (newest first).  It supports no ``$filter``; with
    ``expand_replies`` each root carries up to 200 replies inline plus a
    ``replies@odata.nextLink`` for the rest (``expanded_replies``)."""
    url = f"teams/{quote(team_id)}/channels/{quote(channel_id, safe='')}/messages?$top={CHANNEL_MESSAGES_PAGE_SIZE}"
    if expand_replies:
        url += "&$expand=replies"
    return url


def channel_delta_url(team_id: str, channel_id: str) -> str:
    return f"teams/{quote(team_id)}/channels/{quote(channel_id, safe='')}/messages/delta?$top={CHANNEL_MESSAGES_PAGE_SIZE}"


def channel_message_url(team_id: str, channel_id: str, message_id: str) -> str:
    return f"teams/{quote(team_id)}/channels/{quote(channel_id, safe='')}/messages/{quote(message_id)}"


def message_replies_url(team_id: str, channel_id: str, message_id: str) -> str:
    return f"{channel_message_url(team_id, channel_id, message_id)}/replies?$top={CHANNEL_MESSAGES_PAGE_SIZE}"


def chat_messages_url(chat_id: str, since_ms: int | None = None, top: int = CHATS_PAGE_SIZE) -> str:
    url = f"chats/{quote(chat_id, safe='')}/messages?$top={top}"
    if since_ms is not None:
        url += f"&$filter=lastModifiedDateTime gt {epoch_ms_to_graph(since_ms)}"
    return url


def user_chats_url(user_id: str) -> str:
    """App-only Graph cannot list ``/chats`` tenant-wide; chats are discovered per user."""
    return f"users/{quote(user_id)}/chats?$expand=members&$top={CHATS_PAGE_SIZE}"


def me_chats_url() -> str:
    """Personal scope: the signed-in user's chats through the delegated token (``Chat.Read``)."""
    return f"me/chats?$expand=members&$top={CHATS_PAGE_SIZE}"


def encode_sharing_url(url: str) -> str:
    """Graph sharing token for a URL: ``"u!" + unpadded base64url(url)``
    (https://learn.microsoft.com/graph/api/shares-get#encoding-sharing-urls)."""
    token = base64.b64encode(url.strip().encode("utf-8")).decode("ascii")
    return "u!" + token.rstrip("=").replace("/", "_").replace("+", "-")


def shared_drive_item_url(content_url: str) -> str:
    """``driveItem`` behind a SharePoint / OneDrive link (a ``reference`` attachment's ``contentUrl``)."""
    return f"shares/{encode_sharing_url(content_url)}/driveItem"


def drive_item_url(drive_id: str, item_id: str) -> str:
    """Same GET the OneDrive connector uses to read ``@microsoft.graph.downloadUrl``.
    Drive ids look like ``b!Z3Jv...`` and item ids are base64url — URL-safe as-is."""
    return f"drives/{drive_id}/items/{item_id}"


def hosted_content_value_url(graph_path: str) -> str:
    return f"{graph_path}{HOSTED_CONTENT_VALUE_SUFFIX}"


def graph_relative_path(url: str | None) -> str | None:
    """``https://graph.microsoft.com/v1.0/teams/...?x=y`` -> ``teams/...``; ``None`` for anything else."""
    if not url or not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if parts.scheme != "https" or parts.netloc.lower() not in GRAPH_HOSTS:
        return None
    segments = parts.path.split("/", 2)  # ["", "v1.0" | "beta", "rest"]
    if len(segments) < 3 or segments[1] not in ("v1.0", "beta") or not segments[2]:
        return None
    return segments[2]


# ---------------------------------------------------------------------------
# Teams / channels / members
# ---------------------------------------------------------------------------


def team_display_name(team: Mapping[str, Any]) -> str:
    return str(team.get("displayName") or team.get("id") or "Team")


def channel_display_name(channel: Mapping[str, Any]) -> str:
    return str(channel.get("displayName") or channel.get("id") or "Channel")


def channel_membership_type(channel: Mapping[str, Any]) -> str:
    return str(channel.get("membershipType") or MEMBERSHIP_STANDARD).lower()


def is_private_or_shared_channel(channel: Mapping[str, Any]) -> bool:
    return channel_membership_type(channel) in (MEMBERSHIP_PRIVATE, MEMBERSHIP_SHARED)


def should_sync_channel(channel: Mapping[str, Any], *, include_private_channels: bool) -> bool:
    """Archived channels stay in (their history is still readable in Teams)."""
    if not channel.get("id"):
        return False
    return include_private_channels or not is_private_or_shared_channel(channel)


def select_teams(teams: Iterable[Mapping[str, Any]], allow_list: Sequence[str] | None) -> list[dict[str, Any]]:
    """Apply the ``teams`` allow-list (ids or display names, case-insensitive).
    Empty / ``None`` means every team the app can see."""
    rows = [dict(t) for t in teams if t.get("id")]
    if not allow_list:
        return rows
    wanted = {str(v).strip().lower() for v in allow_list if v}
    if not wanted:
        return rows
    return [t for t in rows if str(t["id"]).lower() in wanted or team_display_name(t).lower() in wanted]


def channel_record_group_name(team_name: str, channel_name: str) -> str:
    return f"{team_name} › {channel_name}"


@dataclass(frozen=True)
class Member:
    """A conversation member (team, channel or chat) resolved to a directory user."""

    user_id: str
    email: str | None
    display_name: str
    roles: tuple[str, ...] = ()

    @property
    def is_owner(self) -> bool:
        return "owner" in self.roles


def _clean_email(value: object) -> str | None:
    if value and isinstance(value, str) and "@" in value:
        return value.strip().lower()
    return None


def parse_conversation_members(rows: Iterable[Mapping[str, Any]]) -> list[Member]:
    """``/teams/{id}/members``, ``/teams/{id}/channels/{cid}/members`` and
    ``chat.members`` all return ``aadUserConversationMember`` rows."""
    members: list[Member] = []
    seen: set[str] = set()
    for row in rows:
        user_id = row.get("userId")
        if not user_id or str(user_id) in seen:
            continue
        seen.add(str(user_id))
        members.append(Member(
            user_id=str(user_id),
            email=_clean_email(row.get("email")),
            display_name=str(row.get("displayName") or row.get("email") or user_id),
            roles=tuple(str(r).lower() for r in (row.get("roles") or [])),
        ))
    return members


def parse_group_members(rows: Iterable[Mapping[str, Any]]) -> list[Member]:
    """``/groups/{id}/members`` fallback (``directoryObject`` rows; only users count)."""
    members: list[Member] = []
    seen: set[str] = set()
    for row in rows:
        odata_type = str(row.get("@odata.type") or "")
        if odata_type and odata_type != "#microsoft.graph.user":
            continue
        user_id = row.get("id")
        if not user_id or str(user_id) in seen:
            continue
        seen.add(str(user_id))
        members.append(Member(
            user_id=str(user_id),
            email=_clean_email(row.get("mail")) or _clean_email(row.get("userPrincipalName")),
            display_name=str(row.get("displayName") or row.get("mail") or user_id),
        ))
    return members


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Attachment:
    id: str
    name: str
    content_type: str
    url: str | None

    @property
    def is_file_reference(self) -> bool:
        return self.content_type == ATTACHMENT_REFERENCE and bool(self.url)

    @property
    def is_card(self) -> bool:
        return self.content_type.startswith(ATTACHMENT_CARD_PREFIX)


@dataclass(frozen=True)
class HostedImage:
    """A picture pasted into a message body: ``<img src="https://graph.microsoft.com/v1.0/<graph_path>/$value">``."""

    graph_path: str  # e.g. teams/<t>/channels/<c>/messages/<m>/hostedContents/<id>
    alt: str | None = None

    @property
    def hosted_id(self) -> str:
        return self.graph_path.rsplit("/", 1)[-1]

    def file_name(self) -> str:
        """Stable, extension-bearing name for the ``FileRecord``."""
        alt = " ".join((self.alt or "").split())
        stem = alt if alt and alt.lower() != "image" else f"image-{self.hosted_id[-12:]}"
        return stem if stem.lower().endswith(f".{HOSTED_IMAGE_EXTENSION}") else f"{stem}.{HOSTED_IMAGE_EXTENSION}"


@dataclass(frozen=True)
class Mention:
    id: int | None
    text: str
    user_id: str | None


@dataclass
class MessageView:
    """A ``chatMessage`` reduced to what rendering and permissions need."""

    id: str
    reply_to_id: str | None
    message_type: str
    deleted: bool
    created_ms: int | None
    modified_ms: int | None
    edited_ms: int | None
    author_id: str | None
    author_name: str
    subject: str | None
    body_text: str
    importance: str | None
    web_url: str | None
    mentions: tuple[Mention, ...] = ()
    reactions: dict[str, int] = field(default_factory=dict)
    attachments: tuple[Attachment, ...] = ()
    hosted_images: tuple[HostedImage, ...] = ()
    team_id: str | None = None
    channel_id: str | None = None
    chat_id: str | None = None

    @property
    def is_indexable(self) -> bool:
        return self.message_type == MESSAGE_TYPE_MESSAGE and not self.deleted

    @property
    def last_activity_ms(self) -> int | None:
        candidates = [t for t in (self.modified_ms, self.edited_ms, self.created_ms) if t is not None]
        return max(candidates) if candidates else None


def _author(raw: Mapping[str, Any]) -> tuple[str | None, str]:
    sender = raw.get("from") or {}
    user = sender.get("user") or {}
    if user.get("id") or user.get("displayName"):
        return (str(user["id"]) if user.get("id") else None), str(user.get("displayName") or "Unknown user")
    app = sender.get("application") or {}
    if app.get("displayName") or app.get("id"):
        return None, f"{app.get('displayName') or 'App'} (app)"
    return None, "Unknown sender"


def _mentions(raw: Mapping[str, Any]) -> tuple[Mention, ...]:
    out: list[Mention] = []
    for m in raw.get("mentions") or []:
        mentioned = (m.get("mentioned") or {}).get("user") or {}
        try:
            mention_id = int(m["id"]) if m.get("id") is not None else None
        except (TypeError, ValueError):
            mention_id = None
        out.append(Mention(
            id=mention_id,
            text=str(m.get("mentionText") or mentioned.get("displayName") or ""),
            user_id=str(mentioned["id"]) if mentioned.get("id") else None,
        ))
    return tuple(out)


def _reactions(raw: Mapping[str, Any]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for r in raw.get("reactions") or []:
        kind = str(r.get("reactionType") or "").strip().lower()
        if not kind:
            continue
        summary[kind] = summary.get(kind, 0) + 1
    return summary


def _attachments(raw: Mapping[str, Any]) -> tuple[Attachment, ...]:
    out: list[Attachment] = []
    for a in raw.get("attachments") or []:
        content_type = str(a.get("contentType") or "")
        if content_type == ATTACHMENT_MESSAGE_REFERENCE:
            continue  # quoted message: its text is already indexed on its own thread
        name = a.get("name")
        if not name and content_type.startswith(ATTACHMENT_CARD_PREFIX):
            name = f"card ({content_type[len(ATTACHMENT_CARD_PREFIX):]})"
        out.append(Attachment(
            id=str(a.get("id") or ""),
            name=str(name or a.get("contentUrl") or "attachment"),
            content_type=content_type,
            url=a.get("contentUrl") or None,
        ))
    return tuple(out)


def normalize_message(raw: Mapping[str, Any]) -> MessageView:
    body = raw.get("body") or {}
    content = body.get("content") or ""
    content_type = str(body.get("contentType") or "html").lower()
    is_html = content_type == "html"
    author_id, author_name = _author(raw)
    identity = raw.get("channelIdentity") or {}
    return MessageView(
        id=str(raw.get("id") or ""),
        reply_to_id=str(raw["replyToId"]) if raw.get("replyToId") else None,
        message_type=str(raw.get("messageType") or MESSAGE_TYPE_MESSAGE),
        deleted=bool(raw.get("deletedDateTime")),
        created_ms=parse_graph_timestamp(raw.get("createdDateTime")),
        modified_ms=parse_graph_timestamp(raw.get("lastModifiedDateTime")),
        edited_ms=parse_graph_timestamp(raw.get("lastEditedDateTime")),
        author_id=author_id,
        author_name=author_name,
        subject=(str(raw["subject"]).strip() or None) if raw.get("subject") else None,
        body_text=html_to_text(content) if is_html else str(content).strip(),
        importance=str(raw["importance"]).lower() if raw.get("importance") else None,
        web_url=raw.get("webUrl") or None,
        mentions=_mentions(raw),
        reactions=_reactions(raw),
        attachments=_attachments(raw),
        hosted_images=hosted_images_in_html(content) if is_html else (),
        team_id=str(identity["teamId"]) if identity.get("teamId") else None,
        channel_id=str(identity["channelId"]) if identity.get("channelId") else None,
        chat_id=str(raw["chatId"]) if raw.get("chatId") else None,
    )


# ---------------------------------------------------------------------------
# Files shared in messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FileInfo:
    """The ``driveItem`` behind a shared file, reduced to what a ``FileRecord`` needs
    (same fields the OneDrive connector reads off its kiota ``DriveItem``)."""

    drive_id: str
    item_id: str
    name: str
    mime_type: str
    size: int | None
    web_url: str | None
    etag: str | None
    ctag: str | None
    created_ms: int | None
    modified_ms: int | None
    quick_xor_hash: str | None = None
    crc32_hash: str | None = None
    sha1_hash: str | None = None
    sha256_hash: str | None = None
    download_url: str | None = None

    @property
    def external_id(self) -> str:
        return file_external_id(self.drive_id, self.item_id)


def drive_item_file_info(item: Mapping[str, Any]) -> FileInfo | None:
    """``None`` for folders, packages (OneNote notebooks) and anything without a
    drive/item identity — those cannot be streamed as one file."""
    if not isinstance(item, Mapping) or item.get("folder") is not None or item.get("package") is not None:
        return None
    item_id = item.get("id")
    parent = item.get("parentReference") or {}
    drive_id = parent.get("driveId")
    if not item_id or not drive_id:
        return None
    name = str(item.get("name") or item_id)
    file_facet = item.get("file") or {}
    mime_type = str(file_facet.get("mimeType") or "").split(";", 1)[0].strip().lower()
    if not mime_type:
        guessed, _ = mimetypes.guess_type(name)
        mime_type = guessed or "application/octet-stream"
    hashes = file_facet.get("hashes") or {}
    size = item.get("size")
    return FileInfo(
        drive_id=str(drive_id),
        item_id=str(item_id),
        name=name,
        mime_type=mime_type,
        size=int(size) if isinstance(size, (int, float)) and not isinstance(size, bool) else None,
        web_url=item.get("webUrl") or None,
        etag=item.get("eTag") or None,
        ctag=item.get("cTag") or None,
        created_ms=parse_graph_timestamp(item.get("createdDateTime")),
        modified_ms=parse_graph_timestamp(item.get("lastModifiedDateTime")),
        quick_xor_hash=hashes.get("quickXorHash") or None,
        crc32_hash=hashes.get("crc32Hash") or None,
        sha1_hash=hashes.get("sha1Hash") or None,
        sha256_hash=hashes.get("sha256Hash") or None,
        download_url=item.get("@microsoft.graph.downloadUrl") or None,
    )


def file_attachments(messages: Iterable[MessageView]) -> list[Attachment]:
    """``reference`` attachments of the given messages, first occurrence per URL wins."""
    out: list[Attachment] = []
    seen: set[str] = set()
    for view in messages:
        for attachment in view.attachments:
            if not attachment.is_file_reference or attachment.url in seen:
                continue
            seen.add(attachment.url or "")
            out.append(attachment)
    return out


def skipped_attachments(messages: Iterable[MessageView]) -> list[Attachment]:
    """Attachments that never become records (cards, tabs, meetings...) — for debug logging."""
    return [a for view in messages for a in view.attachments if not a.is_file_reference]


def hosted_images(messages: Iterable[MessageView]) -> list[HostedImage]:
    out: list[HostedImage] = []
    seen: set[str] = set()
    for view in messages:
        for image in view.hosted_images:
            if image.graph_path in seen:
                continue
            seen.add(image.graph_path)
            out.append(image)
    return out


class _HostedImageExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.images: list[HostedImage] = []
        self._seen: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "img":
            return
        attributes = dict(attrs)
        path = graph_relative_path(attributes.get("src"))
        if not path or HOSTED_CONTENTS_SEGMENT not in path or not path.endswith(HOSTED_CONTENT_VALUE_SUFFIX):
            return
        path = path[: -len(HOSTED_CONTENT_VALUE_SUFFIX)]
        if path in self._seen:
            return
        self._seen.add(path)
        self.images.append(HostedImage(graph_path=path, alt=(attributes.get("alt") or "").strip() or None))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)


def hosted_images_in_html(html: str | None) -> tuple[HostedImage, ...]:
    if not html:
        return ()
    parser = _HostedImageExtractor()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return ()
    return tuple(parser.images)


# ---------------------------------------------------------------------------
# HTML -> text
# ---------------------------------------------------------------------------

_BLOCK_TAGS = {"p", "div", "br", "li", "ul", "ol", "blockquote", "pre", "table", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "hr"}
_SKIP_TAGS = {"style", "script", "attachment"}


class _TextExtractor(HTMLParser):
    """Minimal Teams-HTML flattener: block tags become newlines, ``<li>`` a
    bullet, ``<a>`` keeps its href, ``<img>``/``<emoji>`` their alt text.
    Text is passed through verbatim (Unicode, bidi controls and Arabic
    presentation forms included); only ASCII whitespace runs are collapsed.
    (BeautifulSoup is available at runtime but keeps this module stdlib-only.)"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0
        self._href: str | None = None
        self._link_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "li":
            self.parts.append("\n- ")
        elif tag == "blockquote":
            self.parts.append("\n> ")
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")
        if tag == "a":
            self._href = attributes.get("href")
            self._link_text = []
        elif tag in ("img", "emoji"):
            alt = attributes.get("alt") or attributes.get("title")
            self.parts.append(alt if alt else ("[image]" if tag == "img" else ""))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in ("img", "emoji", "br", "hr"):
            self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP_TAGS:
            self._skip_depth = max(self._skip_depth - 1, 0)
            return
        if self._skip_depth:
            return
        if tag == "a":
            text = "".join(self._link_text).strip()
            href = self._href
            self._href = None
            if href and text and href.strip() != text and not href.startswith(("mailto:", "#")):
                self.parts.append(f"{text} ({href})")
            else:
                self.parts.append(text or (href or ""))
            self._link_text = []
        elif tag in _BLOCK_TAGS and tag not in ("br", "hr", "li"):
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if self._href is not None:
            self._link_text.append(data)
        else:
            self.parts.append(data)

    def text(self) -> str:
        raw = "".join(self.parts)
        lines = [" ".join(line.split()) for line in raw.replace("\r", "").split("\n")]
        out: list[str] = []
        blank = 0
        for line in lines:
            if not line:
                blank += 1
                if blank > 1:
                    continue
            else:
                blank = 0
            out.append(line)
        return "\n".join(out).strip()


def html_to_text(html: str | None) -> str:
    if not html:
        return ""
    parser = _TextExtractor()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return " ".join(str(html).split())
    return parser.text()


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------


@dataclass
class Thread:
    root: MessageView
    replies: list[MessageView] = field(default_factory=list)

    @property
    def messages(self) -> list[MessageView]:
        return [self.root, *self.replies]

    @property
    def last_activity_ms(self) -> int | None:
        candidates = [m.last_activity_ms for m in self.messages if m.last_activity_ms is not None]
        return max(candidates) if candidates else None


def _sort_key(view: MessageView) -> tuple[int, str]:
    return (view.created_ms if view.created_ms is not None else 0, view.id)


def build_thread(root_raw: Mapping[str, Any], reply_raws: Iterable[Mapping[str, Any]]) -> Thread:
    """Normalise a root message and its replies; drop deleted / system replies and
    order them oldest first so the newest reply is rendered last."""
    root = normalize_message(root_raw)
    replies = [normalize_message(r) for r in reply_raws]
    replies = [r for r in replies if r.is_indexable]
    replies.sort(key=_sort_key)
    return Thread(root=root, replies=replies)


def _first_line(text: str, limit: int = _TITLE_MAX_CHARS) -> str:
    line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    if len(line) <= limit:
        return line
    cut = limit - 1
    # never separate a base letter from its combining marks (Arabic tashkeel, etc.)
    while cut > 0 and unicodedata.combining(line[cut]):
        cut -= 1
    return line[:cut].rstrip() + "…"


def thread_title(thread: Thread, channel_name: str) -> str:
    root = thread.root
    if root.subject:
        return _first_line(root.subject)
    body = _first_line(root.body_text)
    if body:
        return body
    return f"{root.author_name} in {channel_name}"


def thread_revision(thread: Thread) -> str:
    """Changes whenever the root is edited or the reply set changes."""
    last = thread.last_activity_ms or 0
    return f"{last}:{len(thread.replies)}"


def thread_participant_ids(thread: Thread) -> list[str]:
    """Ordered, de-duplicated author ids (for ``involved_user_source_ids``)."""
    out: list[str] = []
    for m in thread.messages:
        if m.author_id and m.author_id not in out:
            out.append(m.author_id)
    return out


def thread_mentioned_user_ids(thread: Thread) -> list[str]:
    out: list[str] = []
    for m in thread.messages:
        for mention in m.mentions:
            if mention.user_id and mention.user_id not in out:
                out.append(mention.user_id)
    return out


def render_message_block(view: MessageView, heading: str) -> list[str]:
    lines = [f"{heading} {view.author_name} · {format_timestamp(view.created_ms)}"]
    meta: list[str] = []
    if view.edited_ms is not None:
        meta.append("edited")
    if view.importance and view.importance != "normal":
        meta.append(f"importance: {view.importance}")
    if meta:
        lines[0] += f" ({', '.join(meta)})"
    lines.append("")
    if view.subject:
        lines.append(f"**{view.subject}**")
        lines.append("")
    lines.append(view.body_text if view.body_text else "_(no text)_")
    if view.mentions:
        names = [m.text for m in view.mentions if m.text]
        if names:
            lines.append("")
            lines.append(f"Mentions: {', '.join(names)}")
    if view.attachments:
        lines.append("")
        lines.append("Attachments:")
        lines.extend(f"- {a.name} — {a.url}" if a.url else f"- {a.name}" for a in view.attachments)
    if view.reactions:
        lines.append("")
        lines.append("Reactions: " + ", ".join(f"{kind} ×{count}" for kind, count in sorted(view.reactions.items())))
    lines.append("")
    return lines


def render_thread_markdown(team_name: str, channel_name: str, thread: Thread) -> str:
    title = thread_title(thread, channel_name)
    lines: list[str] = [f"# {title}", ""]
    lines.append(f"**Microsoft Teams** · {channel_record_group_name(team_name, channel_name)} · {len(thread.replies)} repl{'y' if len(thread.replies) == 1 else 'ies'}")
    lines.append("")
    lines.extend(render_message_block(thread.root, "##"))
    if thread.replies:
        lines.append("## Replies")
        lines.append("")
        for reply in thread.replies:
            lines.extend(render_message_block(reply, "###"))
    lines.append("## Source metadata")
    lines.append(f"- Team: {team_name}")
    lines.append(f"- Channel: {channel_name}")
    lines.append(f"- Message id: {thread.root.id}")
    lines.append(f"- Last activity: {format_timestamp(thread.last_activity_ms)}")
    if thread.root.web_url:
        lines.append(f"- URL: {thread.root.web_url}")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Chats
# ---------------------------------------------------------------------------


def chat_title(chat: Mapping[str, Any], members: Sequence[Member]) -> str:
    topic = chat.get("topic")
    if topic and str(topic).strip():
        return _first_line(str(topic))
    names = [m.display_name for m in members if m.display_name]
    if not names:
        return f"Chat {chat.get('id', '')}".strip()
    if len(names) > 4:
        names = names[:4] + [f"+{len(names) - 4} more"]
    kind = "Chat" if str(chat.get("chatType") or "") != "meeting" else "Meeting chat"
    return f"{kind}: {', '.join(names)}"


def select_chat_messages(raw_messages: Iterable[Mapping[str, Any]], since_ms: int | None) -> list[MessageView]:
    """Rolling window: indexable messages created at/after ``since_ms``, oldest first."""
    views = [normalize_message(r) for r in raw_messages]
    views = [v for v in views if v.is_indexable and (since_ms is None or (v.created_ms or 0) >= since_ms)]
    views.sort(key=_sort_key)
    return views


def chat_revision(messages: Sequence[MessageView]) -> str:
    last = max((m.last_activity_ms or 0 for m in messages), default=0)
    return f"{last}:{len(messages)}"


def render_chat_markdown(chat: Mapping[str, Any], members: Sequence[Member], messages: Sequence[MessageView], lookback_days: int) -> str:
    title = chat_title(chat, members)
    lines: list[str] = [f"# {title}", ""]
    lines.append(f"**Microsoft Teams chat** · {chat.get('chatType') or 'chat'} · last {lookback_days} days · {len(messages)} message{'s' if len(messages) != 1 else ''}")
    lines.append("")
    if members:
        lines.append("Participants: " + ", ".join(
            f"{m.display_name} ({m.email})" if m.email else m.display_name for m in members
        ))
        lines.append("")
    if messages:
        lines.append("## Messages")
        lines.append("")
        for view in messages:
            lines.extend(render_message_block(view, "###"))
    else:
        lines.append("_No messages in the lookback window._")
        lines.append("")
    lines.append("## Source metadata")
    lines.append(f"- Chat id: {chat.get('id', '')}")
    if messages:
        lines.append(f"- Window: {format_timestamp(messages[0].created_ms)} → {format_timestamp(messages[-1].created_ms)}")
    if chat.get("webUrl"):
        lines.append(f"- URL: {chat['webUrl']}")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


class GrantRole(str, Enum):
    READER = "READER"


class GrantEntity(str, Enum):
    USER = "USER"
    GROUP = "GROUP"


@dataclass(frozen=True)
class PermissionGrant:
    """Connector-agnostic permission; ``connector.py`` turns it into ``Permission``."""

    entity_type: GrantEntity
    role: GrantRole
    external_id: str | None = None  # GROUP external id
    email: str | None = None        # USER
    reason: str = field(default="", compare=False)


def channel_grants(team_id: str, channel: Mapping[str, Any]) -> list[PermissionGrant]:
    """Apply the permission table from the module docstring to a channel."""
    channel_id = str(channel.get("id") or "")
    if is_private_or_shared_channel(channel):
        return [PermissionGrant(
            GrantEntity.GROUP, GrantRole.READER,
            external_id=channel_members_group_external_id(team_id, channel_id),
            reason=f"{channel_membership_type(channel)} channel members",
        )]
    return [PermissionGrant(
        GrantEntity.GROUP, GrantRole.READER,
        external_id=team_group_external_id(team_id),
        reason="team members (standard channel)",
    )]


def chat_grants(members: Iterable[Member]) -> list[PermissionGrant]:
    """Every chat participant with a resolvable email is a READER; nothing else."""
    grants: list[PermissionGrant] = []
    seen: set[str] = set()
    for member in members:
        if not member.email or member.email in seen:
            continue
        seen.add(member.email)
        grants.append(PermissionGrant(GrantEntity.USER, GrantRole.READER, email=member.email, reason="chat participant"))
    return grants


def personal_chat_grants(creator_email: str | None) -> list[PermissionGrant]:
    """Personal scope: the connector creator is the only READER of every chat record
    (and of the chats record group); participants are never granted anything.
    Empty when the creator email is unknown so the caller fails closed."""
    email = _clean_email(creator_email)
    if not email:
        return []
    return [PermissionGrant(GrantEntity.USER, GrantRole.READER, email=email, reason="personal connector creator")]


# ---------------------------------------------------------------------------
# Delta handling
# ---------------------------------------------------------------------------


@dataclass
class DeltaPage:
    items: list[dict[str, Any]]
    next_link: str | None
    delta_link: str | None


def parse_delta_page(payload: Mapping[str, Any]) -> DeltaPage:
    """Also used for the plain listing (which never carries a ``@odata.deltaLink``)."""
    return DeltaPage(
        items=[dict(v) for v in (payload.get("value") or []) if isinstance(v, Mapping)],
        next_link=payload.get("@odata.nextLink") or None,
        delta_link=payload.get("@odata.deltaLink") or None,
    )


def expanded_replies(root: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    """Inline ``replies`` of a ``$expand=replies`` root plus the ``replies@odata.nextLink``
    Graph adds when the thread has more than one page (200) of replies."""
    replies = [dict(r) for r in (root.get("replies") or []) if isinstance(r, Mapping)]
    return replies, root.get("replies@odata.nextLink") or None


def reply_chain_last_modified_ms(root: Mapping[str, Any], replies: Iterable[Mapping[str, Any]]) -> int:
    """Newest ``lastModifiedDateTime`` / ``deletedDateTime`` across root and replies (0 if none) —
    the key Graph sorts the channel listing by."""
    latest = 0
    for message in (root, *replies):
        for key in ("lastModifiedDateTime", "deletedDateTime", "lastEditedDateTime", "createdDateTime"):
            ts = parse_graph_timestamp(message.get(key))
            if ts is not None and ts > latest:
                latest = ts
    return latest


@dataclass
class ListingChanges:
    """One page of the reply-chain-sorted channel listing, classified.

    ``threads``: ``(root, inline replies, replies@odata.nextLink)`` to rebuild;
    ``deleted_root_ids``: roots that are gone (soft-deleted or non-message);
    ``stale``: the page reached a chain older than ``since_ms`` — later pages are older still.
    """

    threads: list[tuple[dict[str, Any], list[dict[str, Any]], str | None]] = field(default_factory=list)
    deleted_root_ids: list[str] = field(default_factory=list)
    stale: bool = False


def classify_listing_items(
    items: Iterable[Mapping[str, Any]],
    since_ms: int | None,
    skip_root_ids: Iterable[str] = (),
) -> ListingChanges:
    """Fold one listing page (``channel_messages_url(expand_replies=True)``).

    Roots already handled by the delta pass (``skip_root_ids``) are ignored but
    still count for the stop condition.  ``since_ms=None`` classifies everything
    (full sync of a channel without delta support).
    """
    skip = {str(s) for s in skip_root_ids}
    changes = ListingChanges()
    for item in items:
        root_id = item.get("id")
        if not root_id:
            continue
        root_id = str(root_id)
        replies, more = expanded_replies(item)
        if since_ms is not None and reply_chain_last_modified_ms(item, replies) <= since_ms:
            changes.stale = True
            break
        if root_id in skip:
            continue
        view = normalize_message(item)
        if not view.is_indexable:
            if view.deleted or view.message_type != MESSAGE_TYPE_MESSAGE:
                changes.deleted_root_ids.append(root_id)
            continue
        changes.threads.append((dict(item), replies, more))
    return changes


@dataclass
class DeltaChanges:
    """Root message ids whose thread must be (re)built, and roots that were deleted."""

    dirty_root_ids: list[str] = field(default_factory=list)
    deleted_root_ids: list[str] = field(default_factory=list)
    # root id -> the raw root payload when the delta page carried it (saves a GET)
    roots: dict[str, dict[str, Any]] = field(default_factory=dict)

    def _mark_dirty(self, root_id: str) -> None:
        if root_id in self.deleted_root_ids:
            return
        if root_id not in self.dirty_root_ids:
            self.dirty_root_ids.append(root_id)

    def _mark_deleted(self, root_id: str) -> None:
        if root_id in self.dirty_root_ids:
            self.dirty_root_ids.remove(root_id)
        self.roots.pop(root_id, None)
        if root_id not in self.deleted_root_ids:
            self.deleted_root_ids.append(root_id)


def classify_delta_items(items: Iterable[Mapping[str, Any]], changes: DeltaChanges | None = None) -> DeltaChanges:
    """Fold one delta page into ``changes``.

    * a reply (``replyToId`` set) — added, edited **or deleted** — dirties its
      root so the whole thread is re-rendered;
    * a root with ``deletedDateTime`` is a deletion;
    * any other root is dirty (new or edited).
    """
    changes = changes or DeltaChanges()
    for item in items:
        message_id = item.get("id")
        if not message_id:
            continue
        message_id = str(message_id)
        reply_to = item.get("replyToId")
        if reply_to:
            changes._mark_dirty(str(reply_to))
        elif item.get("deletedDateTime"):
            changes._mark_deleted(message_id)
        else:
            changes._mark_dirty(message_id)
            changes.roots[message_id] = dict(item)
    return changes


def read_delta_link(sync_point: Mapping[str, Any] | None) -> str | None:
    if not sync_point:
        return None
    value = sync_point.get("deltaLink")
    return str(value) if value else None


def read_last_sync_ms(sync_point: Mapping[str, Any] | None) -> int | None:
    if not sync_point:
        return None
    value = sync_point.get("lastSyncTimestamp")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def delta_sync_point_data(delta_link: str | None, synced_at_ms: int) -> dict[str, Any]:
    """Payload stored per channel.  ``deltaLink`` is ``None`` for channels that
    fell back to the ``lastModifiedDateTime`` filter."""
    return {"deltaLink": delta_link, "lastSyncTimestamp": synced_at_ms}


def is_delta_unsupported_status(status_code: int) -> bool:
    """Graph answers 400/404/501 for channel types without delta support."""
    return status_code in (400, 404, 501)
