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
* Files posted to a channel arrive as ``reference`` attachments pointing at
  SharePoint.  They are listed (name + URL) inside the thread markdown; they are
  **not** emitted as child ``FileRecord`` s — see ``TODO(teams)`` in
  ``connector.py``.

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
| message author                           | nothing beyond the membership edge          | —      |
| team owners (``roles: ["owner"]``)       | same as members (no WRITER/OWNER edges)     | READER |
+------------------------------------------+---------------------------------------------+--------+

Standard channels inherit the team group; private/shared channels get their own
``AppUserGroup``.  When a private channel's membership cannot be read the
channel is skipped rather than falling back to the team group (fail closed).

Microsoft Graph application permissions
=======================================

``REQUIRED_APPLICATION_PERMISSIONS`` must be granted (admin consent) to the app
registration; ``CHAT_APPLICATION_PERMISSIONS`` only when ``include_chats`` is
on.  ``ChannelMessage.Read.All`` and ``Chat.Read.All`` are **protected APIs**:
Microsoft must approve the tenant/app through the "Microsoft Teams protected
APIs" request form before app-only calls stop returning 403
(https://learn.microsoft.com/graph/teams-protected-apis).  The connector logs
that and skips the channel/chat instead of failing the whole sync.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from html.parser import HTMLParser
from typing import Any, Iterable, Mapping, Optional, Sequence
from urllib.parse import quote

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

REQUIRED_APPLICATION_PERMISSIONS: tuple[str, ...] = (
    "Team.ReadBasic.All",
    "Channel.ReadBasic.All",
    "ChannelMessage.Read.All",
    "TeamMember.Read.All",
    "ChannelMember.Read.All",  # private / shared channel rosters
    "User.Read.All",
    "GroupMember.Read.All",    # fallback roster via /groups/{id}/members
)
CHAT_APPLICATION_PERMISSIONS: tuple[str, ...] = ("Chat.Read.All",)
PROTECTED_API_PERMISSIONS: tuple[str, ...] = ("ChannelMessage.Read.All", "Chat.Read.All")
# Personal scope (delegated OAuth, the signed-in user's own chats only). Not protected APIs.
PERSONAL_DELEGATED_PERMISSIONS: tuple[str, ...] = ("Chat.Read", "User.Read", "offline_access")

# Filter keys (sync filters)
TEAMS_FILTER_KEY = "teams"
INCLUDE_PRIVATE_CHANNELS_FILTER_KEY = "include_private_channels"
INCLUDE_CHATS_FILTER_KEY = "include_chats"
CHAT_LOOKBACK_DAYS_FILTER_KEY = "chat_lookback_days"
DEFAULT_CHAT_LOOKBACK_DAYS = 30
MAX_CHAT_LOOKBACK_DAYS = 3650

# External-id prefixes keep GROUP ids and record ids unambiguous in the graph.
TEAM_GROUP_PREFIX = "team:"
CHANNEL_MEMBERS_GROUP_PREFIX = "channel-members:"
CHANNEL_RECORD_GROUP_PREFIX = "channel:"
THREAD_ID_PREFIX = "thread"
CHAT_ID_PREFIX = "chat"
CHATS_RECORD_GROUP_ID = "teams-chats"
CHATS_RECORD_GROUP_NAME = "Teams chats"

CHANNEL_SYNC_POINT_PREFIX = "channel"
CHATS_SYNC_POINT_KEY = "chats"

_TITLE_MAX_CHARS = 80


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


def parse_graph_timestamp(value: Any) -> Optional[int]:
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


def format_timestamp(epoch_ms: Optional[int]) -> str:
    if epoch_ms is None:
        return "n/a"
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def lookback_start_ms(now_ms: int, days: int) -> int:
    return now_ms - int(timedelta(days=days).total_seconds() * 1000)


def resolve_chat_lookback_days(value: Any) -> int:
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


def split_external_id(external_id: str) -> tuple[str, tuple[str, ...]]:
    """Inverse of ``thread_external_id`` / ``chat_external_id``.

    ``thread:<team>/<channel>/<message>`` -> ``("thread", (team, channel, message))``
    ``chat:<chatId>``                     -> ``("chat", (chatId,))``
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
    raise ValueError(f"Unknown Microsoft Teams external id kind: {kind!r}")


def channel_sync_point_key(team_id: str, channel_id: str) -> str:
    return f"{CHANNEL_SYNC_POINT_PREFIX}/{team_id}/{channel_id}"


# ---------------------------------------------------------------------------
# Graph URLs (relative to GRAPH_BASE_URL unless absolute)
# ---------------------------------------------------------------------------


def channel_messages_url(team_id: str, channel_id: str, since_ms: Optional[int] = None) -> str:
    """Plain listing (root messages only).  With ``since_ms`` this is the
    fallback for channels where ``/messages/delta`` is not supported."""
    url = f"teams/{quote(team_id)}/channels/{quote(channel_id, safe='')}/messages?$top={CHANNEL_MESSAGES_PAGE_SIZE}"
    if since_ms is not None:
        url += f"&$filter=lastModifiedDateTime gt {epoch_ms_to_graph(since_ms)}"
    return url


def channel_delta_url(team_id: str, channel_id: str) -> str:
    return f"teams/{quote(team_id)}/channels/{quote(channel_id, safe='')}/messages/delta?$top={CHANNEL_MESSAGES_PAGE_SIZE}"


def channel_message_url(team_id: str, channel_id: str, message_id: str) -> str:
    return f"teams/{quote(team_id)}/channels/{quote(channel_id, safe='')}/messages/{quote(message_id)}"


def message_replies_url(team_id: str, channel_id: str, message_id: str) -> str:
    return f"{channel_message_url(team_id, channel_id, message_id)}/replies?$top={CHANNEL_MESSAGES_PAGE_SIZE}"


def chat_messages_url(chat_id: str, since_ms: Optional[int] = None, top: int = CHATS_PAGE_SIZE) -> str:
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


def should_sync_channel(channel: Mapping[str, Any], include_private_channels: bool) -> bool:
    """Archived channels stay in (their history is still readable in Teams)."""
    if not channel.get("id"):
        return False
    return include_private_channels or not is_private_or_shared_channel(channel)


def select_teams(teams: Iterable[Mapping[str, Any]], allow_list: Optional[Sequence[str]]) -> list[dict[str, Any]]:
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
    email: Optional[str]
    display_name: str
    roles: tuple[str, ...] = ()

    @property
    def is_owner(self) -> bool:
        return "owner" in self.roles


def _clean_email(value: Any) -> Optional[str]:
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
    url: Optional[str]

    @property
    def is_file_reference(self) -> bool:
        return self.content_type == ATTACHMENT_REFERENCE


@dataclass(frozen=True)
class Mention:
    id: Optional[int]
    text: str
    user_id: Optional[str]


@dataclass
class MessageView:
    """A ``chatMessage`` reduced to what rendering and permissions need."""

    id: str
    reply_to_id: Optional[str]
    message_type: str
    deleted: bool
    created_ms: Optional[int]
    modified_ms: Optional[int]
    edited_ms: Optional[int]
    author_id: Optional[str]
    author_name: str
    subject: Optional[str]
    body_text: str
    importance: Optional[str]
    web_url: Optional[str]
    mentions: tuple[Mention, ...] = ()
    reactions: dict[str, int] = field(default_factory=dict)
    attachments: tuple[Attachment, ...] = ()
    team_id: Optional[str] = None
    channel_id: Optional[str] = None
    chat_id: Optional[str] = None

    @property
    def is_indexable(self) -> bool:
        return self.message_type == MESSAGE_TYPE_MESSAGE and not self.deleted

    @property
    def last_activity_ms(self) -> Optional[int]:
        candidates = [t for t in (self.modified_ms, self.edited_ms, self.created_ms) if t is not None]
        return max(candidates) if candidates else None


def _author(raw: Mapping[str, Any]) -> tuple[Optional[str], str]:
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
        body_text=html_to_text(content) if content_type == "html" else str(content).strip(),
        importance=str(raw["importance"]).lower() if raw.get("importance") else None,
        web_url=raw.get("webUrl") or None,
        mentions=_mentions(raw),
        reactions=_reactions(raw),
        attachments=_attachments(raw),
        team_id=str(identity["teamId"]) if identity.get("teamId") else None,
        channel_id=str(identity["channelId"]) if identity.get("channelId") else None,
        chat_id=str(raw["chatId"]) if raw.get("chatId") else None,
    )


# ---------------------------------------------------------------------------
# HTML -> text
# ---------------------------------------------------------------------------

_BLOCK_TAGS = {"p", "div", "br", "li", "ul", "ol", "blockquote", "pre", "table", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "hr"}
_SKIP_TAGS = {"style", "script", "attachment"}


class _TextExtractor(HTMLParser):
    """Minimal Teams-HTML flattener: block tags become newlines, ``<li>`` a
    bullet, ``<a>`` keeps its href, ``<img>``/``<emoji>`` their alt text.
    (BeautifulSoup is available at runtime but keeps this module stdlib-only.)"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0
        self._href: Optional[str] = None
        self._link_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
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

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
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


def html_to_text(html: Optional[str]) -> str:
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
    def last_activity_ms(self) -> Optional[int]:
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
    if len(line) > limit:
        return line[: limit - 1].rstrip() + "…"
    return line


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
        for a in view.attachments:
            lines.append(f"- {a.name} — {a.url}" if a.url else f"- {a.name}")
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


def select_chat_messages(raw_messages: Iterable[Mapping[str, Any]], since_ms: Optional[int]) -> list[MessageView]:
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
    external_id: Optional[str] = None  # GROUP external id
    email: Optional[str] = None        # USER
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


def personal_chat_grants(creator_email: Optional[str]) -> list[PermissionGrant]:
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
    next_link: Optional[str]
    delta_link: Optional[str]


def parse_delta_page(payload: Mapping[str, Any]) -> DeltaPage:
    return DeltaPage(
        items=[dict(v) for v in (payload.get("value") or []) if isinstance(v, Mapping)],
        next_link=payload.get("@odata.nextLink") or None,
        delta_link=payload.get("@odata.deltaLink") or None,
    )


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


def classify_delta_items(items: Iterable[Mapping[str, Any]], changes: Optional[DeltaChanges] = None) -> DeltaChanges:
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


def read_delta_link(sync_point: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not sync_point:
        return None
    value = sync_point.get("deltaLink")
    return str(value) if value else None


def read_last_sync_ms(sync_point: Optional[Mapping[str, Any]]) -> Optional[int]:
    if not sync_point:
        return None
    value = sync_point.get("lastSyncTimestamp")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def delta_sync_point_data(delta_link: Optional[str], synced_at_ms: int) -> dict[str, Any]:
    """Payload stored per channel.  ``deltaLink`` is ``None`` for channels that
    fell back to the ``lastModifiedDateTime`` filter."""
    return {"deltaLink": delta_link, "lastSyncTimestamp": synced_at_ms}


def is_delta_unsupported_status(status_code: int) -> bool:
    """Graph answers 400/404/501 for channel types without delta support."""
    return status_code in (400, 404, 501)
