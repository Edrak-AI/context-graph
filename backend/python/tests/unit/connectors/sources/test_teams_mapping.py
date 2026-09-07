"""Tests for app.connectors.sources.microsoft.teams.mapping.

Pure functions only (message normalisation, HTML flattening, thread rendering,
permission derivation, delta-cursor handling, shared-file / hosted-image
resolution helpers, reply-sweep classification) — no network, no pydantic, no
azure/httpx/msgraph.  Fixtures are shaped like real Graph payloads.  Written
pytest-style but runnable with plain
``python3 tests/unit/connectors/sources/test_teams_mapping.py`` as well.
"""

import base64
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager

from app.connectors.sources.microsoft.teams.mapping import (
    CHAT_APPLICATION_PERMISSIONS,
    DEFAULT_CHAT_LOOKBACK_DAYS,
    FILE_APPLICATION_PERMISSIONS,
    HOSTED_IMAGE_MIME_TYPE,
    MAX_CHAT_LOOKBACK_DAYS,
    PERSONAL_DELEGATED_PERMISSIONS,
    PROTECTED_API_PERMISSIONS,
    REQUIRED_APPLICATION_PERMISSIONS,
    DeltaChanges,
    FileInfo,
    GrantEntity,
    GrantRole,
    HostedImage,
    Member,
    PermissionGrant,
    build_thread,
    channel_delta_url,
    channel_grants,
    channel_members_group_external_id,
    channel_messages_url,
    channel_record_group_external_id,
    channel_record_group_name,
    channel_sync_point_key,
    chat_external_id,
    chat_grants,
    chat_messages_url,
    chat_revision,
    chat_title,
    classify_delta_items,
    classify_listing_items,
    delta_sync_point_data,
    drive_item_file_info,
    drive_item_url,
    encode_sharing_url,
    epoch_ms_to_graph,
    expanded_replies,
    file_attachments,
    file_external_id,
    graph_relative_path,
    hosted_content_value_url,
    hosted_external_id,
    hosted_images,
    hosted_images_in_html,
    html_to_text,
    is_delta_unsupported_status,
    is_private_or_shared_channel,
    lookback_start_ms,
    me_chats_url,
    message_replies_url,
    normalize_message,
    parse_conversation_members,
    parse_delta_page,
    parse_graph_timestamp,
    parse_group_members,
    personal_chat_grants,
    read_delta_link,
    read_last_sync_ms,
    render_chat_markdown,
    render_thread_markdown,
    reply_chain_last_modified_ms,
    resolve_chat_lookback_days,
    select_chat_messages,
    select_teams,
    shared_drive_item_url,
    should_sync_channel,
    skipped_attachments,
    split_external_id,
    team_group_external_id,
    thread_external_id,
    thread_mentioned_user_ids,
    thread_participant_ids,
    thread_revision,
    thread_title,
    user_chats_url,
)

TEAM = "11111111-1111-1111-1111-111111111111"
CHANNEL = "19:abc123@thread.tacv2"
ALICE = "aaaaaaaa-0000-0000-0000-000000000001"
BOB = "bbbbbbbb-0000-0000-0000-000000000002"
CAROL = "cccccccc-0000-0000-0000-000000000003"


@contextmanager
def raises(exc_type: type[BaseException]) -> Iterator[None]:
    try:
        yield
    except exc_type:
        return
    raise AssertionError(f"expected {exc_type.__name__}")


def _msg(msg_id: str, author_id: str, author_name: str, created: str, body_html: str, **overrides) -> dict:
    row = {
        "id": msg_id,
        "replyToId": None,
        "etag": msg_id,
        "messageType": "message",
        "createdDateTime": created,
        "lastModifiedDateTime": created,
        "lastEditedDateTime": None,
        "deletedDateTime": None,
        "subject": None,
        "importance": "normal",
        "webUrl": f"https://teams.microsoft.com/l/message/{CHANNEL}/{msg_id}",
        "from": {"user": {"id": author_id, "displayName": author_name, "userIdentityType": "aadUser"}},
        "body": {"contentType": "html", "content": body_html},
        "channelIdentity": {"teamId": TEAM, "channelId": CHANNEL},
        "attachments": [],
        "mentions": [],
        "reactions": [],
    }
    row.update(overrides)
    return row


ROOT = _msg(
    "1700000000001", ALICE, "Alice Adams", "2024-05-01T10:15:30.1234567Z",
    '<div>Release <b>1.2</b> is ready.<br>Please review <a href="https://contoso.sharepoint.com/sites/eng/Shared%20Documents/notes.docx">notes.docx</a>'
    ' <at id="0">Bob Brown</at>&nbsp;<attachment id="f1"></attachment></div>',
    subject="Release 1.2",
    importance="high",
    mentions=[{"id": 0, "mentionText": "Bob Brown", "mentioned": {"user": {"id": BOB, "displayName": "Bob Brown"}}}],
    reactions=[
        {"reactionType": "like", "user": {"user": {"id": BOB}}},
        {"reactionType": "like", "user": {"user": {"id": CAROL}}},
        {"reactionType": "heart", "user": {"user": {"id": CAROL}}},
    ],
    attachments=[
        {"id": "f1", "contentType": "reference", "name": "notes.docx",
         "contentUrl": "https://contoso.sharepoint.com/sites/eng/Shared%20Documents/notes.docx"},
        {"id": "q1", "contentType": "messageReference", "name": None, "content": "{\"messagePreview\":\"old\"}"},
        {"id": "c1", "contentType": "application/vnd.microsoft.card.adaptive", "name": None, "contentUrl": None},
    ],
)
REPLY_LATE = _msg("1700000000300", CAROL, "Carol Cruz", "2024-05-01T12:00:00Z", "<p>LGTM 🚀</p>", replyToId=ROOT["id"],
                  lastEditedDateTime="2024-05-01T12:05:00Z", lastModifiedDateTime="2024-05-01T12:05:00Z")
REPLY_EARLY = _msg("1700000000200", BOB, "Bob Brown", "2024-05-01T11:00:00Z", "<p>Looking now</p>", replyToId=ROOT["id"])
REPLY_DELETED = _msg("1700000000250", BOB, "Bob Brown", "2024-05-01T11:30:00Z", "", replyToId=ROOT["id"],
                     deletedDateTime="2024-05-01T11:31:00Z")
REPLY_SYSTEM = _msg("1700000000260", ALICE, "Alice Adams", "2024-05-01T11:40:00Z", "", replyToId=ROOT["id"],
                    messageType="systemEventMessage")


class TestTimestamps:
    def test_parse_graph_timestamp_handles_seven_fractional_digits(self) -> None:
        assert parse_graph_timestamp("2024-05-01T10:15:30.1234567Z") == 1714558530123
        assert parse_graph_timestamp("2024-05-01T10:15:30Z") == 1714558530000
        assert parse_graph_timestamp("2024-05-01T10:15:30.5+02:00") == 1714551330500
        assert parse_graph_timestamp(None) is None
        assert parse_graph_timestamp("not a date") is None

    def test_epoch_ms_to_graph_roundtrip(self) -> None:
        assert epoch_ms_to_graph(1714558530000) == "2024-05-01T10:15:30Z"
        assert parse_graph_timestamp(epoch_ms_to_graph(1714558530999)) == 1714558530000

    def test_lookback(self) -> None:
        now = 1_800_000_000_000
        assert lookback_start_ms(now, 1) == now - 86_400_000
        assert resolve_chat_lookback_days(None) == DEFAULT_CHAT_LOOKBACK_DAYS
        assert resolve_chat_lookback_days("abc") == DEFAULT_CHAT_LOOKBACK_DAYS
        assert resolve_chat_lookback_days(0) == DEFAULT_CHAT_LOOKBACK_DAYS
        assert resolve_chat_lookback_days(7.0) == 7
        assert resolve_chat_lookback_days("14") == 14
        assert resolve_chat_lookback_days(10**9) == MAX_CHAT_LOOKBACK_DAYS


class TestExternalIds:
    def test_thread_and_chat_ids_roundtrip(self) -> None:
        ext = thread_external_id(TEAM, CHANNEL, "1700000000001")
        assert ext == f"thread:{TEAM}/{CHANNEL}/1700000000001"
        assert split_external_id(ext) == ("thread", (TEAM, CHANNEL, "1700000000001"))
        assert split_external_id(chat_external_id("19:xyz@unq.gbl.spaces")) == ("chat", ("19:xyz@unq.gbl.spaces",))

    def test_split_rejects_foreign_ids(self) -> None:
        for bad in ("", "thread:", "thread:a/b", "opportunity:guid", "nope", "file:", "file:drive-only", "hosted:"):
            with raises(ValueError):
                split_external_id(bad)

    def test_file_and_hosted_ids_roundtrip(self) -> None:
        drive = "b!Z3JvdXBzaXRl-drive"
        item = "01BYE5RZ6QN3ZWBTUFOFD3GSPGOHDJD36K"
        assert file_external_id(drive, item) == f"file:{drive}/{item}"
        assert split_external_id(file_external_id(drive, item)) == ("file", (drive, item))
        path = f"teams/{TEAM}/channels/{CHANNEL}/messages/1616963377068/hostedContents/aWQ9eF8wLXd1cy1kMS02"
        assert split_external_id(hosted_external_id(path)) == ("hosted", (path,))
        assert hosted_content_value_url(path) == path + "/$value"
        assert drive_item_url(drive, item) == f"drives/{drive}/items/{item}"

    def test_group_and_sync_point_ids(self) -> None:
        assert team_group_external_id(TEAM) == f"team:{TEAM}"
        assert channel_members_group_external_id(TEAM, CHANNEL) == f"channel-members:{TEAM}/{CHANNEL}"
        assert channel_record_group_external_id(TEAM, CHANNEL) == f"channel:{TEAM}/{CHANNEL}"
        assert channel_sync_point_key(TEAM, CHANNEL) == f"channel/{TEAM}/{CHANNEL}"
        assert channel_record_group_name("Engineering", "General") == "Engineering › General"


class TestUrls:
    def test_channel_urls_encode_channel_id_and_cap_top_at_50(self) -> None:
        url = channel_messages_url(TEAM, CHANNEL)
        assert url == f"teams/{TEAM}/channels/19%3Aabc123%40thread.tacv2/messages?$top=50"
        # the channel listing supports no $filter at all (Graph 400s); replies come inline instead
        assert "$filter" not in channel_messages_url(TEAM, CHANNEL, expand_replies=True)
        assert channel_messages_url(TEAM, CHANNEL, expand_replies=True).endswith("?$top=50&$expand=replies")
        assert channel_delta_url(TEAM, CHANNEL).endswith("/messages/delta?$top=50")
        assert message_replies_url(TEAM, CHANNEL, "42").endswith("/messages/42/replies?$top=50")

    def test_chat_urls(self) -> None:
        assert chat_messages_url("19:x@unq.gbl.spaces") == "chats/19%3Ax%40unq.gbl.spaces/messages?$top=50"
        assert "$filter=lastModifiedDateTime gt" in chat_messages_url("c", since_ms=0)
        assert user_chats_url(ALICE) == f"users/{ALICE}/chats?$expand=members&$top=50"


class TestHtmlToText:
    def test_blocks_links_mentions_and_entities(self) -> None:
        text = html_to_text(ROOT["body"]["content"])
        assert text == (
            "Release 1.2 is ready.\n"
            "Please review notes.docx (https://contoso.sharepoint.com/sites/eng/Shared%20Documents/notes.docx) Bob Brown"
        )

    def test_lists_quotes_images_and_scripts(self) -> None:
        html = (
            '<p>Agenda</p><ul><li>one</li><li>two &amp; three</li></ul>'
            '<blockquote>quoted</blockquote><img alt="chart.png" src="x"><img src="y">'
            '<emoji alt="😀" title="grinning"></emoji><style>p{}</style><script>alert(1)</script>'
            '<a href="mailto:a@b.c">a@b.c</a> <a href="https://x.y">https://x.y</a>'
        )
        assert html_to_text(html) == (
            "Agenda\n\n- one\n- two & three\n\n> quoted\nchart.png[image]😀a@b.c https://x.y"
        )

    def test_empty_and_plain(self) -> None:
        assert html_to_text(None) == ""
        assert html_to_text("") == ""
        assert html_to_text("just   text\n\n\n\nmore") == "just text\n\nmore"


class TestNormalizeMessage:
    def test_root_message_fields(self) -> None:
        view = normalize_message(ROOT)
        assert view.id == "1700000000001"
        assert view.reply_to_id is None
        assert view.is_indexable
        assert view.author_id == ALICE and view.author_name == "Alice Adams"
        assert view.subject == "Release 1.2"
        assert view.importance == "high"
        assert view.created_ms == 1714558530123
        assert view.team_id == TEAM and view.channel_id == CHANNEL and view.chat_id is None
        assert [m.user_id for m in view.mentions] == [BOB]
        assert view.reactions == {"like": 2, "heart": 1}
        # quoted messages are dropped; cards get a synthetic name; file references keep their URL
        assert [(a.name, a.is_file_reference) for a in view.attachments] == [
            ("notes.docx", True), ("card (adaptive)", False),
        ]
        assert view.attachments[0].url.endswith("notes.docx")

    def test_text_body_deleted_and_system_messages(self) -> None:
        plain = normalize_message(_msg("1", ALICE, "Alice", "2024-05-01T10:00:00Z", "  hi  ", body={"contentType": "text", "content": "  hi  "}))
        assert plain.body_text == "hi"
        assert not normalize_message(REPLY_DELETED).is_indexable
        assert normalize_message(REPLY_DELETED).deleted
        assert not normalize_message(REPLY_SYSTEM).is_indexable

    def test_application_and_unknown_senders(self) -> None:
        bot = normalize_message(_msg("2", ALICE, "x", "2024-05-01T10:00:00Z", "hi", **{"from": {"application": {"id": "app", "displayName": "Planner"}}}))
        assert bot.author_id is None and bot.author_name == "Planner (app)"
        anon = normalize_message(_msg("3", ALICE, "x", "2024-05-01T10:00:00Z", "hi", **{"from": None}))
        assert anon.author_name == "Unknown sender"

    def test_last_activity_prefers_latest_timestamp(self) -> None:
        view = normalize_message(REPLY_LATE)
        assert view.edited_ms == parse_graph_timestamp("2024-05-01T12:05:00Z")
        assert view.last_activity_ms == view.edited_ms


class TestThreads:
    def test_build_thread_orders_replies_and_drops_noise(self) -> None:
        thread = build_thread(ROOT, [REPLY_LATE, REPLY_SYSTEM, REPLY_EARLY, REPLY_DELETED])
        assert [r.id for r in thread.replies] == [REPLY_EARLY["id"], REPLY_LATE["id"]]
        assert thread.last_activity_ms == parse_graph_timestamp("2024-05-01T12:05:00Z")
        assert thread_participant_ids(thread) == [ALICE, BOB, CAROL]
        assert thread_mentioned_user_ids(thread) == [BOB]

    def test_thread_title_falls_back_to_body_then_author(self) -> None:
        assert thread_title(build_thread(ROOT, []), "General") == "Release 1.2"
        no_subject = _msg("9", ALICE, "Alice Adams", "2024-05-01T10:00:00Z", "<p>" + "x" * 100 + "</p><p>second</p>")
        title = thread_title(build_thread(no_subject, []), "General")
        assert title.endswith("…") and len(title) == 80
        empty = _msg("10", ALICE, "Alice Adams", "2024-05-01T10:00:00Z", "<img src='only'>", body={"contentType": "html", "content": ""})
        assert thread_title(build_thread(empty, []), "General") == "Alice Adams in General"

    def test_thread_revision_changes_with_replies_and_edits(self) -> None:
        base = thread_revision(build_thread(ROOT, [REPLY_EARLY]))
        assert base == f"{parse_graph_timestamp('2024-05-01T11:00:00Z')}:1"
        assert thread_revision(build_thread(ROOT, [REPLY_EARLY, REPLY_LATE])) != base
        assert thread_revision(build_thread(ROOT, [REPLY_EARLY, REPLY_DELETED])) == base

    def test_render_thread_markdown(self) -> None:
        md = render_thread_markdown("Engineering", "General", build_thread(ROOT, [REPLY_LATE, REPLY_EARLY]))
        lines = md.splitlines()
        assert lines[0] == "# Release 1.2"
        assert lines[2] == "**Microsoft Teams** · Engineering › General · 2 replies"
        assert "## Alice Adams · 2024-05-01 10:15 UTC (importance: high)" in lines
        assert "**Release 1.2**" in lines
        assert "Release 1.2 is ready." in lines
        assert "Mentions: Bob Brown" in lines
        assert "- notes.docx — https://contoso.sharepoint.com/sites/eng/Shared%20Documents/notes.docx" in lines
        assert "- card (adaptive)" in lines
        assert "Reactions: heart ×1, like ×2" in lines
        # replies: oldest first, newest last, edited flag on the late one
        bob = lines.index("### Bob Brown · 2024-05-01 11:00 UTC")
        carol = lines.index("### Carol Cruz · 2024-05-01 12:00 UTC (edited)")
        assert lines.index("## Replies") < bob < carol < lines.index("## Source metadata")
        assert lines[-1] == f"- URL: {ROOT['webUrl']}"
        assert md.endswith("\n") and "\n\n\n" not in md

    def test_render_singular_reply_and_no_text(self) -> None:
        md = render_thread_markdown("T", "C", build_thread(ROOT, [REPLY_EARLY]))
        assert "· 1 reply" in md
        md = render_thread_markdown("T", "C", build_thread(_msg("5", ALICE, "A", "2024-05-01T10:00:00Z", ""), []))
        assert "_(no text)_" in md and "## Replies" not in md


class TestMembers:
    def test_parse_conversation_members_dedupes_and_lowercases(self) -> None:
        rows = [
            {"@odata.type": "#microsoft.graph.aadUserConversationMember", "id": "m1", "roles": ["owner"],
             "displayName": "Alice Adams", "userId": ALICE, "email": "Alice@Contoso.com"},
            {"id": "m2", "roles": [], "displayName": "Bob Brown", "userId": BOB, "email": None},
            {"id": "m3", "roles": [], "displayName": "Alice dup", "userId": ALICE, "email": "alice@contoso.com"},
            {"id": "m4", "roles": [], "displayName": "Bot", "userId": None},
        ]
        members = parse_conversation_members(rows)
        assert members == [
            Member(ALICE, "alice@contoso.com", "Alice Adams", ("owner",)),
            Member(BOB, None, "Bob Brown", ()),
        ]
        assert members[0].is_owner and not members[1].is_owner

    def test_parse_group_members_skips_non_users(self) -> None:
        rows = [
            {"@odata.type": "#microsoft.graph.user", "id": ALICE, "displayName": "Alice", "mail": None, "userPrincipalName": "alice@contoso.com"},
            {"@odata.type": "#microsoft.graph.group", "id": "g1", "displayName": "Nested group"},
            {"id": BOB, "displayName": "Bob", "mail": "BOB@contoso.com"},
        ]
        assert parse_group_members(rows) == [
            Member(ALICE, "alice@contoso.com", "Alice"),
            Member(BOB, "bob@contoso.com", "Bob"),
        ]


class TestTeamsAndChannels:
    TEAMS = [
        {"id": "t1", "displayName": "Engineering"},
        {"id": "t2", "displayName": "Sales"},
        {"displayName": "no id"},
    ]

    def test_select_teams(self) -> None:
        assert [t["id"] for t in select_teams(self.TEAMS, None)] == ["t1", "t2"]
        assert [t["id"] for t in select_teams(self.TEAMS, [])] == ["t1", "t2"]
        assert [t["id"] for t in select_teams(self.TEAMS, ["T2"])] == ["t2"]
        assert [t["id"] for t in select_teams(self.TEAMS, ["engineering"])] == ["t1"]
        assert select_teams(self.TEAMS, ["unknown"]) == []

    def test_channel_type_and_sync_decision(self) -> None:
        standard = {"id": "c1", "membershipType": "standard"}
        private = {"id": "c2", "membershipType": "private"}
        shared = {"id": "c3", "membershipType": "shared", "isArchived": True}
        legacy = {"id": "c4"}  # membershipType missing -> standard
        assert not is_private_or_shared_channel(standard) and not is_private_or_shared_channel(legacy)
        assert is_private_or_shared_channel(private) and is_private_or_shared_channel(shared)
        assert should_sync_channel(standard, include_private_channels=False)
        assert should_sync_channel(shared, include_private_channels=True)
        assert not should_sync_channel(private, include_private_channels=False)
        assert not should_sync_channel({"membershipType": "standard"}, include_private_channels=True)


class TestPermissions:
    def test_standard_channel_grants_team_group_reader(self) -> None:
        grants = channel_grants(TEAM, {"id": CHANNEL, "membershipType": "standard"})
        assert grants == [PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id=f"team:{TEAM}")]

    def test_private_and_shared_channels_grant_channel_group_reader(self) -> None:
        for kind in ("private", "shared"):
            grants = channel_grants(TEAM, {"id": CHANNEL, "membershipType": kind})
            assert grants == [PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id=f"channel-members:{TEAM}/{CHANNEL}")]
            assert grants[0].reason == f"{kind} channel members"

    def test_chat_grants_one_reader_per_participant_email(self) -> None:
        members = [
            Member(ALICE, "alice@contoso.com", "Alice", ("owner",)),
            Member(BOB, None, "Bob"),
            Member(CAROL, "carol@contoso.com", "Carol"),
            Member("dup", "alice@contoso.com", "Alice again"),
        ]
        grants = chat_grants(members)
        assert [g.email for g in grants] == ["alice@contoso.com", "carol@contoso.com"]
        assert all(g.entity_type == GrantEntity.USER and g.role == GrantRole.READER for g in grants)

    def test_only_reader_role_exists(self) -> None:
        assert [r.value for r in GrantRole] == ["READER"]

    def test_permission_grant_reason_is_not_part_of_identity(self) -> None:
        a = PermissionGrant(GrantEntity.USER, GrantRole.READER, email="x@y.z", reason="one")
        b = PermissionGrant(GrantEntity.USER, GrantRole.READER, email="x@y.z", reason="two")
        assert a == b

    def test_documented_graph_permissions(self) -> None:
        assert "ChannelMessage.Read.All" in REQUIRED_APPLICATION_PERMISSIONS
        assert "ChannelMember.Read.All" in REQUIRED_APPLICATION_PERMISSIONS
        assert CHAT_APPLICATION_PERMISSIONS == ("Chat.Read.All",)
        assert set(PROTECTED_API_PERMISSIONS) <= set(REQUIRED_APPLICATION_PERMISSIONS + CHAT_APPLICATION_PERMISSIONS)
        # /shares/{token}/driveItem for files shared in messages
        assert FILE_APPLICATION_PERMISSIONS == ("Files.Read.All",)
        assert set(FILE_APPLICATION_PERMISSIONS) <= set(REQUIRED_APPLICATION_PERMISSIONS)
        assert not set(FILE_APPLICATION_PERMISSIONS) & set(PROTECTED_API_PERMISSIONS)


class TestChats:
    CHAT = {"id": "19:x@unq.gbl.spaces", "chatType": "group", "topic": None, "webUrl": "https://teams.microsoft.com/l/chat/19:x/0"}
    MEMBERS = [Member(ALICE, "alice@contoso.com", "Alice Adams"), Member(BOB, "bob@contoso.com", "Bob Brown")]

    def test_chat_title(self) -> None:
        assert chat_title(self.CHAT, self.MEMBERS) == "Chat: Alice Adams, Bob Brown"
        assert chat_title({**self.CHAT, "topic": " Q3 planning "}, self.MEMBERS) == "Q3 planning"
        assert chat_title({**self.CHAT, "chatType": "meeting"}, self.MEMBERS) == "Meeting chat: Alice Adams, Bob Brown"
        many = [Member(str(i), None, f"P{i}") for i in range(6)]
        assert chat_title(self.CHAT, many) == "Chat: P0, P1, P2, P3, +2 more"
        assert chat_title(self.CHAT, []) == "Chat 19:x@unq.gbl.spaces"

    def test_select_chat_messages_window_and_order(self) -> None:
        old = _msg("1", ALICE, "Alice", "2024-04-01T10:00:00Z", "old", chatId=self.CHAT["id"], channelIdentity=None)
        new1 = _msg("2", BOB, "Bob", "2024-05-02T10:00:00Z", "second", chatId=self.CHAT["id"], channelIdentity=None)
        new0 = _msg("3", ALICE, "Alice", "2024-05-01T10:00:00Z", "first", chatId=self.CHAT["id"], channelIdentity=None)
        gone = _msg("4", ALICE, "Alice", "2024-05-03T10:00:00Z", "", chatId=self.CHAT["id"], channelIdentity=None, deletedDateTime="2024-05-03T11:00:00Z")
        window = parse_graph_timestamp("2024-05-01T00:00:00Z")
        views = select_chat_messages([new1, old, gone, new0], window)
        assert [v.id for v in views] == ["3", "2"]
        assert views[0].chat_id == self.CHAT["id"] and views[0].team_id is None
        assert chat_revision(views) == f"{views[-1].created_ms}:2"
        assert chat_revision([]) == "0:0"
        assert [v.id for v in select_chat_messages([old], None)] == ["1"]

    def test_render_chat_markdown(self) -> None:
        views = select_chat_messages([
            _msg("2", BOB, "Bob Brown", "2024-05-02T10:00:00Z", "<p>second</p>", chatId=self.CHAT["id"]),
            _msg("3", ALICE, "Alice Adams", "2024-05-01T10:00:00Z", "<p>first</p>", chatId=self.CHAT["id"]),
        ], None)
        md = render_chat_markdown(self.CHAT, self.MEMBERS, views, 30)
        lines = md.splitlines()
        assert lines[0] == "# Chat: Alice Adams, Bob Brown"
        assert lines[2] == "**Microsoft Teams chat** · group · last 30 days · 2 messages"
        assert "Participants: Alice Adams (alice@contoso.com), Bob Brown (bob@contoso.com)" in lines
        assert lines.index("### Alice Adams · 2024-05-01 10:00 UTC") < lines.index("### Bob Brown · 2024-05-02 10:00 UTC")
        assert "- Window: 2024-05-01 10:00 UTC → 2024-05-02 10:00 UTC" in lines
        assert lines[-1] == "- URL: https://teams.microsoft.com/l/chat/19:x/0"
        empty = render_chat_markdown(self.CHAT, [], [], 7)
        assert "_No messages in the lookback window._" in empty and "1 message" not in empty


class TestDelta:
    def test_parse_delta_page(self) -> None:
        page = parse_delta_page({"value": [ROOT, "junk"], "@odata.nextLink": "https://graph/next"})
        assert [i["id"] for i in page.items] == [ROOT["id"]]
        assert page.next_link == "https://graph/next" and page.delta_link is None
        last = parse_delta_page({"value": [], "@odata.deltaLink": "https://graph/delta?token=1"})
        assert last.items == [] and last.next_link is None and last.delta_link == "https://graph/delta?token=1"
        assert parse_delta_page({}).items == []

    def test_classify_delta_items(self) -> None:
        other_root = _msg("1700000000900", BOB, "Bob", "2024-05-02T10:00:00Z", "new thread")
        deleted_root = _msg("1700000000800", BOB, "Bob", "2024-05-02T10:00:00Z", "", deletedDateTime="2024-05-02T11:00:00Z")
        orphan_reply = _msg("1700000000950", BOB, "Bob", "2024-05-02T10:00:00Z", "reply", replyToId="1700000000700")
        changes = classify_delta_items([REPLY_LATE, other_root, deleted_root, REPLY_DELETED, orphan_reply, {"noid": True}])
        # replies (added / edited / deleted) dirty their root; roots carried by the page are cached
        assert changes.dirty_root_ids == [ROOT["id"], other_root["id"], "1700000000700"]
        assert changes.deleted_root_ids == [deleted_root["id"]]
        assert set(changes.roots) == {other_root["id"]}

    def test_deleting_a_root_wins_over_a_dirty_mark_and_folds_across_pages(self) -> None:
        changes = classify_delta_items([ROOT, REPLY_EARLY])
        assert changes.dirty_root_ids == [ROOT["id"]] and ROOT["id"] in changes.roots
        classify_delta_items([{**ROOT, "deletedDateTime": "2024-05-03T00:00:00Z"}], changes)
        assert changes.dirty_root_ids == [] and changes.deleted_root_ids == [ROOT["id"]] and changes.roots == {}
        # a later reply to a deleted root must not resurrect it within the same run
        classify_delta_items([REPLY_LATE], changes)
        assert changes.dirty_root_ids == []
        assert isinstance(changes, DeltaChanges)

    def test_sync_point_cursor_roundtrip(self) -> None:
        data = delta_sync_point_data("https://graph/delta?token=abc", 1714558530000)
        assert data == {"deltaLink": "https://graph/delta?token=abc", "lastSyncTimestamp": 1714558530000}
        assert read_delta_link(data) == "https://graph/delta?token=abc"
        assert read_last_sync_ms(data) == 1714558530000
        fallback = delta_sync_point_data(None, 5)
        assert read_delta_link(fallback) is None and read_last_sync_ms(fallback) == 5
        assert read_delta_link(None) is None and read_delta_link({}) is None
        assert read_last_sync_ms({"lastSyncTimestamp": "bad"}) is None

    def test_delta_unsupported_statuses(self) -> None:
        assert all(is_delta_unsupported_status(s) for s in (400, 404, 501))
        assert not any(is_delta_unsupported_status(s) for s in (401, 403, 429, 500, 503))


SHAREPOINT_URL = "https://contoso.sharepoint.com/sites/eng/Shared%20Documents/General/تقرير Q3.docx"
DRIVE_ID = "b!ZLqnd4v4L0mGC7ULY1qWxtUCzNoFl1NMr1kU8QeDmQ7nOd-5N2stTL_bWFs70HM3"
ITEM_ID = "01BYE5RZ6QN3ZWBTUFOFD3GSPGOHDJD36K"

# GET /shares/{u!...}/driveItem for a Word file in a channel's Files tab
DRIVE_ITEM = {
    "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#shares('u!aHR0')/driveItem/$entity",
    "@microsoft.graph.downloadUrl": "https://contoso.sharepoint.com/sites/eng/_layouts/15/download.aspx?UniqueId=abc&Translate=false&tempauth=eyJ0eXAi",
    "createdDateTime": "2024-04-30T08:00:00Z",
    "eTag": "\"{5B0AA2B2-27D7-4E0A-B1E3-3F7B7B5D6C8A},3\"",
    "id": ITEM_ID,
    "lastModifiedDateTime": "2024-05-01T09:30:12.567Z",
    "name": "تقرير Q3.docx",
    "webUrl": "https://contoso.sharepoint.com/sites/eng/Shared%20Documents/General/%D8%AA%D9%82%D8%B1%D9%8A%D8%B1%20Q3.docx",
    "cTag": "\"c:{5B0AA2B2-27D7-4E0A-B1E3-3F7B7B5D6C8A},3\"",
    "size": 48213,
    "parentReference": {"driveType": "documentLibrary", "driveId": DRIVE_ID, "id": "01BYE5RZ56Y2GOVW7725BZO354PWSELRRZ", "path": f"/drives/{DRIVE_ID}/root:/General"},
    "file": {
        "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "hashes": {"quickXorHash": "Jh1Ha6ytRz0Ijm0Zoe9fFAOhnwE="},
    },
    "fileSystemInfo": {"createdDateTime": "2024-04-30T08:00:00Z", "lastModifiedDateTime": "2024-05-01T09:30:12Z"},
}

HOSTED_A = f"https://graph.microsoft.com/v1.0/teams/{TEAM}/channels/{CHANNEL}/messages/1616963377068/hostedContents/aWQ9eF8wLXd1cy1kMS02YmI3Nzk3ZGU2MmRjODdjODA4YmQ1ZmI0OWM4NjI2ZA/$value"
HOSTED_B = f"https://graph.microsoft.com/v1.0/teams/{TEAM}/channels/{CHANNEL}/messages/1616963377068/replies/1616989753153/hostedContents/aWQ9eF8wLXd1cy1kNi0xMzY3OTE4MzVlODIx/$value"
HOSTED_CHAT = "https://graph.microsoft.com/v1.0/chats/19:2da4c29f6d7041eca70b638b43d45437@thread.v2/messages/1615971548136/hostedContents/aWQ9eF8wLXd1cy1kOS1lNTRmNjM1NWYxYmJkNGQ3ZTNmNGJhZmU4NTI5MTBmNi/$value"


class TestSharedFiles:
    """``reference`` attachments -> driveItem -> FileRecord inputs (mirrors the OneDrive shape)."""

    def test_encode_sharing_url_matches_graph_recipe(self) -> None:
        token = encode_sharing_url(SHAREPOINT_URL)
        expected = "u!" + base64.urlsafe_b64encode(SHAREPOINT_URL.encode("utf-8")).decode("ascii").rstrip("=")
        assert token == expected
        assert token.startswith("u!") and not token.endswith("=")
        assert "+" not in token and "/" not in token[2:]
        # the documented C# sample: base64 of the URL with / -> _ and + -> -
        raw = base64.b64encode(SHAREPOINT_URL.encode("utf-8")).decode("ascii")
        assert token[2:] == raw.rstrip("=").replace("/", "_").replace("+", "-")
        assert shared_drive_item_url(SHAREPOINT_URL) == f"shares/{token}/driveItem"
        assert encode_sharing_url("  https://x.y/z  ") == encode_sharing_url("https://x.y/z")

    def test_drive_item_file_info_reads_the_onedrive_fields(self) -> None:
        info = drive_item_file_info(DRIVE_ITEM)
        assert isinstance(info, FileInfo)
        assert (info.drive_id, info.item_id, info.name) == (DRIVE_ID, ITEM_ID, "تقرير Q3.docx")
        assert info.mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        assert info.size == 48213
        assert info.web_url == DRIVE_ITEM["webUrl"]
        assert info.etag == DRIVE_ITEM["eTag"] and info.ctag == DRIVE_ITEM["cTag"]
        assert info.created_ms == parse_graph_timestamp("2024-04-30T08:00:00Z")
        assert info.modified_ms == parse_graph_timestamp("2024-05-01T09:30:12.567Z")
        assert info.quick_xor_hash == "Jh1Ha6ytRz0Ijm0Zoe9fFAOhnwE=" and info.sha256_hash is None
        assert info.download_url == DRIVE_ITEM["@microsoft.graph.downloadUrl"]
        assert info.external_id == f"file:{DRIVE_ID}/{ITEM_ID}"

    def test_drive_item_file_info_rejects_folders_and_unusable_items(self) -> None:
        assert drive_item_file_info({**DRIVE_ITEM, "folder": {"childCount": 3}, "file": None}) is None
        assert drive_item_file_info({**DRIVE_ITEM, "package": {"type": "oneNote"}}) is None  # OneNote notebook
        assert drive_item_file_info({**DRIVE_ITEM, "parentReference": {}}) is None
        assert drive_item_file_info({**DRIVE_ITEM, "id": None}) is None
        assert drive_item_file_info("not a dict") is None

    def test_drive_item_file_info_guesses_mime_and_tolerates_missing_metadata(self) -> None:
        sparse = {"id": ITEM_ID, "name": "notes.PDF", "parentReference": {"driveId": DRIVE_ID}, "file": {}}
        info = drive_item_file_info(sparse)
        assert info.mime_type == "application/pdf"
        assert info.size is None and info.etag is None and info.created_ms is None
        unknown = drive_item_file_info({**sparse, "name": "blob", "size": 7.0})
        assert unknown.mime_type == "application/octet-stream" and unknown.size == 7
        assert drive_item_file_info({**sparse, "file": {"mimeType": "Text/Plain; charset=utf-8"}}).mime_type == "text/plain"

    def test_file_attachments_only_references_deduped_across_the_thread(self) -> None:
        same_file_again = _msg("1700000000400", BOB, "Bob Brown", "2024-05-01T13:00:00Z", "<p>re-sharing</p>", replyToId=ROOT["id"],
                               attachments=[
                                   {"id": "f1", "contentType": "reference", "name": "notes.docx",
                                    "contentUrl": "https://contoso.sharepoint.com/sites/eng/Shared%20Documents/notes.docx"},
                                   {"id": "f2", "contentType": "reference", "name": "budget.xlsx",
                                    "contentUrl": "https://contoso.sharepoint.com/sites/eng/Shared%20Documents/budget.xlsx"},
                                   {"id": "f3", "contentType": "reference", "name": "no-url.docx", "contentUrl": None},
                               ])
        deleted_with_file = _msg("1700000000450", BOB, "Bob Brown", "2024-05-01T13:30:00Z", "", replyToId=ROOT["id"],
                                 deletedDateTime="2024-05-01T13:31:00Z",
                                 attachments=[{"id": "f9", "contentType": "reference", "name": "gone.pdf",
                                               "contentUrl": "https://contoso.sharepoint.com/sites/eng/Shared%20Documents/gone.pdf"}])
        thread = build_thread(ROOT, [same_file_again, deleted_with_file])
        files = file_attachments(thread.messages)
        assert [a.name for a in files] == ["notes.docx", "budget.xlsx"]  # first occurrence wins, deleted reply excluded
        assert all(a.is_file_reference for a in files)
        # cards / quoted messages / url-less references never become records
        skipped = skipped_attachments(thread.messages)
        assert [(a.name, a.is_card) for a in skipped] == [("card (adaptive)", True), ("no-url.docx", False)]


class TestHostedImages:
    """Pictures pasted into a message body (Graph-hosted content)."""

    BODY = (
        '<div><div><span><img height="145" src="' + HOSTED_A + '" width="131" style="vertical-align:bottom"></span>'
        '<div>&nbsp;</div></div><div><span><img alt="لقطة الشاشة" src="' + HOSTED_B + '"></span></div>'
        '<img src="' + HOSTED_A + '"><img src="https://media.giphy.com/media/x/giphy.gif" alt="gif">'
        '<img src="http://graph.microsoft.com/v1.0/teams/x/channels/y/messages/1/hostedContents/z/$value">'
        '<img src="https://graph.microsoft.com/v1.0/teams/x/channels/y/messages/1/hostedContents/z">'
        '<img src="https://graph.microsoft.com/v2.0/teams/x/hostedContents/z/$value"></div>'
    )

    def test_extracts_graph_hosted_images_only_and_dedupes(self) -> None:
        images = hosted_images_in_html(self.BODY)
        assert [i.graph_path for i in images] == [
            HOSTED_A[len("https://graph.microsoft.com/v1.0/"):-len("/$value")],
            HOSTED_B[len("https://graph.microsoft.com/v1.0/"):-len("/$value")],
        ]
        assert images[0].alt is None and images[1].alt == "لقطة الشاشة"
        assert images[0].hosted_id == "aWQ9eF8wLXd1cy1kMS02YmI3Nzk3ZGU2MmRjODdjODA4YmQ1ZmI0OWM4NjI2ZA"
        assert hosted_images_in_html(None) == () and hosted_images_in_html("<p>no images</p>") == ()
        assert hosted_images_in_html("<img src='" + HOSTED_CHAT + "'/>")[0].graph_path.startswith("chats/19:2da4c29f")

    def test_file_names_are_stable_and_carry_the_png_extension(self) -> None:
        a, b = hosted_images_in_html(self.BODY)
        assert a.file_name() == "image-I0OWM4NjI2ZA.png"  # last 12 chars of the hosted id
        assert b.file_name() == "لقطة الشاشة.png"
        assert HostedImage("x/hostedContents/abc", alt="image").file_name() == "image-abc.png"
        assert HostedImage("x/hostedContents/abc", alt="Chart.PNG").file_name() == "Chart.PNG"
        assert HOSTED_IMAGE_MIME_TYPE == "image/png"

    def test_normalize_message_and_thread_collect_hosted_images(self) -> None:
        root = _msg("1616963377068", ALICE, "Alice Adams", "2024-05-01T10:00:00Z", self.BODY)
        reply = _msg("1616989753153", BOB, "Bob Brown", "2024-05-01T11:00:00Z", '<img src="' + HOSTED_B + '">', replyToId=root["id"])
        plain = _msg("2", BOB, "Bob", "2024-05-01T11:00:00Z", HOSTED_A, body={"contentType": "text", "content": HOSTED_A})
        assert len(normalize_message(root).hosted_images) == 2
        assert normalize_message(plain).hosted_images == ()
        thread = build_thread(root, [reply])
        assert len(hosted_images(thread.messages)) == 2  # the reply's image is the root's second one
        # the alt text is still part of the searchable body
        assert "لقطة الشاشة" in thread.root.body_text

    def test_graph_relative_path(self) -> None:
        assert graph_relative_path("https://graph.microsoft.com/v1.0/teams/t/channels/c?x=1") == "teams/t/channels/c"
        assert graph_relative_path("https://graph.microsoft.com/beta/chats/c/messages/m") == "chats/c/messages/m"
        assert graph_relative_path("https://GRAPH.microsoft.com/v1.0/me") == "me"
        for bad in (None, "", "https://contoso.sharepoint.com/v1.0/x", "http://graph.microsoft.com/v1.0/x",
                    "https://graph.microsoft.com/v1.0/", "https://graph.microsoft.com/x/y", 42):
            assert graph_relative_path(bad) is None


class TestReplySweep:
    """``/messages?$expand=replies`` sorted by reply-chain modification, newest first."""

    T0 = parse_graph_timestamp("2024-05-01T12:00:00Z")

    @staticmethod
    def _expanded(root: dict, replies: list, more: str | None = None) -> dict:
        row = {**root, "replies@odata.count": len(replies), "replies": replies}
        if more:
            row["replies@odata.nextLink"] = more
        return row

    def test_expanded_replies_and_chain_last_modified(self) -> None:
        root = self._expanded(ROOT, [REPLY_LATE, REPLY_EARLY], "https://graph/replies?$skiptoken=MSww")
        replies, more = expanded_replies(root)
        assert [r["id"] for r in replies] == [REPLY_LATE["id"], REPLY_EARLY["id"]]
        assert more == "https://graph/replies?$skiptoken=MSww"
        assert expanded_replies(ROOT) == ([], None)
        # the newest lastModified across root + replies; a reply's deletion counts too
        assert reply_chain_last_modified_ms(ROOT, replies) == parse_graph_timestamp("2024-05-01T12:05:00Z")
        assert reply_chain_last_modified_ms(ROOT, [REPLY_DELETED]) == parse_graph_timestamp("2024-05-01T11:31:00Z")
        assert reply_chain_last_modified_ms({"id": "x"}, []) == 0

    def test_classify_listing_stops_at_the_first_stale_chain(self) -> None:
        fresh_reply = _msg("1700000000900", CAROL, "Carol Cruz", "2024-05-01T12:30:00Z", "<p>new reply</p>", replyToId=ROOT["id"])
        active = self._expanded(ROOT, [REPLY_EARLY, fresh_reply])                # old root, new reply -> rebuild
        deleted_root = self._expanded(_msg("1700000000800", BOB, "Bob", "2024-04-30T10:00:00Z", "",
                                           deletedDateTime="2024-05-01T12:10:00Z"), [])  # soft-deleted after T0
        system = self._expanded(_msg("1700000000700", ALICE, "x", "2024-05-01T12:20:00Z", "", messageType="systemEventMessage"), [])
        stale = self._expanded(_msg("1700000000600", BOB, "Bob", "2024-04-01T10:00:00Z", "<p>old</p>"), [REPLY_EARLY])
        never_seen = self._expanded(_msg("1700000000500", BOB, "Bob", "2024-03-01T10:00:00Z", "<p>older</p>"), [])
        changes = classify_listing_items([active, deleted_root, system, stale, never_seen, {"noid": 1}], self.T0)
        assert [root["id"] for root, _, _ in changes.threads] == [ROOT["id"]]
        assert changes.threads[0][1] == [REPLY_EARLY, fresh_reply] and changes.threads[0][2] is None
        assert changes.deleted_root_ids == [deleted_root["id"], system["id"]]
        assert changes.stale is True  # `stale` ended the page; `never_seen` was not even looked at

    def test_classify_listing_skips_roots_handled_by_delta_but_keeps_the_stop_condition(self) -> None:
        fresh_reply = _msg("1700000000900", CAROL, "Carol", "2024-05-01T12:30:00Z", "x", replyToId=ROOT["id"])
        active = self._expanded(ROOT, [fresh_reply], "https://graph/replies?$skiptoken=next")
        changes = classify_listing_items([active], self.T0, skip_root_ids={ROOT["id"]})
        assert changes.threads == [] and changes.deleted_root_ids == [] and not changes.stale
        changes = classify_listing_items([active], self.T0)
        assert changes.threads[0][2] == "https://graph/replies?$skiptoken=next"

    def test_classify_listing_full_sync_takes_everything(self) -> None:
        old = self._expanded(_msg("1700000000600", BOB, "Bob", "2024-04-01T10:00:00Z", "<p>old</p>"), [])
        changes = classify_listing_items([self._expanded(ROOT, []), old], None)
        assert [r["id"] for r, _, _ in changes.threads] == [ROOT["id"], old["id"]]
        assert not changes.stale

    def test_listing_page_reuses_the_delta_page_parser(self) -> None:
        page = parse_delta_page({"value": [self._expanded(ROOT, [])], "@odata.nextLink": "https://graph/messages?$skiptoken=x"})
        assert page.delta_link is None and page.next_link.endswith("skiptoken=x")
        assert expanded_replies(page.items[0]) == ([], None)


class TestDeletedMessages:
    """Soft deletes (``deletedDateTime``) must remove content from the index."""

    def test_deleted_root_in_delta_is_a_deletion_not_a_rebuild(self) -> None:
        deleted_root = {**ROOT, "deletedDateTime": "2024-05-02T11:00:00Z", "body": {"contentType": "html", "content": ""}}
        changes = classify_delta_items([deleted_root])
        assert changes.deleted_root_ids == [ROOT["id"]] and changes.dirty_root_ids == []
        # a dirty root that comes back deleted from GET is not indexable -> connector deletes the record
        assert not build_thread(deleted_root, [REPLY_EARLY]).root.is_indexable

    def test_deleted_reply_leaves_thread_and_attachments(self) -> None:
        thread = build_thread(ROOT, [REPLY_EARLY, REPLY_DELETED])
        assert [r.id for r in thread.replies] == [REPLY_EARLY["id"]]
        assert thread_revision(thread) != thread_revision(build_thread(ROOT, [REPLY_EARLY, REPLY_LATE]))
        assert "gone.pdf" not in render_thread_markdown("T", "C", thread)

    def test_chat_with_every_message_deleted_yields_no_messages(self) -> None:
        chat_id = "19:x@unq.gbl.spaces"
        gone = _msg("4", ALICE, "Alice", "2024-05-03T10:00:00Z", "", chatId=chat_id, channelIdentity=None,
                    deletedDateTime="2024-05-03T11:00:00Z", lastModifiedDateTime="2024-05-03T11:00:00Z")
        # Graph still lists it (with a bumped lastModifiedDateTime), so the incremental filter sees a change...
        assert parse_graph_timestamp(gone["lastModifiedDateTime"]) > parse_graph_timestamp(gone["createdDateTime"])
        # ...and the window is now empty -> the connector deletes the chat record and its files
        assert select_chat_messages([gone], parse_graph_timestamp("2024-05-01T00:00:00Z")) == []
        assert chat_revision([]) == "0:0"


class TestArabic:
    """Arabic / RTL content must come out verbatim: no ASCII folding, no reordering, no lower-casing."""

    ARABIC_HTML = (
        '<div dir="rtl" style="direction: rtl; text-align: right;">'
        '<p>مرحباً <b>فريق التطوير</b>، تم رفع <a href="' + SHAREPOINT_URL + '">تقرير Q3</a>'
        ' ‏(النسخة 2.1)&nbsp;— يرجى المراجعة قبل الأحد</p>'
        '<ul><li>الميزانية: 1,250,000 ر.س</li><li>&#1575;&#1604;&#1605;&#1608;&#1593;&#1583;: 15 مايو</li></ul>'
        '<p>شكراً <at id="0">محمد العلي</at></p></div>'
    )
    TEAM_NAME = "فريق الهندسة"
    CHANNEL_NAME = "عام"

    def _arabic_root(self) -> dict:
        return _msg(
            "1700000009001", ALICE, "سارة الأحمد", "2024-05-01T10:15:30Z", self.ARABIC_HTML,
            subject="تقرير الربع الثالث",
            mentions=[{"id": 0, "mentionText": "محمد العلي", "mentioned": {"user": {"id": BOB, "displayName": "محمد العلي"}}}],
            attachments=[{"id": "a1", "contentType": "reference", "name": "تقرير Q3.docx", "contentUrl": SHAREPOINT_URL}],
        )

    def test_html_to_text_preserves_arabic_order_marks_and_entities(self) -> None:
        text = html_to_text(self.ARABIC_HTML)
        lines = text.splitlines()
        assert lines[0] == f"مرحباً فريق التطوير، تم رفع تقرير Q3 ({SHAREPOINT_URL}) ‏(النسخة 2.1) — يرجى المراجعة قبل الأحد"
        assert lines[2] == "- الميزانية: 1,250,000 ر.س"
        assert lines[3] == "- الموعد: 15 مايو"  # numeric character references decoded
        assert lines[5] == "شكراً محمد العلي"
        assert "‏" in text  # the RLM is not whitespace and survives collapsing
        assert text == text.strip() and "\n\n\n" not in text
        # nothing was folded to ASCII, lower-cased or NFKD-normalised
        assert "مرحباً" in text and "Q3" in text and "q3" not in text

    def test_names_titles_and_group_names_keep_arabic(self) -> None:
        view = normalize_message(self._arabic_root())
        assert view.author_name == "سارة الأحمد"
        assert view.subject == "تقرير الربع الثالث"
        assert [m.text for m in view.mentions] == ["محمد العلي"]
        assert view.attachments[0].name == "تقرير Q3.docx" and view.attachments[0].is_file_reference
        thread = build_thread(self._arabic_root(), [])
        assert thread_title(thread, self.CHANNEL_NAME) == "تقرير الربع الثالث"
        assert channel_record_group_name(self.TEAM_NAME, self.CHANNEL_NAME) == "فريق الهندسة › عام"
        md = render_thread_markdown(self.TEAM_NAME, self.CHANNEL_NAME, thread)
        lines = md.splitlines()
        assert lines[0] == "# تقرير الربع الثالث"
        assert lines[2] == "**Microsoft Teams** · فريق الهندسة › عام · 0 replies"
        assert "## سارة الأحمد · 2024-05-01 10:15 UTC" in lines
        assert "Mentions: محمد العلي" in lines
        assert f"- تقرير Q3.docx — {SHAREPOINT_URL}" in lines
        assert "- Team: فريق الهندسة" in lines and "- Channel: عام" in lines
        assert md.encode("utf-8").decode("utf-8") == md
        no_subject = _msg("2", ALICE, "سارة الأحمد", "2024-05-01T10:00:00Z", "<p>نص قصير</p>")
        assert thread_title(build_thread(no_subject, []), self.CHANNEL_NAME) == "نص قصير"
        empty = _msg("3", ALICE, "سارة الأحمد", "2024-05-01T10:00:00Z", "")
        assert thread_title(build_thread(empty, []), self.CHANNEL_NAME) == "سارة الأحمد in عام"

    def test_title_truncation_never_splits_a_letter_from_its_tashkeel(self) -> None:
        # 78 chars, then a letter carrying two combining marks: the 80-char cut would land
        # between the letter (index 78) and its fatha (79) and orphan the shadda (80)
        word = "بِسْمِ اللَّهِ الرَّحْمَٰنِ الرَّحِيمِ "
        body = (word * 3)[:78] + "مَّ" + " وبعد ذلك نص طويل جداً " * 4
        assert unicodedata.combining(body[79]) and unicodedata.combining(body[80])
        title = thread_title(build_thread(_msg("5", ALICE, "A", "2024-05-01T10:00:00Z", "<p>" + body + "</p>"), []), "C")
        assert title.endswith("…") and len(title) <= 80
        kept = title[:-1]
        assert body.startswith(kept)
        # the first character we dropped is a base letter, so no cluster was split
        assert not unicodedata.combining(body[len(kept)])
        assert kept == body[:78].rstrip()
        # ASCII behaviour unchanged: exactly 80 chars
        ascii_title = thread_title(build_thread(_msg("6", ALICE, "A", "2024-05-01T10:00:00Z", "<p>" + "x" * 100 + "</p>"), []), "C")
        assert len(ascii_title) == 80

    def test_chat_titles_members_and_team_filter_with_arabic(self) -> None:
        members = parse_conversation_members([
            {"id": "m1", "roles": [], "displayName": "سارة الأحمد", "userId": ALICE, "email": "Sara@Contoso.com"},
            {"id": "m2", "roles": [], "displayName": "محمد العلي", "userId": BOB, "email": None},
        ])
        assert [m.display_name for m in members] == ["سارة الأحمد", "محمد العلي"]
        assert members[0].email == "sara@contoso.com"  # only the email is lower-cased
        chat = {"id": "19:x@unq.gbl.spaces", "chatType": "group", "topic": None}
        assert chat_title(chat, members) == "Chat: سارة الأحمد, محمد العلي"
        assert chat_title({**chat, "topic": " تخطيط الربع الثالث "}, members) == "تخطيط الربع الثالث"
        teams = [{"id": "t1", "displayName": self.TEAM_NAME}, {"id": "t2", "displayName": "Sales"}]
        assert [t["id"] for t in select_teams(teams, ["فريق الهندسة"])] == ["t1"]
        assert [t["id"] for t in select_teams(teams, ["فريق الهندسه"])] == []  # no fuzzy folding of ة / ه
        md = render_chat_markdown(chat, members, select_chat_messages([
            _msg("1", ALICE, "سارة الأحمد", "2024-05-01T10:00:00Z", "<p>السلام عليكم</p>", chatId=chat["id"]),
        ], None), 30)
        assert "Participants: سارة الأحمد (sara@contoso.com), محمد العلي" in md
        assert "السلام عليكم" in md


class TestPersonalScope:
    """Personal (delegated OAuth) scope: the signed-in user's chats, creator-only READER."""

    def test_personal_chat_grants_creator_only(self) -> None:
        grants = personal_chat_grants("Alice.Owner@Contoso.com")
        assert grants == [PermissionGrant(
            GrantEntity.USER, GrantRole.READER, email="alice.owner@contoso.com", reason="personal connector creator",
        )]
        # participants of the chat never leak into the grant list
        members = parse_conversation_members([
            {"userId": "u1", "email": "alice.owner@contoso.com", "displayName": "Alice"},
            {"userId": "u2", "email": "bob@contoso.com", "displayName": "Bob"},
        ])
        assert {g.email for g in chat_grants(members)} == {"alice.owner@contoso.com", "bob@contoso.com"}
        assert [g.email for g in personal_chat_grants("alice.owner@contoso.com")] == ["alice.owner@contoso.com"]
        assert all(g.entity_type is GrantEntity.USER and g.role is GrantRole.READER for g in grants)
        assert all(g.external_id is None for g in grants)

    def test_personal_chat_grants_fail_closed_without_creator(self) -> None:
        assert personal_chat_grants(None) == []
        assert personal_chat_grants("") == []
        assert personal_chat_grants("   ") == []
        assert personal_chat_grants("not-an-email") == []

    def test_me_chats_url_and_delegated_permissions(self) -> None:
        assert me_chats_url() == "me/chats?$expand=members&$top=50"
        assert not me_chats_url().startswith("users/")
        # Files.Read resolves files shared in the user's own chats (/shares/{token}/driveItem)
        assert PERSONAL_DELEGATED_PERMISSIONS == ("Chat.Read", "Files.Read", "User.Read", "offline_access")
        # delegated Chat.Read is not one of the protected app-only APIs
        assert not set(PERSONAL_DELEGATED_PERMISSIONS) & set(PROTECTED_API_PERMISSIONS)


if __name__ == "__main__":  # plain-python runner for machines without pytest
    import inspect
    import sys
    import traceback

    failures = 0
    total = 0
    for _name, cls in sorted(globals().items()):
        if not (inspect.isclass(cls) and _name.startswith("Test")):
            continue
        for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not method_name.startswith("test_"):
                continue
            total += 1
            try:
                method(cls())
            except Exception:
                failures += 1
                print(f"FAIL {cls.__name__}.{method_name}")
                traceback.print_exc()
    print(f"{total - failures}/{total} tests passed")
    sys.exit(1 if failures else 0)
