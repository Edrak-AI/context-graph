"""Tests for app.connectors.sources.microsoft.teams.mapping.

Pure functions only (message normalisation, HTML flattening, thread rendering,
permission derivation, delta-cursor handling) — no network, no pydantic, no
azure/httpx/msgraph.  Written pytest-style but runnable with plain
``python3 tests/unit/connectors/sources/test_teams_mapping.py`` as well.
"""

from contextlib import contextmanager

from app.connectors.sources.microsoft.teams.mapping import (
    CHAT_APPLICATION_PERMISSIONS,
    DEFAULT_CHAT_LOOKBACK_DAYS,
    MAX_CHAT_LOOKBACK_DAYS,
    PROTECTED_API_PERMISSIONS,
    REQUIRED_APPLICATION_PERMISSIONS,
    DeltaChanges,
    GrantEntity,
    GrantRole,
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
    delta_sync_point_data,
    epoch_ms_to_graph,
    html_to_text,
    is_delta_unsupported_status,
    is_private_or_shared_channel,
    lookback_start_ms,
    message_replies_url,
    normalize_message,
    parse_conversation_members,
    parse_delta_page,
    parse_graph_timestamp,
    parse_group_members,
    read_delta_link,
    read_last_sync_ms,
    render_chat_markdown,
    render_thread_markdown,
    resolve_chat_lookback_days,
    select_chat_messages,
    select_teams,
    should_sync_channel,
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
def raises(exc_type):
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
    def test_parse_graph_timestamp_handles_seven_fractional_digits(self):
        assert parse_graph_timestamp("2024-05-01T10:15:30.1234567Z") == 1714558530123
        assert parse_graph_timestamp("2024-05-01T10:15:30Z") == 1714558530000
        assert parse_graph_timestamp("2024-05-01T10:15:30.5+02:00") == 1714551330500
        assert parse_graph_timestamp(None) is None
        assert parse_graph_timestamp("not a date") is None

    def test_epoch_ms_to_graph_roundtrip(self):
        assert epoch_ms_to_graph(1714558530000) == "2024-05-01T10:15:30Z"
        assert parse_graph_timestamp(epoch_ms_to_graph(1714558530999)) == 1714558530000

    def test_lookback(self):
        now = 1_800_000_000_000
        assert lookback_start_ms(now, 1) == now - 86_400_000
        assert resolve_chat_lookback_days(None) == DEFAULT_CHAT_LOOKBACK_DAYS
        assert resolve_chat_lookback_days("abc") == DEFAULT_CHAT_LOOKBACK_DAYS
        assert resolve_chat_lookback_days(0) == DEFAULT_CHAT_LOOKBACK_DAYS
        assert resolve_chat_lookback_days(7.0) == 7
        assert resolve_chat_lookback_days("14") == 14
        assert resolve_chat_lookback_days(10**9) == MAX_CHAT_LOOKBACK_DAYS


class TestExternalIds:
    def test_thread_and_chat_ids_roundtrip(self):
        ext = thread_external_id(TEAM, CHANNEL, "1700000000001")
        assert ext == f"thread:{TEAM}/{CHANNEL}/1700000000001"
        assert split_external_id(ext) == ("thread", (TEAM, CHANNEL, "1700000000001"))
        assert split_external_id(chat_external_id("19:xyz@unq.gbl.spaces")) == ("chat", ("19:xyz@unq.gbl.spaces",))

    def test_split_rejects_foreign_ids(self):
        for bad in ("", "thread:", "thread:a/b", "opportunity:guid", "nope"):
            with raises(ValueError):
                split_external_id(bad)

    def test_group_and_sync_point_ids(self):
        assert team_group_external_id(TEAM) == f"team:{TEAM}"
        assert channel_members_group_external_id(TEAM, CHANNEL) == f"channel-members:{TEAM}/{CHANNEL}"
        assert channel_record_group_external_id(TEAM, CHANNEL) == f"channel:{TEAM}/{CHANNEL}"
        assert channel_sync_point_key(TEAM, CHANNEL) == f"channel/{TEAM}/{CHANNEL}"
        assert channel_record_group_name("Engineering", "General") == "Engineering › General"


class TestUrls:
    def test_channel_urls_encode_channel_id_and_cap_top_at_50(self):
        url = channel_messages_url(TEAM, CHANNEL)
        assert url.startswith(f"teams/{TEAM}/channels/19%3Aabc123%40thread.tacv2/messages?$top=50")
        assert "$filter" not in url
        assert channel_messages_url(TEAM, CHANNEL, since_ms=1714558530000).endswith(
            "&$filter=lastModifiedDateTime gt 2024-05-01T10:15:30Z"
        )
        assert channel_delta_url(TEAM, CHANNEL).endswith("/messages/delta?$top=50")
        assert message_replies_url(TEAM, CHANNEL, "42").endswith("/messages/42/replies?$top=50")

    def test_chat_urls(self):
        assert chat_messages_url("19:x@unq.gbl.spaces") == "chats/19%3Ax%40unq.gbl.spaces/messages?$top=50"
        assert "$filter=lastModifiedDateTime gt" in chat_messages_url("c", since_ms=0)
        assert user_chats_url(ALICE) == f"users/{ALICE}/chats?$expand=members&$top=50"


class TestHtmlToText:
    def test_blocks_links_mentions_and_entities(self):
        text = html_to_text(ROOT["body"]["content"])
        assert text == (
            "Release 1.2 is ready.\n"
            "Please review notes.docx (https://contoso.sharepoint.com/sites/eng/Shared%20Documents/notes.docx) Bob Brown"
        )

    def test_lists_quotes_images_and_scripts(self):
        html = (
            '<p>Agenda</p><ul><li>one</li><li>two &amp; three</li></ul>'
            '<blockquote>quoted</blockquote><img alt="chart.png" src="x"><img src="y">'
            '<emoji alt="😀" title="grinning"></emoji><style>p{}</style><script>alert(1)</script>'
            '<a href="mailto:a@b.c">a@b.c</a> <a href="https://x.y">https://x.y</a>'
        )
        assert html_to_text(html) == (
            "Agenda\n\n- one\n- two & three\n\n> quoted\nchart.png[image]😀a@b.c https://x.y"
        )

    def test_empty_and_plain(self):
        assert html_to_text(None) == ""
        assert html_to_text("") == ""
        assert html_to_text("just   text\n\n\n\nmore") == "just text\n\nmore"


class TestNormalizeMessage:
    def test_root_message_fields(self):
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

    def test_text_body_deleted_and_system_messages(self):
        plain = normalize_message(_msg("1", ALICE, "Alice", "2024-05-01T10:00:00Z", "  hi  ", body={"contentType": "text", "content": "  hi  "}))
        assert plain.body_text == "hi"
        assert not normalize_message(REPLY_DELETED).is_indexable
        assert normalize_message(REPLY_DELETED).deleted
        assert not normalize_message(REPLY_SYSTEM).is_indexable

    def test_application_and_unknown_senders(self):
        bot = normalize_message(_msg("2", ALICE, "x", "2024-05-01T10:00:00Z", "hi", **{"from": {"application": {"id": "app", "displayName": "Planner"}}}))
        assert bot.author_id is None and bot.author_name == "Planner (app)"
        anon = normalize_message(_msg("3", ALICE, "x", "2024-05-01T10:00:00Z", "hi", **{"from": None}))
        assert anon.author_name == "Unknown sender"

    def test_last_activity_prefers_latest_timestamp(self):
        view = normalize_message(REPLY_LATE)
        assert view.edited_ms == parse_graph_timestamp("2024-05-01T12:05:00Z")
        assert view.last_activity_ms == view.edited_ms


class TestThreads:
    def test_build_thread_orders_replies_and_drops_noise(self):
        thread = build_thread(ROOT, [REPLY_LATE, REPLY_SYSTEM, REPLY_EARLY, REPLY_DELETED])
        assert [r.id for r in thread.replies] == [REPLY_EARLY["id"], REPLY_LATE["id"]]
        assert thread.last_activity_ms == parse_graph_timestamp("2024-05-01T12:05:00Z")
        assert thread_participant_ids(thread) == [ALICE, BOB, CAROL]
        assert thread_mentioned_user_ids(thread) == [BOB]

    def test_thread_title_falls_back_to_body_then_author(self):
        assert thread_title(build_thread(ROOT, []), "General") == "Release 1.2"
        no_subject = _msg("9", ALICE, "Alice Adams", "2024-05-01T10:00:00Z", "<p>" + "x" * 100 + "</p><p>second</p>")
        title = thread_title(build_thread(no_subject, []), "General")
        assert title.endswith("…") and len(title) == 80
        empty = _msg("10", ALICE, "Alice Adams", "2024-05-01T10:00:00Z", "<img src='only'>", body={"contentType": "html", "content": ""})
        assert thread_title(build_thread(empty, []), "General") == "Alice Adams in General"

    def test_thread_revision_changes_with_replies_and_edits(self):
        base = thread_revision(build_thread(ROOT, [REPLY_EARLY]))
        assert base == f"{parse_graph_timestamp('2024-05-01T11:00:00Z')}:1"
        assert thread_revision(build_thread(ROOT, [REPLY_EARLY, REPLY_LATE])) != base
        assert thread_revision(build_thread(ROOT, [REPLY_EARLY, REPLY_DELETED])) == base

    def test_render_thread_markdown(self):
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

    def test_render_singular_reply_and_no_text(self):
        md = render_thread_markdown("T", "C", build_thread(ROOT, [REPLY_EARLY]))
        assert "· 1 reply" in md
        md = render_thread_markdown("T", "C", build_thread(_msg("5", ALICE, "A", "2024-05-01T10:00:00Z", ""), []))
        assert "_(no text)_" in md and "## Replies" not in md


class TestMembers:
    def test_parse_conversation_members_dedupes_and_lowercases(self):
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

    def test_parse_group_members_skips_non_users(self):
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

    def test_select_teams(self):
        assert [t["id"] for t in select_teams(self.TEAMS, None)] == ["t1", "t2"]
        assert [t["id"] for t in select_teams(self.TEAMS, [])] == ["t1", "t2"]
        assert [t["id"] for t in select_teams(self.TEAMS, ["T2"])] == ["t2"]
        assert [t["id"] for t in select_teams(self.TEAMS, ["engineering"])] == ["t1"]
        assert select_teams(self.TEAMS, ["unknown"]) == []

    def test_channel_type_and_sync_decision(self):
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
    def test_standard_channel_grants_team_group_reader(self):
        grants = channel_grants(TEAM, {"id": CHANNEL, "membershipType": "standard"})
        assert grants == [PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id=f"team:{TEAM}")]

    def test_private_and_shared_channels_grant_channel_group_reader(self):
        for kind in ("private", "shared"):
            grants = channel_grants(TEAM, {"id": CHANNEL, "membershipType": kind})
            assert grants == [PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id=f"channel-members:{TEAM}/{CHANNEL}")]
            assert grants[0].reason == f"{kind} channel members"

    def test_chat_grants_one_reader_per_participant_email(self):
        members = [
            Member(ALICE, "alice@contoso.com", "Alice", ("owner",)),
            Member(BOB, None, "Bob"),
            Member(CAROL, "carol@contoso.com", "Carol"),
            Member("dup", "alice@contoso.com", "Alice again"),
        ]
        grants = chat_grants(members)
        assert [g.email for g in grants] == ["alice@contoso.com", "carol@contoso.com"]
        assert all(g.entity_type == GrantEntity.USER and g.role == GrantRole.READER for g in grants)

    def test_only_reader_role_exists(self):
        assert [r.value for r in GrantRole] == ["READER"]

    def test_permission_grant_reason_is_not_part_of_identity(self):
        a = PermissionGrant(GrantEntity.USER, GrantRole.READER, email="x@y.z", reason="one")
        b = PermissionGrant(GrantEntity.USER, GrantRole.READER, email="x@y.z", reason="two")
        assert a == b

    def test_documented_graph_permissions(self):
        assert "ChannelMessage.Read.All" in REQUIRED_APPLICATION_PERMISSIONS
        assert "ChannelMember.Read.All" in REQUIRED_APPLICATION_PERMISSIONS
        assert CHAT_APPLICATION_PERMISSIONS == ("Chat.Read.All",)
        assert set(PROTECTED_API_PERMISSIONS) <= set(REQUIRED_APPLICATION_PERMISSIONS + CHAT_APPLICATION_PERMISSIONS)


class TestChats:
    CHAT = {"id": "19:x@unq.gbl.spaces", "chatType": "group", "topic": None, "webUrl": "https://teams.microsoft.com/l/chat/19:x/0"}
    MEMBERS = [Member(ALICE, "alice@contoso.com", "Alice Adams"), Member(BOB, "bob@contoso.com", "Bob Brown")]

    def test_chat_title(self):
        assert chat_title(self.CHAT, self.MEMBERS) == "Chat: Alice Adams, Bob Brown"
        assert chat_title({**self.CHAT, "topic": " Q3 planning "}, self.MEMBERS) == "Q3 planning"
        assert chat_title({**self.CHAT, "chatType": "meeting"}, self.MEMBERS) == "Meeting chat: Alice Adams, Bob Brown"
        many = [Member(str(i), None, f"P{i}") for i in range(6)]
        assert chat_title(self.CHAT, many) == "Chat: P0, P1, P2, P3, +2 more"
        assert chat_title(self.CHAT, []) == "Chat 19:x@unq.gbl.spaces"

    def test_select_chat_messages_window_and_order(self):
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

    def test_render_chat_markdown(self):
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
    def test_parse_delta_page(self):
        page = parse_delta_page({"value": [ROOT, "junk"], "@odata.nextLink": "https://graph/next"})
        assert [i["id"] for i in page.items] == [ROOT["id"]]
        assert page.next_link == "https://graph/next" and page.delta_link is None
        last = parse_delta_page({"value": [], "@odata.deltaLink": "https://graph/delta?token=1"})
        assert last.items == [] and last.next_link is None and last.delta_link == "https://graph/delta?token=1"
        assert parse_delta_page({}).items == []

    def test_classify_delta_items(self):
        other_root = _msg("1700000000900", BOB, "Bob", "2024-05-02T10:00:00Z", "new thread")
        deleted_root = _msg("1700000000800", BOB, "Bob", "2024-05-02T10:00:00Z", "", deletedDateTime="2024-05-02T11:00:00Z")
        orphan_reply = _msg("1700000000950", BOB, "Bob", "2024-05-02T10:00:00Z", "reply", replyToId="1700000000700")
        changes = classify_delta_items([REPLY_LATE, other_root, deleted_root, REPLY_DELETED, orphan_reply, {"noid": True}])
        # replies (added / edited / deleted) dirty their root; roots carried by the page are cached
        assert changes.dirty_root_ids == [ROOT["id"], other_root["id"], "1700000000700"]
        assert changes.deleted_root_ids == [deleted_root["id"]]
        assert set(changes.roots) == {other_root["id"]}

    def test_deleting_a_root_wins_over_a_dirty_mark_and_folds_across_pages(self):
        changes = classify_delta_items([ROOT, REPLY_EARLY])
        assert changes.dirty_root_ids == [ROOT["id"]] and ROOT["id"] in changes.roots
        classify_delta_items([{**ROOT, "deletedDateTime": "2024-05-03T00:00:00Z"}], changes)
        assert changes.dirty_root_ids == [] and changes.deleted_root_ids == [ROOT["id"]] and changes.roots == {}
        # a later reply to a deleted root must not resurrect it within the same run
        classify_delta_items([REPLY_LATE], changes)
        assert changes.dirty_root_ids == []
        assert isinstance(changes, DeltaChanges)

    def test_sync_point_cursor_roundtrip(self):
        data = delta_sync_point_data("https://graph/delta?token=abc", 1714558530000)
        assert data == {"deltaLink": "https://graph/delta?token=abc", "lastSyncTimestamp": 1714558530000}
        assert read_delta_link(data) == "https://graph/delta?token=abc"
        assert read_last_sync_ms(data) == 1714558530000
        fallback = delta_sync_point_data(None, 5)
        assert read_delta_link(fallback) is None and read_last_sync_ms(fallback) == 5
        assert read_delta_link(None) is None and read_delta_link({}) is None
        assert read_last_sync_ms({"lastSyncTimestamp": "bad"}) is None

    def test_delta_unsupported_statuses(self):
        assert all(is_delta_unsupported_status(s) for s in (400, 404, 501))
        assert not any(is_delta_unsupported_status(s) for s in (401, 403, 429, 500, 503))


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
