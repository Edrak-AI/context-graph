# Change notifications (Layer 2) — Microsoft and Google connectors

The six Microsoft connectors (OneDrive, SharePoint Online, Outlook, Teams, Dynamics 365 Sales, Business
Central) and the four Google connectors (Google Drive individual/workspace, Gmail individual/workspace)
poll on their schedule (2-minute interval in Edrak's deployments). Layer 2 adds an event-driven fast
path: when the provider reports a change, the affected connector instance runs an **incremental** sync
within seconds. Polling is untouched and stays the safety net — every part of this layer degrades to it.

## Flow

```
Microsoft / Google Drive ──► edrak-ai (public receiver, FRONTEND_PUBLIC_URL) ──► CGraph Node gateway ──► connectors service
                              /api/webhooks/microsoft/{graph|dataverse|bc}/{id}   POST /api/v1/connectors/internal/{id}/notify
                              /api/webhooks/google/drive/{id}                     (scoped JWT, scope connector:notify)

Gmail ──► Cloud Pub/Sub topic ──► edrak-ai (push subscription endpoint) ──► CGraph Node gateway ──► connectors service
                                                                            POST /api/v1/connectors/internal/notify-by-resource
```

1. The fork registers subscriptions/webhooks pointing at edrak-ai, one per connector instance (Google: one per
   connector instance × user):
   * Graph: `{FRONTEND_PUBLIC_URL}/api/webhooks/microsoft/graph/{connectorId}` (edrak-ai answers the
     `validationToken` handshake; notifications carry `clientState`).
   * Dataverse: `{FRONTEND_PUBLIC_URL}/api/webhooks/microsoft/dataverse/{connectorId}` (header
     `x-edrak-webhook-key`).
   * Business Central: `{FRONTEND_PUBLIC_URL}/api/webhooks/microsoft/bc/{connectorId}` (handshake echo by
     edrak-ai; notifications carry `clientState`).
   * Google Drive: `{FRONTEND_PUBLIC_URL}/api/webhooks/google/drive/{connectorId}` (channel `token`; Google sends
     it back as `X-Goog-Channel-Token`, plus `X-Goog-Channel-ID`, `X-Goog-Resource-ID`, `X-Goog-Resource-State` —
     `sync` on channel creation is ignored, `change` is forwarded).
   * Gmail: no URL — `users.watch` publishes `{emailAddress, historyId}` to the Pub/Sub topic in
     `GOOGLE_PUBSUB_TOPIC`; edrak-ai owns the push subscription.
2. edrak-ai verifies the secret and forwards to the fork with
   `Authorization: Bearer <HS256 JWT, scopes ["connector:notify"], issuer "edrak-ai", exp ≤ 5 min>`:
   * per connector: `POST /api/v1/connectors/internal/{connectorId}/notify`, body
     `{"source": "graph"|"dataverse"|"bc"|"google-drive"|"gmail", "events": [{resource, changeType, subscriptionId, entity, recordId, message}]}`;
   * per mailbox (Gmail): `POST /api/v1/connectors/internal/notify-by-resource`, body
     `{"source": "gmail", "resourceKey": "<emailAddress>", "events": [{resource, changeType, recordId}]}`.
   The Node gateway (`connectors.routes.ts`, `scopedTokenValidator(CONNECTOR_NOTIFY)`) forwards both verbatim to
   the Python connectors service (`app/connectors/api/notify_router.py`).
3. The connectors service replies:
   * `/notify`: `202 {"accepted": true, "scheduled": true|false}` — `401` bad token, `404` unknown/inactive
     connector, `400` bad body.
   * `/notify-by-resource`: `202 {"accepted": true, "connectors": n, "scheduled": m}` where `connectors` is the
     number of **active** connector instances the reverse index maps the mailbox to and `scheduled` how many new
     runs that produced (the rest were coalesced). An unknown mailbox answers `connectors: 0`, still `202` — Pub/Sub
     retries any non-2xx. `401` bad token, `400` bad body.

   Notifications for one connector are coalesced (`app/connectors/services/notify_service.py`): the first one claims
   Redis key `cgraph:notify:{connectorId}` (SET NX, 25 s) and schedules a run ~10 s later; later ones in the window,
   or while a run is still pending in this process, return `scheduled=false`. The run is a `<connector>.resync`
   event on the `sync-events` topic with `incremental: true` — the very path the Node scheduler uses — so
   `EventService` applies its normal `start_if_idle` serialisation and calls `run_incremental_sync()`. If a sync is
   running when the delay elapses, the publish waits for it to finish (poll 5 s, max 30 min) so mid-sync changes are
   not lost.

## Shared secret

* Microsoft: `expected(connectorId) = hex(HMAC-SHA256(key = scopedJwtSecret, msg = "ms-webhook:" + connectorId))[:40]`
  — Graph `clientState`, Dataverse `x-edrak-webhook-key`, BC `clientState`
  (`microsoft/common/change_notifications.py::webhook_client_state`).
* Google Drive: `expected(connectorId) = hex(HMAC-SHA256(key = scopedJwtSecret, msg = "google-webhook:" + connectorId))[:40]`
  — the channel `token` (`google/common/push_notifications.py::webhook_channel_token`).
* Gmail: none at this layer; Pub/Sub push authentication (OIDC token on the push subscription) is edrak-ai's concern.

`scopedJwtSecret` is the value already shared with edrak-ai (persisted by `cm.service.ts` into the KV store under
`/services/secretKeys`).

## Registrations per connector

Every registry lives in the connector's records sync point under key `webhooks`
(`app/connectors/core/base/webhooks/subscription_store.py`, row shape per provider below). A full sync that deletes
sync points loses it; the managers cope (Microsoft adopts existing Graph subscriptions, Google simply creates new
channels/watches and lets orphans lapse).

### Microsoft

Graph connectors go through `GraphSubscriptionManager` (`microsoft/common/change_notifications.py`, plain HTTPS, no
Graph SDK). After every successful `run_sync` / `run_incremental_sync` they call `ensure()` for the resources that
run touched and `renew_expiring(within_minutes=120)`; rows are `[{id, resource, changeType, expiresAt, maxMinutes}]`.
When that state is missing the manager lists the app's Graph subscriptions and adopts those whose `notificationUrl`
matches. A renew that returns 404 recreates the subscription; 403 is logged once and the resource is skipped.

| Connector | Resource | changeType | Max lifetime |
| --- | --- | --- | --- |
| OneDrive | `users/{userId}/drive/root` per synced drive | `updated` | 4230 min (≈3 d) |
| SharePoint Online | `sites/{siteId}/drives/{driveId}/root` per synced library | `updated` | 4230 min |
| Outlook | `users/{userId}/messages` per processed mailbox | `created,updated,deleted` | 4230 min |
| Teams (team scope) | `teams/{teamId}/channels/{channelId}/messages` per synced channel | `created,updated,deleted` | 60 min |
| Teams (team scope) | `groups` (membership) | `updated` | 41 d |

Teams channel-message subscriptions need `ChannelMessage.Read.All`, a protected API; without Microsoft's
approval Graph answers 403 and polling remains. Personal-scope Teams connectors register nothing.

**Dynamics 365** (`dynamics365/webhooks.py`, app-only, the application user needs System Administrator):
one `serviceendpoint` named `Edrak CGraph <connectorId>` (`contract=8` Webhook, `authtype=4` HttpHeader,
`authvalue={"x-edrak-webhook-key": expected}`, `messageformat=2` JSON) and asynchronous post-operation
`sdkmessageprocessingsteps` (`mode=1`, `stage=40`, `supporteddeployment=0`, `asyncautodelete`) for
Create/Update/Delete/Assign/GrantAccess/ModifyAccess/RevokeAccess on each synced table (account, contact, lead,
opportunity, incident, annotation) plus Associate/Disassociate without an entity filter (team/role membership).
Everything is looked up by name first, so re-runs are idempotent; the endpoint URL (and header) is re-pointed when
`FRONTEND_PUBLIC_URL` changes. Verified after a run once per process and then every 6 h. A 403 logs a clear
message and the connector keeps polling. `remove_all()` deletes the steps, then the endpoint.

**Business Central** (`business_central/webhooks.py`): `POST /api/v2.0/subscriptions
{notificationUrl, resource: "/api/v2.0/companies(<id>)/<entitySet>", clientState}` per synced company × entity
set; BC's maximum lifetime is 3 days, renewed with `PATCH subscriptions('{id}')` + `If-Match: *` when less than
12 h remain (404 ⇒ recreate). Same sync-point registry and adoption logic as Graph.

### Google Drive (`google/common/push_notifications.py::DriveChannelManager`)

One `changes.watch` channel per (connector instance, user): the individual connector watches the OAuth user's
changes feed, the workspace connector impersonates each user whose Drive sync succeeded in that run (domain-wide
delegation, `_build_drive_data_source_for_email`). `files.watch` is not used. Request:
`POST changes/watch?pageToken=<the user's stored startPageToken>&supportsAllDrives=true&includeItemsFromAllDrives=true`
with body `{id: <uuid4>, type: "web_hook", address, token, expiration: <ms epoch>}`. Rows are
`[{id, resourceId, expiration, userEmail, stopPending?}]`.

* Lifetime **23 h** — Drive caps a `changes` channel at one day
  (developers.google.com/workspace/drive/api/guides/push, "Renew notification channels"); there is no renew API.
* On every run: channels with **< 2 h** left are renewed by creating the replacement first and then `channels.stop`
  on the old one (`{id, resourceId}`). If the stop fails the old row is kept with `stopPending: true` and retried
  next run, so edrak-ai never sees a channel the fork has forgotten. Channels of users a run did not cover stay
  until they lapse (no page token to renew them with) and are pruned once expired.
* `push.webhookUrlUnauthorized` (HTTP 401 from `changes.watch`) is logged **once per run** with the fix and stops
  further attempts in that run: the GCP project that owns the OAuth client / service account must have the
  notification domain (e.g. `dev.edrak.com`) **verified in Google Search Console** and listed under the OAuth
  consent screen's **authorised domains**. Until then the connector polls.
* Other failures are aggregated into one warning per run; an expiring channel whose replacement failed is kept.
* Google delivers a `sync` message on creation (edrak-ai drops it) and `change` afterwards; the notification carries
  no item details, which is fine — the run walks `changes.list` from the stored page token as usual.

`run_incremental_sync()` on both Drive connectors now delegates to `run_sync()`, which already walks
`changes.list` per user once a page token exists (before this change the workspace connector raised
`NotImplementedError`, so a notification-triggered run would have failed).

### Gmail (`google/common/push_notifications.py::GmailWatchManager`)

One `users.watch` per mailbox (`userId=me` as the OAuth user, or as the impersonated workspace user), body
`{topicName: $GOOGLE_PUBSUB_TOPIC, labelIds: ["INBOX", "SENT"], labelFilterBehavior: "INCLUDE"}`. Rows are
`[{emailAddress, historyId, expiration}]` (values from the watch response).

* `GOOGLE_PUBSUB_TOPIC` = full topic name `projects/<project>/topics/<name>`. **Unset or empty ⇒ Gmail push is
  skipped entirely** with one info log per run; polling remains. The topic must grant
  `gmail-api-push@system.gserviceaccount.com` the Pub/Sub Publisher role, and the mailbox's project must be the one
  the topic lives in (or the topic must be shared), otherwise `users.watch` returns 403/400 — logged as one warning
  per run.
* Gmail sets the expiry itself (**7 days**, developers.google.com/workspace/gmail/api/guides/push, "Renewing mailbox
  watch"); the manager re-calls `users.watch` on every run when **< 24 h** remain (the call is idempotent and simply
  refreshes). Watches of mailboxes a run did not cover are kept until expired, then dropped from the registry and
  the reverse index.
* **Reverse index** (KV store, the same `EncryptedKeyValueStore` the notify route and the coalescing claims use):
  `cgraph:watch:gmail:<lower-cased emailAddress>` → JSON list of connector ids, no TTL. Added when a watch is
  (re)issued, removed on `users.stop` / expiry; the key is deleted when the list becomes empty. Two connector
  instances watching the same mailbox (individual + workspace) are both listed and both get woken. The
  read-modify-write is unlocked; a lost update is healed by the next run's `ensure`.
* `/notify-by-resource` resolves the mailbox through that index and schedules each still-active connector with the
  same coalescing as `/notify`. Messages for mailboxes nobody watches (stale Pub/Sub deliveries after a connector
  was deleted) are acknowledged with `connectors: 0`.

## Teardown and fallback

`remove_change_notifications()` runs best-effort when a connector is deleted (`EventService._handle_delete`) or
its sync is disabled (`POST /api/v1/connectors/{id}/toggle`): Graph/BC delete their subscriptions, Dynamics removes
steps + endpoint, Drive calls `channels.stop` for every live channel, Gmail calls `users.stop` per mailbox and
removes its reverse-index entries. Registration or renewal errors never fail a sync — they are logged as warnings
(one per run). Without `FRONTEND_PUBLIC_URL` or `scopedJwtSecret` nothing is registered for Microsoft or Drive;
without `GOOGLE_PUBSUB_TOPIC` nothing for Gmail; the connectors simply poll. Notifications for a connector that is
inactive or gone get `404` from `/notify`, which edrak-ai can use to drop the stale subscription/channel.

## Identity matching (which platform user a connector's person is)

Sources report people by their **primary** address (Entra primary `SMTP:` proxy / UPN, Google Workspace
`primaryEmail`, permission grantee e-mails), while people may sign in to Edrak with an **alias domain**. The graph
resolves an address against a `User` node in three tiers, first hit wins (`_user_email_match` /
`_user_email_rank` in the Neo4j and Arango providers):

1. `email` — the address the person signs in to Edrak with.
2. `alternateEmails` — linked sign-ins, owned by edrak-ai provisioning (`POST /api/v1/users/internal/provision`,
   replace semantics, unique per org in Mongo).
3. `sourceEmails` — addresses **learned from connector directories**: when an `AppUser` links to a platform user
   (`batch_upsert_app_users`, tried by the source primary then each `AppUser.alternate_emails`), its primary +
   alternates are set-unioned into this list (lower-cased, never the user's own `email`), so repeated syncs and
   several connectors accumulate instead of replacing.

Connectors fill `AppUser.alternate_emails` from their own directory data — no tenant changes needed:

| Connector | Source of alternates |
|---|---|
| Teams, OneDrive, SharePoint Online (`MSGraphClient.get_all_users`), Outlook (`USER_SYNC_SELECT_FIELDS`) | Graph `proxyAddresses` (`smtp:`/`SMTP:`), `otherMails`, `userPrincipalName` via `entra_identity.alternate_addresses` |
| Dynamics 365 Sales | `EntraUserEmailResolver.resolve_identities` (same `getByIds` call) + the Dataverse address |
| Business Central, SAP (`group:` principals) | `EntraGroupResolver.alternates_by_email`, filled while expanding groups |
| Google Drive / Gmail workspace | Directory `users.list` (`projection=full`): `aliases`, `nonEditableAliases`, non-primary `emails[].address` |
| Individual/personal connectors, SharePoint site user lists, Teams roster-only members | none (single address known) |

An `AppUser` that matches nobody still creates the inactive placeholder node as before (with its
`alternateEmails`); when edrak-ai later provisions that person the entity handler adopts the node and moves the
placeholder's address into `sourceEmails`. Uniqueness is enforced only for `email`/`alternateEmails`;
`sourceEmails` are best-effort — an address on two users resolves by the ranking above and logs a warning.
