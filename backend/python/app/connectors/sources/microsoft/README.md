# Microsoft connectors — change notifications (Layer 2)

The six Microsoft connectors (OneDrive, SharePoint Online, Outlook, Teams, Dynamics 365
Sales, Business Central) poll on their schedule (2-minute interval in Edrak's
deployments). Layer 2 adds an event-driven fast path: when Microsoft reports a change,
the affected connector instance runs an **incremental** sync within seconds. Polling is
untouched and stays the safety net — every part of this layer degrades to it.

## Flow

```
Microsoft  ──► edrak-ai (public receiver, FRONTEND_PUBLIC_URL)  ──► CGraph Node gateway  ──► connectors service
              /api/webhooks/microsoft/{graph|dataverse|bc}/{id}     POST /api/v1/connectors/internal/{id}/notify
                                                                     (scoped JWT, scope connector:notify)
```

1. The fork registers subscriptions/webhooks pointing at edrak-ai, one per connector instance:
   * Graph: `{FRONTEND_PUBLIC_URL}/api/webhooks/microsoft/graph/{connectorId}` (edrak-ai answers the
     `validationToken` handshake; notifications carry `clientState`).
   * Dataverse: `{FRONTEND_PUBLIC_URL}/api/webhooks/microsoft/dataverse/{connectorId}` (header
     `x-edrak-webhook-key`).
   * Business Central: `{FRONTEND_PUBLIC_URL}/api/webhooks/microsoft/bc/{connectorId}` (handshake echo by
     edrak-ai; notifications carry `clientState`).
2. edrak-ai verifies the secret and forwards to the fork:
   `POST /api/v1/connectors/internal/{connectorId}/notify` with
   `Authorization: Bearer <HS256 JWT, scopes ["connector:notify"], issuer "edrak-ai", exp ≤ 5 min>` and body
   `{"source": "graph"|"dataverse"|"bc", "events": [{resource, changeType, subscriptionId, entity, recordId, message}]}`.
   The Node gateway (`connectors.routes.ts`, `scopedTokenValidator(CONNECTOR_NOTIFY)`) forwards it verbatim to the
   Python connectors service (`app/connectors/api/notify_router.py`).
3. The connectors service replies `202 {"accepted": true, "scheduled": true|false}` — `401` bad token, `404`
   unknown/inactive connector, `400` bad body. Notifications for one connector are coalesced
   (`app/connectors/services/notify_service.py`): the first one claims Redis key `cgraph:notify:{connectorId}`
   (SET NX, 25 s) and schedules a run ~10 s later; later ones in the window, or while a run is still pending in
   this process, return `scheduled=false`. The run is a `<connector>.resync` event on the `sync-events` topic
   with `incremental: true` — the very path the Node scheduler uses — so `EventService` applies its normal
   `start_if_idle` serialisation and calls `run_incremental_sync()`. If a sync is running when the delay
   elapses, the publish waits for it to finish (poll 5 s, max 30 min) so mid-sync changes are not lost.

## Shared secret

`expected(connectorId) = hex(HMAC-SHA256(key = scopedJwtSecret, msg = "ms-webhook:" + connectorId))[:40]`

`scopedJwtSecret` is the value already shared with edrak-ai (persisted by
`cm.service.ts` into the KV store under `/services/secretKeys`). It is used as Graph `clientState`, the Dataverse
`x-edrak-webhook-key` header value and the BC `clientState`
(`common/change_notifications.py::webhook_client_state`).

## Registrations per connector

Graph connectors go through `GraphSubscriptionManager` (`common/change_notifications.py`, plain HTTPS, no Graph
SDK). After every successful `run_sync` / `run_incremental_sync` they call `ensure()` for the resources that run
touched and `renew_expiring(within_minutes=120)`; registrations live in the connector's records sync point under
key `webhooks` (`[{id, resource, changeType, expiresAt, maxMinutes}]`). When that state is missing (a full sync
deletes sync points) the manager lists the app's Graph subscriptions and adopts those whose `notificationUrl`
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

## Teardown and fallback

`remove_change_notifications()` runs best-effort when a connector is deleted (`EventService._handle_delete`) or
its sync is disabled (`POST /api/v1/connectors/{id}/toggle`). Registration or renewal errors never fail a sync —
they are logged as warnings. Without `FRONTEND_PUBLIC_URL` or `scopedJwtSecret` nothing is registered and the
connectors simply poll. Notifications for a connector that is inactive or gone get `404` from `/notify`, which
edrak-ai can use to drop the stale subscription.
