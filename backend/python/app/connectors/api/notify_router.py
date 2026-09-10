"""Change-notification intake: ``POST /api/v1/connectors/internal/{connector_id}/notify``
and ``POST /api/v1/connectors/internal/notify-by-resource``.

Service-to-service only: edrak-ai (the public receiver) forwards Microsoft Graph /
Dataverse / Business Central / Google Drive notifications to the per-connector route,
and Gmail Pub/Sub messages — which name a mailbox, not a connector — to the
resource-keyed route, with a scoped JWT (``connector:notify``). There is no user, so
this router deliberately bypasses ``authMiddleware``.

Per-connector route: ``202 {"accepted": true, "scheduled": bool}``; ``scheduled`` is
false when the notification was coalesced into a run that is already pending. 401 bad
token, 404 unknown / inactive connector, 400 malformed body.

Resource-keyed route: ``202 {"accepted": true, "connectors": n, "scheduled": m}`` where
``connectors`` counts the active connectors the reverse index maps the mailbox to and
``scheduled`` how many new runs that produced. An unknown mailbox is ``connectors: 0``,
still 202 — Pub/Sub retries anything else.
"""

from __future__ import annotations

from typing import Any, Literal, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from app.api.middlewares.auth import require_internal_scope
from app.config.constants.arangodb import CollectionNames
from app.config.constants.http_status_code import HttpStatusCode
from app.config.constants.service import TokenScopes
from app.connectors.core.constants import ConnectorStateKeys
from app.connectors.services.notify_service import notify_scheduler
from app.connectors.sources.google.common.push_notifications import GmailReverseIndex

router = APIRouter()

NotifySource = Literal["graph", "dataverse", "bc", "google-drive", "gmail"]
ResourceNotifySource = Literal["gmail"]
BodyModel = TypeVar("BodyModel", bound=BaseModel)


class NotifyEvent(BaseModel):
    resource: str | None = None
    changeType: str | None = None
    subscriptionId: str | None = None
    entity: str | None = None
    recordId: str | None = None
    message: str | None = None


class NotifyRequest(BaseModel):
    source: NotifySource
    events: list[NotifyEvent] = Field(default_factory=list)


class NotifyResponse(BaseModel):
    accepted: bool = True
    scheduled: bool


class ResourceNotifyRequest(BaseModel):
    source: ResourceNotifySource
    resourceKey: str = Field(min_length=1)
    events: list[NotifyEvent] = Field(default_factory=list)


class ResourceNotifyResponse(BaseModel):
    accepted: bool = True
    connectors: int
    scheduled: int


async def _parse_body(request: Request, model: type[BodyModel]) -> BodyModel:
    try:
        raw = await request.json()
    except ValueError as e:
        raise HTTPException(status_code=HttpStatusCode.BAD_REQUEST.value, detail="Body must be JSON") from e
    try:
        return model.model_validate(raw)
    except ValidationError as e:
        raise HTTPException(status_code=HttpStatusCode.BAD_REQUEST.value, detail=e.errors(include_url=False)) from e


async def _active_connector(request: Request, connector_id: str) -> tuple[str, str] | None:
    """``(orgId, type)`` of an active, authenticated connector instance, else ``None``."""
    document = await request.app.state.graph_provider.get_document(connector_id, CollectionNames.APPS.value)
    if (
        not document
        or document.get(ConnectorStateKeys.IS_ACTIVE) is not True
        or document.get(ConnectorStateKeys.IS_AUTHENTICATED) is False
    ):
        return None
    org_id = document.get("orgId")
    connector_type = document.get("type")
    if not org_id or not connector_type:
        return None
    return str(org_id), str(connector_type)


async def _schedule(request: Request, connector_id: str, org_id: str, connector_type: str, source: str) -> bool:
    container = request.app.container
    return await notify_scheduler.request(
        connector_id=connector_id,
        org_id=org_id,
        connector_type=connector_type,
        source=source,
        kv_store=container.key_value_store(),
        publish=container.kafka_service().publish_event,
        logger=container.logger(),
    )


@router.post(
    "/api/v1/connectors/internal/notify-by-resource",
    status_code=HttpStatusCode.ACCEPTED.value,
    response_model=ResourceNotifyResponse,
)
async def notify_resource_change(
    request: Request,
    _token: dict[str, Any] = Depends(require_internal_scope(TokenScopes.CONNECTOR_NOTIFY.value)),
) -> JSONResponse:
    body = await _parse_body(request, ResourceNotifyRequest)
    container = request.app.container
    logger = container.logger()

    connector_ids = await GmailReverseIndex(container.key_value_store()).lookup(body.resourceKey)
    connectors = scheduled = 0
    for connector_id in connector_ids:
        resolved = await _active_connector(request, connector_id)
        if resolved is None:
            logger.debug("notify-by-resource: connector %s for %s is gone or inactive", connector_id, body.resourceKey)
            continue
        connectors += 1
        if await _schedule(request, connector_id, resolved[0], resolved[1], body.source):
            scheduled += 1
    logger.info(
        "notify-by-resource: %s event(s) from %s for %s -> %d connector(s), scheduled=%d",
        len(body.events), body.source, body.resourceKey, connectors, scheduled,
    )
    return JSONResponse(
        status_code=HttpStatusCode.ACCEPTED.value,
        content=ResourceNotifyResponse(connectors=connectors, scheduled=scheduled).model_dump(),
    )


@router.post(
    "/api/v1/connectors/internal/{connector_id}/notify",
    status_code=HttpStatusCode.ACCEPTED.value,
    response_model=NotifyResponse,
)
async def notify_connector_change(
    connector_id: str,
    request: Request,
    _token: dict[str, Any] = Depends(require_internal_scope(TokenScopes.CONNECTOR_NOTIFY.value)),
) -> JSONResponse:
    body = await _parse_body(request, NotifyRequest)
    logger = request.app.container.logger()

    resolved = await _active_connector(request, connector_id)
    if resolved is None:
        raise HTTPException(status_code=HttpStatusCode.NOT_FOUND.value, detail="Connector not found or inactive")
    org_id, connector_type = resolved

    scheduled = await _schedule(request, connector_id, org_id, connector_type, body.source)
    logger.info(
        "notify: %s event(s) from %s for connector %s (%s) -> scheduled=%s",
        len(body.events), body.source, connector_id, connector_type, scheduled,
    )
    return JSONResponse(
        status_code=HttpStatusCode.ACCEPTED.value,
        content=NotifyResponse(scheduled=scheduled).model_dump(),
    )
