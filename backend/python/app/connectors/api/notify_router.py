"""``POST /api/v1/connectors/internal/{connector_id}/notify`` — Microsoft change notifications.

Service-to-service only: edrak-ai (the public receiver) forwards Graph / Dataverse /
Business Central notifications here with a scoped JWT (``connector:notify``). There
is no user, so this router deliberately bypasses ``authMiddleware``.

Replies ``202 {"accepted": true, "scheduled": bool}``; ``scheduled`` is false when the
notification was coalesced into a run that is already pending. 401 bad token, 404
unknown / inactive connector, 400 malformed body.
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from app.api.middlewares.auth import require_internal_scope
from app.config.constants.arangodb import CollectionNames
from app.config.constants.http_status_code import HttpStatusCode
from app.config.constants.service import TokenScopes
from app.connectors.core.constants import ConnectorStateKeys
from app.connectors.services.notify_service import notify_scheduler

router = APIRouter()

NotifySource = Literal["graph", "dataverse", "bc"]


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


async def _parse_notify_body(request: Request) -> NotifyRequest:
    try:
        raw = await request.json()
    except ValueError as e:
        raise HTTPException(status_code=HttpStatusCode.BAD_REQUEST.value, detail="Body must be JSON") from e
    try:
        return NotifyRequest.model_validate(raw)
    except ValidationError as e:
        raise HTTPException(status_code=HttpStatusCode.BAD_REQUEST.value, detail=e.errors(include_url=False)) from e


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
    body = await _parse_notify_body(request)
    container = request.app.container
    logger = container.logger()

    document = await request.app.state.graph_provider.get_document(connector_id, CollectionNames.APPS.value)
    if (
        not document
        or document.get(ConnectorStateKeys.IS_ACTIVE) is not True
        or document.get(ConnectorStateKeys.IS_AUTHENTICATED) is False
    ):
        raise HTTPException(status_code=HttpStatusCode.NOT_FOUND.value, detail="Connector not found or inactive")
    org_id = document.get("orgId")
    connector_type = document.get("type")
    if not org_id or not connector_type:
        raise HTTPException(status_code=HttpStatusCode.NOT_FOUND.value, detail="Connector not found or inactive")

    kafka_service = container.kafka_service()
    scheduled = await notify_scheduler.request(
        connector_id=connector_id,
        org_id=str(org_id),
        connector_type=str(connector_type),
        source=body.source,
        kv_store=container.key_value_store(),
        publish=kafka_service.publish_event,
        logger=logger,
    )
    logger.info(
        "notify: %s event(s) from %s for connector %s (%s) -> scheduled=%s",
        len(body.events), body.source, connector_id, connector_type, scheduled,
    )
    return JSONResponse(
        status_code=HttpStatusCode.ACCEPTED.value,
        content=NotifyResponse(scheduled=scheduled).model_dump(),
    )
