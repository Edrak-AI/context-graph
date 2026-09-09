"""Dataverse webhook registration for the Dynamics 365 connector (Layer 2).

Dataverse has no subscription API; instead a *service endpoint* (contract Webhook)
is registered and *SDK message processing steps* bind it to messages on tables.
Everything Dataverse-shaped is here as pure helpers so it can be unit-tested; the
connector supplies the HTTP callables.

Registered per connector instance (app-only, needs the System Administrator role
on the application user — ``prvCreateServiceEndpoint`` / ``prvCreateSdkMessageProcessingStep``):

* one ``serviceendpoint`` named ``Edrak CGraph <connector_id>``: ``contract=8``
  (Webhook), ``authtype=4`` (HttpHeader) with ``authvalue`` carrying the
  ``x-edrak-webhook-key`` header, ``url`` = edrak-ai's dataverse receiver;
* one asynchronous post-operation step (``mode=1``, ``stage=40``,
  ``supporteddeployment=0``) per synced table × message
  (Create/Update/Delete/Assign/GrantAccess/ModifyAccess/RevokeAccess) plus
  Associate/Disassociate without an entity filter (team / role membership).

Idempotent: everything is looked up by name before it is created. A 403 means the
application user lacks the privilege — logged once, polling remains.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from app.config.constants.http_status_code import HttpStatusCode

if TYPE_CHECKING:
    from logging import Logger

WEBHOOK_HEADER = "x-edrak-webhook-key"
ENDPOINT_NAME_PREFIX = "Edrak CGraph"

SERVICE_ENDPOINT_CONTRACT_WEBHOOK = 8
SERVICE_ENDPOINT_AUTHTYPE_HTTP_HEADER = 4
SERVICE_ENDPOINT_MESSAGE_FORMAT_JSON = 2
STEP_MODE_ASYNC = 1
STEP_STAGE_POST_OPERATION = 40
STEP_SUPPORTED_DEPLOYMENT_SERVER = 0

ENTITY_MESSAGES: tuple[str, ...] = (
    "Create", "Update", "Delete", "Assign", "GrantAccess", "ModifyAccess", "RevokeAccess",
)
GLOBAL_MESSAGES: tuple[str, ...] = ("Associate", "Disassociate")
WEBHOOK_TABLES: tuple[str, ...] = ("account", "contact", "lead", "opportunity", "incident", "annotation")

GetJson = Callable[[str, dict[str, str] | None], Awaitable[dict[str, Any]]]
SendJson = Callable[[str, str, dict[str, Any] | None], Awaitable[tuple[int, dict[str, Any]]]]


class DataverseWebhookPrivilegeError(Exception):
    """The application user may not manage service endpoints / processing steps."""


def endpoint_name(connector_id: str) -> str:
    return f"{ENDPOINT_NAME_PREFIX} {connector_id}"


def step_name(connector_id: str, message: str, entity: str | None) -> str:
    suffix = f" {entity}" if entity else ""
    return f"{ENDPOINT_NAME_PREFIX} {connector_id}: {message}{suffix}"


def odata_string(value: str) -> str:
    """Quote a string literal for ``$filter`` (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class StepPlan:
    message: str
    entity: str | None
    name: str


def plan_steps(connector_id: str, tables: Sequence[str] = WEBHOOK_TABLES) -> list[StepPlan]:
    """Every step the endpoint should have: entity messages per table, then the global ones."""
    plans = [
        StepPlan(message, table, step_name(connector_id, message, table))
        for table in tables
        for message in ENTITY_MESSAGES
    ]
    plans.extend(StepPlan(message, None, step_name(connector_id, message, None)) for message in GLOBAL_MESSAGES)
    return plans


def header_auth_value(header_value: str) -> str:
    return json.dumps({WEBHOOK_HEADER: header_value})


def service_endpoint_body(connector_id: str, url: str, header_value: str) -> dict[str, Any]:
    return {
        "name": endpoint_name(connector_id),
        "url": url,
        "contract": SERVICE_ENDPOINT_CONTRACT_WEBHOOK,
        "authtype": SERVICE_ENDPOINT_AUTHTYPE_HTTP_HEADER,
        "authvalue": header_auth_value(header_value),
        "messageformat": SERVICE_ENDPOINT_MESSAGE_FORMAT_JSON,
    }


def step_body(plan: StepPlan, endpoint_id: str, message_id: str, filter_id: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": plan.name,
        "mode": STEP_MODE_ASYNC,
        "stage": STEP_STAGE_POST_OPERATION,
        "supporteddeployment": STEP_SUPPORTED_DEPLOYMENT_SERVER,
        "rank": 1,
        # async steps only: keeps the System Job table from filling with succeeded jobs
        "asyncautodelete": True,
        "eventhandler_serviceendpoint@odata.bind": f"/serviceendpoints({endpoint_id})",
        "sdkmessageid@odata.bind": f"/sdkmessages({message_id})",
    }
    if filter_id:
        body["sdkmessagefilterid@odata.bind"] = f"/sdkmessagefilters({filter_id})"
    return body


def endpoint_query(connector_id: str) -> tuple[str, dict[str, str]]:
    return "serviceendpoints", {
        "$select": "serviceendpointid,name,url",
        "$filter": f"name eq {odata_string(endpoint_name(connector_id))}",
    }


def steps_query(endpoint_id: str) -> tuple[str, dict[str, str]]:
    return "sdkmessageprocessingsteps", {
        "$select": "sdkmessageprocessingstepid,name",
        "$filter": f"_eventhandler_value eq {endpoint_id}",
    }


def message_query(message: str) -> tuple[str, dict[str, str]]:
    return "sdkmessages", {"$select": "sdkmessageid,name", "$filter": f"name eq {odata_string(message)}"}


def message_filter_query(message_id: str, entity: str) -> tuple[str, dict[str, str]]:
    return "sdkmessagefilters", {
        "$select": "sdkmessagefilterid",
        "$filter": f"_sdkmessageid_value eq {message_id} and primaryobjecttypecode eq {odata_string(entity)}",
    }


def entity_ref(entity_set: str, row_id: str) -> str:
    return f"{entity_set}({quote(row_id, safe='')})"


def is_privilege_error(status_code: int) -> bool:
    return status_code in (HttpStatusCode.FORBIDDEN.value, HttpStatusCode.UNAUTHORIZED.value)


def _first_row(payload: dict[str, Any]) -> dict[str, Any] | None:
    rows = payload.get("value") if isinstance(payload, dict) else None
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                return row
    return None


class DataverseWebhookRegistrar:
    """Ensures / removes the service endpoint and its steps for one connector instance."""

    def __init__(
        self,
        *,
        get_json: GetJson,
        send_json: SendJson,
        connector_id: str,
        notification_url: str,
        header_value: str,
        logger: Logger,
        tables: Sequence[str] = WEBHOOK_TABLES,
    ) -> None:
        self._get_json = get_json
        self._send_json = send_json
        self.connector_id = connector_id
        self.notification_url = notification_url
        self.header_value = header_value
        self.logger = logger
        self.tables = tuple(tables)
        self._message_ids: dict[str, str | None] = {}

    async def _send(self, method: str, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        status, payload = await self._send_json(method, path, body)
        if is_privilege_error(status):
            raise DataverseWebhookPrivilegeError(
                f"{method} {path} -> HTTP {status}: {json.dumps(payload)[:300]}"
            )
        if status >= 400:
            raise RuntimeError(f"Dataverse {method} {path} failed with HTTP {status}: {json.dumps(payload)[:300]}")
        return payload

    async def _find_endpoint(self) -> dict[str, Any] | None:
        path, params = endpoint_query(self.connector_id)
        return _first_row(await self._get_json(path, params))

    async def ensure_endpoint(self) -> str:
        """Create the service endpoint (or re-point an existing one) and return its id."""
        body = service_endpoint_body(self.connector_id, self.notification_url, self.header_value)
        existing = await self._find_endpoint()
        if existing and existing.get("serviceendpointid"):
            endpoint_id = str(existing["serviceendpointid"])
            if existing.get("url") != self.notification_url:
                # authvalue is write-only, so it is refreshed together with the URL.
                await self._send("PATCH", entity_ref("serviceendpoints", endpoint_id), {"url": body["url"], "authvalue": body["authvalue"]})
                self.logger.info("Dataverse service endpoint %s re-pointed to %s", endpoint_id, self.notification_url)
            return endpoint_id
        created = await self._send("POST", "serviceendpoints", body)
        endpoint_id = created.get("serviceendpointid")
        if not endpoint_id:
            existing = await self._find_endpoint()
            endpoint_id = (existing or {}).get("serviceendpointid")
        if not endpoint_id:
            raise RuntimeError("Dataverse did not return the id of the created service endpoint")
        self.logger.info("Dataverse service endpoint %s created for connector %s", endpoint_id, self.connector_id)
        return str(endpoint_id)

    async def _message_id(self, message: str) -> str | None:
        if message not in self._message_ids:
            path, params = message_query(message)
            row = _first_row(await self._get_json(path, params))
            self._message_ids[message] = str(row["sdkmessageid"]) if row and row.get("sdkmessageid") else None
        return self._message_ids[message]

    async def _filter_id(self, message_id: str, entity: str) -> str | None:
        path, params = message_filter_query(message_id, entity)
        row = _first_row(await self._get_json(path, params))
        return str(row["sdkmessagefilterid"]) if row and row.get("sdkmessagefilterid") else None

    async def ensure_steps(self, endpoint_id: str) -> int:
        """Create the missing processing steps bound to the endpoint; returns how many were created."""
        path, params = steps_query(endpoint_id)
        existing_names = {
            row.get("name") for row in (await self._get_json(path, params)).get("value") or [] if isinstance(row, dict)
        }
        created = 0
        for plan in plan_steps(self.connector_id, self.tables):
            if plan.name in existing_names:
                continue
            message_id = await self._message_id(plan.message)
            if not message_id:
                self.logger.info("Dataverse has no SDK message %s; skipping that webhook step", plan.message)
                continue
            filter_id: str | None = None
            if plan.entity:
                filter_id = await self._filter_id(message_id, plan.entity)
                if not filter_id:
                    self.logger.info("Dataverse message %s is not available for table %s; skipping", plan.message, plan.entity)
                    continue
            await self._send("POST", "sdkmessageprocessingsteps", step_body(plan, endpoint_id, message_id, filter_id))
            created += 1
        if created:
            self.logger.info("Registered %d Dataverse webhook step(s) for connector %s", created, self.connector_id)
        return created

    async def ensure(self) -> str | None:
        """Endpoint + steps; returns the endpoint id, or ``None`` when the app user lacks the privilege."""
        try:
            endpoint_id = await self.ensure_endpoint()
            await self.ensure_steps(endpoint_id)
            return endpoint_id
        except DataverseWebhookPrivilegeError as e:
            self.logger.warning(
                "Dataverse refused to register webhooks for connector %s: the application user needs the "
                "System Administrator role (service endpoint and SDK message processing step privileges). "
                "Falling back to polling. (%s)",
                self.connector_id, str(e)[:300],
            )
            return None

    async def remove_all(self) -> None:
        """Delete the steps bound to the endpoint, then the endpoint itself (best effort)."""
        existing = await self._find_endpoint()
        endpoint_id = (existing or {}).get("serviceendpointid")
        if not endpoint_id:
            return
        endpoint_id = str(endpoint_id)
        path, params = steps_query(endpoint_id)
        for row in (await self._get_json(path, params)).get("value") or []:
            step_id = row.get("sdkmessageprocessingstepid") if isinstance(row, dict) else None
            if step_id:
                await self._send("DELETE", entity_ref("sdkmessageprocessingsteps", str(step_id)), None)
        await self._send("DELETE", entity_ref("serviceendpoints", endpoint_id), None)
        self.logger.info("Removed Dataverse webhook registration for connector %s", self.connector_id)
