"""Dataverse webhook planning (pure) and the registrar against a fake Web API."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from app.connectors.sources.microsoft.dynamics365.webhooks import (
    ENTITY_MESSAGES,
    GLOBAL_MESSAGES,
    WEBHOOK_HEADER,
    WEBHOOK_TABLES,
    DataverseWebhookRegistrar,
    StepPlan,
    endpoint_name,
    endpoint_query,
    message_filter_query,
    odata_string,
    plan_steps,
    service_endpoint_body,
    step_body,
    step_name,
)

LOGGER = logging.getLogger("test-dyn-webhooks")
URL = "https://dev.edrak.com/api/webhooks/microsoft/dataverse/conn-1"


class TestPlanning:
    def test_plan_covers_every_table_message_and_the_global_ones(self) -> None:
        plans = plan_steps("conn-1")
        assert len(plans) == len(WEBHOOK_TABLES) * len(ENTITY_MESSAGES) + len(GLOBAL_MESSAGES)
        assert plans[0] == StepPlan("Create", "account", "Edrak CGraph conn-1: Create account")
        assert plans[-2:] == [
            StepPlan("Associate", None, "Edrak CGraph conn-1: Associate"),
            StepPlan("Disassociate", None, "Edrak CGraph conn-1: Disassociate"),
        ]
        assert len({p.name for p in plans}) == len(plans)
        assert plan_steps("conn-1", ["lead"])[0].entity == "lead"

    def test_service_endpoint_body(self) -> None:
        body = service_endpoint_body("conn-1", URL, "abc")
        assert body["name"] == endpoint_name("conn-1") == "Edrak CGraph conn-1"
        assert (body["contract"], body["authtype"], body["messageformat"]) == (8, 4, 2)
        assert json.loads(body["authvalue"]) == {WEBHOOK_HEADER: "abc"}
        assert body["url"] == URL

    def test_step_body_binds_endpoint_message_and_filter(self) -> None:
        plan = StepPlan("Update", "contact", step_name("conn-1", "Update", "contact"))
        body = step_body(plan, "ep-1", "msg-1", "flt-1")
        assert (body["mode"], body["stage"], body["supporteddeployment"]) == (1, 40, 0)
        assert body["eventhandler_serviceendpoint@odata.bind"] == "/serviceendpoints(ep-1)"
        assert body["sdkmessageid@odata.bind"] == "/sdkmessages(msg-1)"
        assert body["sdkmessagefilterid@odata.bind"] == "/sdkmessagefilters(flt-1)"
        assert "sdkmessagefilterid@odata.bind" not in step_body(StepPlan("Associate", None, "x"), "ep-1", "msg-2", None)

    def test_queries_quote_literals(self) -> None:
        assert odata_string("O'Neil") == "'O''Neil'"
        path, params = endpoint_query("conn-1")
        assert path == "serviceendpoints" and params["$filter"] == "name eq 'Edrak CGraph conn-1'"
        _, params = message_filter_query("msg-1", "account")
        assert params["$filter"] == "_sdkmessageid_value eq msg-1 and primaryobjecttypecode eq 'account'"


class FakeDataverse:
    """Enough of the Web API for the registrar: metadata lookups plus endpoint/step CRUD."""

    def __init__(self, *, forbid: bool = False, missing_filters: set[tuple[str, str]] | None = None) -> None:
        self.forbid = forbid
        self.missing_filters = missing_filters or set()
        self.endpoints: dict[str, dict[str, Any]] = {}
        self.steps: dict[str, dict[str, Any]] = {}
        self.writes: list[tuple[str, str]] = []
        self._n = 0

    def _new_id(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}-{self._n}"

    async def get_json(self, path: str, params: dict[str, str] | None) -> dict[str, Any]:
        f = (params or {}).get("$filter", "")
        if path == "serviceendpoints":
            name = f.split("name eq ")[1].strip("'")
            return {"value": [e for e in self.endpoints.values() if e["name"] == name]}
        if path == "sdkmessageprocessingsteps":
            ep = f.split("_eventhandler_value eq ")[1]
            return {"value": [s for s in self.steps.values() if s["_eventhandler_value"] == ep]}
        if path == "sdkmessages":
            name = f.split("name eq ")[1].strip("'")
            return {"value": [{"sdkmessageid": f"msg-{name}", "name": name}]}
        if path == "sdkmessagefilters":
            msg = f.split("_sdkmessageid_value eq ")[1].split(" and ")[0].removeprefix("msg-")
            entity = f.split("primaryobjecttypecode eq ")[1].strip("'")
            if (msg, entity) in self.missing_filters:
                return {"value": []}
            return {"value": [{"sdkmessagefilterid": f"flt-{msg}-{entity}"}]}
        raise AssertionError(path)

    async def send_json(self, method: str, path: str, body: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
        self.writes.append((method, path))
        if self.forbid:
            return 403, {"error": {"code": "0x80040220", "message": "Principal user is missing prvCreateServiceEndpoint"}}
        if method == "POST" and path == "serviceendpoints":
            row = {"serviceendpointid": self._new_id("ep"), **(body or {})}
            self.endpoints[row["serviceendpointid"]] = row
            return 201, row
        if method == "POST" and path == "sdkmessageprocessingsteps":
            ep = (body or {})["eventhandler_serviceendpoint@odata.bind"].removeprefix("/serviceendpoints(").rstrip(")")
            row = {"sdkmessageprocessingstepid": self._new_id("step"), "name": (body or {})["name"], "_eventhandler_value": ep}
            self.steps[row["sdkmessageprocessingstepid"]] = row
            return 201, row
        if method == "PATCH" and path.startswith("serviceendpoints("):
            self.endpoints[path[len("serviceendpoints("):-1]].update(body or {})
            return 204, {}
        if method == "DELETE" and path.startswith("sdkmessageprocessingsteps("):
            del self.steps[path[len("sdkmessageprocessingsteps("):-1]]
            return 204, {}
        if method == "DELETE" and path.startswith("serviceendpoints("):
            del self.endpoints[path[len("serviceendpoints("):-1]]
            return 204, {}
        raise AssertionError((method, path))


def _registrar(api: FakeDataverse, url: str = URL, tables: list[str] | None = None) -> DataverseWebhookRegistrar:
    return DataverseWebhookRegistrar(
        get_json=api.get_json, send_json=api.send_json, connector_id="conn-1", notification_url=url,
        header_value="abc", logger=LOGGER, tables=tables or ["account", "annotation"],
    )


class TestRegistrar:
    def test_ensure_is_idempotent(self) -> None:
        api = FakeDataverse(missing_filters={("Assign", "annotation")})
        endpoint_id = asyncio.run(_registrar(api).ensure())
        assert endpoint_id == "ep-1"
        expected_steps = 2 * len(ENTITY_MESSAGES) - 1 + len(GLOBAL_MESSAGES)
        assert len(api.steps) == expected_steps
        assert not any(s["name"].endswith("Assign annotation") for s in api.steps.values())
        first_writes = len(api.writes)

        assert asyncio.run(_registrar(api).ensure()) == "ep-1"
        assert len(api.writes) == first_writes  # nothing re-created

    def test_url_change_repoints_endpoint_and_refreshes_header(self) -> None:
        api = FakeDataverse()
        asyncio.run(_registrar(api).ensure())
        asyncio.run(_registrar(api, url="https://stage.example/api/webhooks/microsoft/dataverse/conn-1").ensure())
        assert api.endpoints["ep-1"]["url"].startswith("https://stage.example/")
        assert json.loads(api.endpoints["ep-1"]["authvalue"]) == {WEBHOOK_HEADER: "abc"}
        assert ("PATCH", "serviceendpoints(ep-1)") in api.writes

    def test_missing_privilege_returns_none_and_logs(self, caplog) -> None:
        api = FakeDataverse(forbid=True)
        with caplog.at_level(logging.WARNING, logger=LOGGER.name):
            assert asyncio.run(_registrar(api).ensure()) is None
        assert any("System Administrator" in rec.getMessage() for rec in caplog.records)

    def test_remove_all_deletes_steps_then_endpoint(self) -> None:
        api = FakeDataverse()
        asyncio.run(_registrar(api).ensure())
        asyncio.run(_registrar(api).remove_all())
        assert api.steps == {} and api.endpoints == {}
        deletes = [p for m, p in api.writes if m == "DELETE"]
        assert deletes[-1] == "serviceendpoints(ep-1)"
        assert all(p.startswith("sdkmessageprocessingsteps(") for p in deletes[:-1])
        asyncio.run(_registrar(api).remove_all())  # nothing left: no error
