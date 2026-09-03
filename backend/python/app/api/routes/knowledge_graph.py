"""Knowledge-graph explorer API (Edrak addition).

Returns the permission-filtered neighbourhood of a record so a UI can draw it: the record's
collection, related records (parent/child, attachments, links…), and — for org admins on request —
the users/groups that hold permissions on it. Every record node is checked against
`get_accessible_virtual_record_ids` for the caller, so a user never sees a record they cannot read.
Provider-agnostic: uses only `IGraphDBProvider` edge/document primitives (Neo4j or Arango).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.api.middlewares.auth import require_scopes
from app.config.constants.arangodb import CollectionNames
from app.config.constants.service import OAuthScopes
from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

router = APIRouter()

MAX_DEPTH = 2
MAX_NODES = 150
DEFAULT_NODES = 60

# Node label field, first match wins.
_LABEL_FIELDS = (
    "recordName",
    "groupName",
    "name",
    "fullName",
    "email",
    "departmentName",
    "title",
)

# Edge collections walked from a record (outbound) and into a record (inbound).
_RECORD_OUT_EDGES = (
    CollectionNames.RECORD_RELATIONS.value,
    CollectionNames.BELONGS_TO.value,
)
_RECORD_IN_EDGES = (CollectionNames.RECORD_RELATIONS.value,)
_PERMISSION_EDGE = CollectionNames.PERMISSION.value


async def get_graph_provider(request: Request) -> IGraphDBProvider:
    container = request.app.container
    return await container.graph_provider()


def _split(node_ref: str) -> tuple[str, str]:
    if "/" in node_ref:
        collection, key = node_ref.split("/", 1)
        return collection, key
    return CollectionNames.RECORDS.value, node_ref


def _label_of(doc: dict[str, Any], key: str) -> str:
    for field in _LABEL_FIELDS:
        value = doc.get(field)
        if isinstance(value, str) and value.strip():
            return value
    return key


def _node_payload(collection: str, key: str, doc: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": f"{collection}/{key}",
        "key": key,
        "collection": collection,
        "label": _label_of(doc, key),
    }
    if collection == CollectionNames.RECORDS.value:
        payload.update(
            {
                "recordType": doc.get("recordType"),
                "origin": doc.get("origin"),
                "connector": doc.get("connectorName"),
                "indexingStatus": doc.get("indexingStatus"),
                "mimeType": doc.get("mimeType"),
                "webUrl": doc.get("webUrl"),
                "recordGroupId": doc.get("recordGroupId"),
                "sizeInBytes": doc.get("sizeInBytes"),
                "updatedAt": doc.get("updatedAtTimestamp") or doc.get("updatedAt"),
            }
        )
    elif collection == CollectionNames.RECORD_GROUPS.value:
        payload.update(
            {
                "groupType": doc.get("groupType"),
                "connector": doc.get("connectorName"),
            }
        )
    return payload


def _edge_type(edge: dict[str, Any], collection: str) -> str:
    for field in ("relationshipType", "relationType", "type", "role"):
        value = edge.get(field)
        if isinstance(value, str) and value:
            return value
    return collection


@router.get(
    "/knowledge-graph/neighborhood",
    dependencies=[Depends(require_scopes(OAuthScopes.KB_READ))],
)
async def knowledge_graph_neighborhood(
    request: Request,
    recordId: str = Query(..., min_length=1, max_length=128),
    depth: int = Query(1, ge=1, le=MAX_DEPTH),
    limit: int = Query(DEFAULT_NODES, ge=1, le=MAX_NODES),
    includePermissions: bool = Query(False),
    graph_provider: IGraphDBProvider = Depends(get_graph_provider),
) -> JSONResponse:
    logger = request.app.container.logger()
    user = request.state.user or {}
    user_id = user.get("userId")
    org_id = user.get("orgId")
    if not user_id or not org_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    accessible_map = await graph_provider.get_accessible_virtual_record_ids(user_id, org_id)
    accessible: set[str] = set(accessible_map.values()) if accessible_map else set()
    if recordId not in accessible:
        # Same answer for "does not exist" and "not yours": no existence oracle.
        raise HTTPException(status_code=404, detail="Record not found")

    # Permission edges expose people; admins only, and only when asked.
    is_admin = str(user.get("role", "")).lower() == "admin"
    include_permissions = includePermissions and is_admin

    records_col = CollectionNames.RECORDS.value
    center = f"{records_col}/{recordId}"

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    truncated = False

    async def add_node(node_ref: str) -> bool:
        """Fetch + add a node; returns False when it must be skipped (inaccessible/missing)."""
        if node_ref in nodes:
            return True
        if len(nodes) >= limit:
            return False
        collection, key = _split(node_ref)
        if collection == records_col and key not in accessible:
            return False
        doc = await graph_provider.get_document(key, collection)
        if not doc:
            return False
        nodes[node_ref] = _node_payload(collection, key, doc)
        return True

    def add_edge(source: str, target: str, edge_type: str) -> None:
        edges[(source, target, edge_type)] = {
            "source": source,
            "target": target,
            "type": edge_type,
        }

    if not await add_node(center):
        raise HTTPException(status_code=404, detail="Record not found")

    frontier = [center]
    for _level in range(depth):
        next_frontier: list[str] = []
        for node_ref in frontier:
            collection, _key = _split(node_ref)
            if collection != records_col:
                continue  # only records are expanded; groups/people are leaves
            for edge_col in _RECORD_OUT_EDGES:
                for edge in await graph_provider.get_edges_from_node(node_ref, edge_col):
                    target = edge.get("_to")
                    if not isinstance(target, str):
                        continue
                    if await add_node(target):
                        add_edge(node_ref, target, _edge_type(edge, edge_col))
                        next_frontier.append(target)
                    else:
                        truncated = truncated or len(nodes) >= limit
            for edge_col in _RECORD_IN_EDGES:
                for edge in await graph_provider.get_edges_to_node(node_ref, edge_col):
                    source = edge.get("_from")
                    if not isinstance(source, str):
                        continue
                    if await add_node(source):
                        add_edge(source, node_ref, _edge_type(edge, edge_col))
                        next_frontier.append(source)
                    else:
                        truncated = truncated or len(nodes) >= limit
            if include_permissions:
                for edge in await graph_provider.get_edges_to_node(node_ref, _PERMISSION_EDGE):
                    source = edge.get("_from")
                    if not isinstance(source, str):
                        continue
                    if await add_node(source):
                        add_edge(source, node_ref, f"permission:{_edge_type(edge, 'permission')}")
        frontier = [n for n in next_frontier if n != center]
        if not frontier:
            break

    logger.debug(
        "knowledge-graph neighborhood",
        extra={"recordId": recordId, "nodes": len(nodes), "edges": len(edges), "depth": depth},
    )
    return JSONResponse(
        {
            "center": center,
            "depth": depth,
            "truncated": truncated,
            "nodes": list(nodes.values()),
            "edges": list(edges.values()),
        }
    )
