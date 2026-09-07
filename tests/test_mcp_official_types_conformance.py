from __future__ import annotations

import asyncio

from mcp_types import (
    LATEST_PROTOCOL_VERSION,
    CallToolRequest,
    CallToolResult,
    CancelledNotification,
    DiscoverRequest,
    DiscoverResult,
    ListToolsRequest,
    ListToolsResult,
    ProgressNotification,
    SubscriptionsAcknowledgedNotification,
    SubscriptionsListenRequest,
    SubscriptionsListenResult,
    Tool,
)

from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    MCPServer,
    _pending_bridge_response,
)


def _modern_request(
    method: str,
    params: dict[str, object] | None = None,
    *,
    request_id: int,
) -> dict[str, object]:
    values = dict(params or {})
    values["_meta"] = {
        PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
        CLIENT_CAPABILITIES_META_KEY: {},
        CLIENT_INFO_META_KEY: {"name": "official-conformance", "version": "1"},
    }
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": values,
    }


def test_current_protocol_matches_the_pinned_official_wire_types(system):
    assert LATEST_PROTOCOL_VERSION == MCP_LATEST_VERSION
    server = MCPServer(system["service"])

    discover_request = _modern_request("server/discover", request_id=1)
    DiscoverRequest.model_validate(discover_request)
    discover_response = server.handle_modern(system["cao"], discover_request)
    assert discover_response is not None
    DiscoverResult.model_validate(discover_response["result"])

    tools_request = _modern_request("tools/list", request_id=2)
    ListToolsRequest.model_validate(tools_request)
    tools_response = server.handle_modern(system["cao"], tools_request)
    assert tools_response is not None
    ListToolsResult.model_validate(tools_response["result"])
    for tool in tools_response["result"]["tools"]:
        Tool.model_validate(tool)

    call_request = _modern_request(
        "tools/call",
        {"name": "cao_get_work", "arguments": {"work_item_id": "wrk_missing"}},
        request_id=3,
    )
    CallToolRequest.model_validate(call_request)
    call_response = server.handle_modern(system["cao"], call_request)
    assert call_response is not None
    CallToolResult.model_validate(call_response["result"])
    assert call_response["result"]["isError"] is True


def test_pending_stdio_results_match_the_pinned_official_wire_types():
    discover_request = _modern_request("server/discover", request_id=4)
    tools_request = _modern_request("tools/list", request_id=5)

    discover_response = _pending_bridge_response(discover_request)
    tools_response = _pending_bridge_response(tools_request)

    assert discover_response is not None
    assert tools_response is not None
    DiscoverResult.model_validate(discover_response["result"])
    ListToolsResult.model_validate(tools_response["result"])
    for tool in tools_response["result"]["tools"]:
        Tool.model_validate(tool)


def test_subscription_and_progress_frames_match_the_pinned_official_wire_types(system):
    server = MCPServer(system["service"])
    request = _modern_request(
        "subscriptions/listen",
        {"notifications": {"resourceSubscriptions": ["cao://self"]}},
        request_id=6,
    )
    SubscriptionsListenRequest.model_validate(request)

    async def collect() -> list[dict[str, object]]:
        return [item async for item in server.subscription_messages(system["cao"], request)]

    acknowledged, result = asyncio.run(collect())
    SubscriptionsAcknowledgedNotification.model_validate(acknowledged)
    SubscriptionsListenResult.model_validate(result["result"])
    ProgressNotification.model_validate(
        server.progress_notification("official-progress", progress=1, total=1)
    )
    CancelledNotification.model_validate(
        server.subscription_cancelled_notification(6, "Subscription stream failed")
    )
