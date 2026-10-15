from __future__ import annotations

from test_protocol_conformance import _modern_request
from test_verified_artifact_content_read import _attached_cao, _read_arguments, _reported_artifact
from test_worker_business_purpose import _worker

from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import ArtifactContentReadInput


def test_chunk_limit_error_returns_safe_actionable_constraint(system):
    service, cao, _ids = _worker(system)
    arguments = {"work_item_id": "wrk_fixture", "attempt_id": "att_fixture",
        "artifact_id": "art_fixture", "expected_digest": "0" * 64,
        "expected_media_type": "text/plain", "idempotency_key": "oversized-chunk",
        "max_bytes": 131072}
    result = MCPServer(service).handle_modern(cao, _modern_request("tools/call", {
        "name": "cao_read_artifact", "arguments": arguments,
    }))
    error = result["result"]["structuredContent"]["error"]
    assert error["details"]["violations"] == [{"field": "max_bytes", "rule": "maximum", "limit": 65536}]
    assert "wrk_fixture" not in str(error)


def test_verified_utf8_ndjson_is_readable_without_relabeling_or_recreating_it(system):
    attachment, cao = _attached_cao(system, thread_id="ndjson-read", project_marker="a")
    content = b'{"measurement":2}\n{"measurement":3}\n'
    work, artifact = _reported_artifact(system, attachment, content=content,
        media_type="application/x-ndjson", key="ndjson")
    result = system["service"].read_artifact_content(cao,
        ArtifactContentReadInput.model_validate(_read_arguments(work, artifact)))
    assert result["complete"] is True
    assert result["content"] == content.decode("utf-8")
    assert result["media_type"] == "application/x-ndjson"
