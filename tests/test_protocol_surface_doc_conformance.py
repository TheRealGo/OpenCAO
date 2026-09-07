"""Keep public documentation and the CLI aligned with the exposed CAO surface."""

from pathlib import Path
from typing import Any, cast

import pytest

from cao_control_plane.cli import build_parser
from cao_control_plane.mcp import CAO_TOOLS, MCPServer

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_CONTRACT_DOCS = (
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / "docs" / "cao-operator-runbook.md",
    ROOT / "docs" / "index.md",
    ROOT / "docs" / "protocols.md",
    ROOT / "docs" / "architecture.md",
    ROOT / "docs" / "supervisor-control-plane.md",
    ROOT / "docs" / "dashboard-read-model.md",
)


def _subparser(parser: Any, name: str) -> Any:
    action = next(action for action in parser._actions if getattr(action, "choices", None))
    return action.choices[name]


def _tools_for(actor: dict[str, Any]) -> set[str]:
    # tools_for is registry-only; it does not consult the service instance.
    server = cast(MCPServer, object.__new__(MCPServer))
    return {tool["name"] for tool in server.tools_for(actor)}


def test_cli_review_verdicts_match_the_exposed_cao_review_schema() -> None:
    parser = build_parser()
    review = _subparser(parser, "review")
    verdict = next(action for action in review._actions if action.dest == "verdict")
    mcp_verdicts = CAO_TOOLS["cao_review"]["inputSchema"]["properties"]["verdict"]["enum"]

    assert verdict.choices == mcp_verdicts == ["ok", "needs_work"]
    assert "requester decisions are recorded only from the attached CAO conversation" in " ".join(
        review.format_help().split()
    )

    parser.parse_args(["review", "attempt-1", "--verdict", "ok", "--summary", "reviewed"])
    with pytest.raises(SystemExit):
        parser.parse_args(["review", "attempt-1", "--verdict", "accepted", "--summary", "invalid"])


def test_role_registry_keeps_review_and_requester_decision_with_cao() -> None:
    user_tools = _tools_for({"role": "user"})
    attached_cao_tools = _tools_for({"role": "cao", "_cao_conversation_credential_id": "attached"})

    assert {"cao_review", "cao_record_requester_decision"}.isdisjoint(user_tools)
    assert {"cao_review", "cao_record_requester_decision"}.issubset(attached_cao_tools)


def test_public_docs_describe_the_conversation_scoped_requester_boundary() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in PUBLIC_CONTRACT_DOCS)

    required = (
        "requester speaks only in the existing CAO conversation",
        "CAO reasons first",
        "not a requester MCP or REST",
        "`cao_review` verdicts are exactly `ok` and `needs_work`",
        "The current canonical projection supplies `closure_state` and\n`closure_summary`",
    )
    forbidden = (
        "User principals can submit durable",
        "requester-facing reads and acceptance use the scoped MCP or REST surfaces",
        "The requester can inspect, submit an explicit cancellation",
        "The requester accepts through UserAcceptance",
        "Actor is requester; review is current for the WorkItem",
        "The current projection does not yet supply `closure_state`",
    )

    assert all(phrase in text for phrase in required)
    assert all(phrase not in text for phrase in forbidden)
