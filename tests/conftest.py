from __future__ import annotations

import asyncio
import itertools
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from cao_control_plane.config import Settings
from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.database import Database
from cao_control_plane.mcp import conversation_proxy_tools
from cao_control_plane.models import (
    CAOSessionAttachment,
    PrincipalCreate,
    RuntimeHeartbeat,
    RuntimeRegistration,
)
from cao_control_plane.release_identity import catalog_digest
from cao_control_plane.runtime_enrollment import ProcessIdentity
from cao_control_plane.service import (
    WORKER_MCP_REQUIRED_TOOLS,
    ControlPlane,
)

_ATTACHMENT_PEER_GENERATIONS = itertools.count(1)
CURRENT_CAO_CATALOG_DIGEST = catalog_digest(conversation_proxy_tools())
CURRENT_CAO_PROXY_ABI_VERSION = CAO_CONVERSATION_PROXY_ABI_VERSION


@pytest.fixture(autouse=True)
def isolated_instruction_judge(monkeypatch):
    """Unrelated contract tests never contact a model or the owner's desktop.

    Semantic gate tests replace these dependencies with explicit outcomes;
    real-model evaluation is an owner-run, separately recorded validation.
    """
    from cao_control_plane.instruction_review import (
        CHECKS,
        CodexInstructionReviewer,
        ReviewDecision,
        ReviewOutcome,
    )

    def source(_settings, _thread, *, checkpoint=None, **_origin):
        return [{"role": "requester", "source_digest": "fixture-input",
                 "text": "Perform the explicitly assigned bounded responsibility and preserve its fixed criteria."}]

    def judge(_self, _snapshot):
        return ReviewOutcome(ReviewDecision.model_validate({
            "verdict": "pass", "summary": "Independent unit-test verdict.",
            "assessments": [{"check": check, "verdict": "met", "reason": "Test-controlled evidence.",
                "evidence_refs": ["requester_context", "purpose", "candidate", "history"]} for check in CHECKS],
        }), "test-judge", 1)

    monkeypatch.setattr("cao_control_plane.service.read_requester_context", source)
    monkeypatch.setattr(CodexInstructionReviewer, "review", judge)


@pytest.fixture(autouse=True)
def release_pytest_asyncio_spare_loop(request):
    """Close the plugin's idle replacement loop before plain asyncio.run.

    pytest-asyncio's function fixture installs a fresh spare loop at teardown.
    A later synchronous test using asyncio.run replaces it without closing it,
    deferring ResourceWarnings to an unrelated test's garbage collection.
    This releases that test-owned resource; warnings remain errors.
    """
    yield
    if "event_loop" not in request.fixturenames:
        return
    policy = asyncio.get_event_loop_policy()
    try:
        loop = policy.get_event_loop()
    except RuntimeError:
        return
    if not loop.is_running():
        loop.close()
        policy.set_event_loop(None)


def business_purpose_contract() -> dict[str, Any]:
    return {
        "outcome": "Keep service outcomes reliable as requirements evolve.",
        "scope": "The responsibilities explicitly assigned to this Worker.",
        "criteria": [{"id": "reliability", "condition": "The assigned outcome is supported by verified evidence.",
                      "evidence": "A reproducible result and its source conditions."}],
        "completion": "Complete each bounded checkpoint when its fixed conditions are verified; continuing responsibility remains.",
        "authority": "Operate only within the assigned scope; no unrequested external effects.",
        "unmet_action": "Continue the unmet conditions or escalate a concrete blocker to CAO.",
    }


def seed_pre_checkpoint_review(service, actor, request):
    """Seed a valid pre-upgrade accepted checkpoint for lifecycle compatibility.

    Only fixture construction uses the former pending-handoff transaction. New
    checkpoint acceptance and atomic Next are exercised through the current API.
    """
    from cao_control_plane import purpose_continuation
    def retained_handoff(connection, _actor, work, *, review_id, next_action):
        assert next_action is None
        purpose_continuation.require_tx(service, connection, work, review_id=review_id)
    with patch.object(service, "_accept_checkpoint_next_tx", retained_handoff):
        return service.review(actor, request)


def seed_pre_review_worker_message(service, connection, **values):
    """Seed retained pre-upgrade input, never a new public instruction.

    Lifecycle tests need historical queued/claimed messages that predate the
    semantic fence. The normal send gate is tested independently and remains
    installed immediately after this fixture-only insertion.
    """
    with patch("cao_control_plane.service.review_worker_message_tx", return_value=None):
        return service._message(connection, **values)


def current_cao_session_attachment(**values: Any) -> CAOSessionAttachment:
    """Build a test attachment that presents the proxy contract it loaded."""

    values.setdefault("proxy_catalog_digest", CURRENT_CAO_CATALOG_DIGEST)
    values.setdefault("proxy_abi_version", CURRENT_CAO_PROXY_ABI_VERSION)
    return CAOSessionAttachment(**values)


def attach_cao_session_with_peer(
    service: ControlPlane,
    request: CAOSessionAttachment,
    *,
    peer: ProcessIdentity | None = None,
) -> dict[str, Any]:
    """Attach through an explicit one-use CAB and exact test peer generation."""

    if peer is None:
        generation = next(_ATTACHMENT_PEER_GENERATIONS)
        peer = ProcessIdentity(
            pid=1_500_000_000 + generation,
            parent_pid=1,
            start_signature=f"explicit-test-peer-{generation}",
        )
    current_catalog = service.db.fetchone(
        "SELECT value FROM metadata WHERE key = 'conversation_mcp_catalog_digest'"
    )
    if current_catalog is None:
        if request.proxy_catalog_digest != CURRENT_CAO_CATALOG_DIGEST:
            raise AssertionError("a custom test proxy catalog must be reconciled before attachment")
        service.reconcile_conversation_tool_catalog(
            CURRENT_CAO_CATALOG_DIGEST,
            CURRENT_CAO_CATALOG_DIGEST,
        )
    capability = service.issue_owner_local_attachment_bootstrap(
        peer,
        request.native_thread_id,
        request.project_digest,
        request.proxy_catalog_digest,
        request.proxy_abi_version,
    )
    with patch("cao_control_plane.service._process_identity", return_value=peer):
        return service.attach_cao_session(service.authenticate(capability), request)


def enroll_worker_runtime(
    service: ControlPlane,
    cao: dict[str, Any],
    worker_id: str,
    *,
    adapter: str = "claude",
    metadata: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Establish the real managed-MCP precondition used by domain tests."""

    runtime = service.register_runtime(
        cao,
        worker_id,
        RuntimeRegistration(
            adapter=adapter,
            metadata=metadata or {"command": ["/bin/cat"]},
        ),
    )
    launch = service.issue_runtime_launch_ticket(runtime["id"])
    exchange = service.exchange_runtime_launch_ticket(launch["ticket"])
    token = str(exchange["token"])
    actor = service.authenticate(token)
    service.record_mcp_tool_discovery(
        actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        actor,
        runtime["id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=actor["_enrollment_generation"],
            sequence=1,
        ),
    )
    # Direct fixture enrollment has no running adapter process.  Model that
    # authenticated but quiescent boundary explicitly so a later Dispatcher
    # launch may rotate the credential without violating a live BUSY epoch.
    service.db.execute("UPDATE runtime_sessions SET state = 'ready' WHERE id = ?", (runtime["id"],))
    return service.get_runtime(runtime["id"]), actor, token


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    value = replace(
        Settings(),
        state_dir=tmp_path / "state",
        runtime_launch_dir=tmp_path / "state" / "runtime-launches",
        public_base_url="http://127.0.0.1:8768",
        dispatcher_recovery_scan_seconds=0.01,
        dispatcher_lease_seconds=1.0,
        runtime_timeout_seconds=5.0,
        callback_timeout_seconds=2.0,
        # Most domain-unit fixtures intentionally exercise legacy unbound
        # records.  Production defaults and cutover tests remain strict.
        require_cao_attachment_for_work=False,
    )
    value.ensure_directories()
    return value


@pytest.fixture
def system(settings: Settings) -> dict[str, Any]:
    service = ControlPlane(Database(settings), settings)
    bootstrap = service.bootstrap()
    service.reconcile_conversation_tool_catalog(
        CURRENT_CAO_CATALOG_DIGEST,
        CURRENT_CAO_CATALOG_DIGEST,
    )
    cao = service.authenticate(bootstrap["tokens"]["cao"]["token"])
    user = service.authenticate(bootstrap["tokens"]["user"]["token"])
    created = service.create_principal(
        cao, PrincipalCreate(name="worker-1", role="worker", metadata={"team": "test"})
    )
    worker = service.authenticate(created["token"])
    runtime, worker, worker_token = enroll_worker_runtime(
        service,
        cao,
        worker["id"],
    )
    return {
        "settings": settings,
        "service": service,
        "bootstrap": bootstrap,
        "cao": cao,
        "cao_token": bootstrap["tokens"]["cao"]["token"],
        "user": user,
        "user_token": bootstrap["tokens"]["user"]["token"],
        "worker": worker,
        "worker_token": worker_token,
        "worker_principal_token": created["token"],
        "runtime": runtime,
    }


@pytest.fixture
def legacy_progress_wakes(system, monkeypatch):
    """Retain replay/coalescing coverage for rows created before progress became history-only."""
    original = system["service"]._message

    def old_message(connection, **kwargs):
        if kwargs.get("kind") == "progress":
            kwargs["create_delivery"] = True
        return original(connection, **kwargs)

    monkeypatch.setattr(system["service"], "_message", old_message)


@pytest.fixture(autouse=True)
def isolated_native_change_subscription(monkeypatch):
    """Unit tests do not subscribe the owner's real native application.

    The feed's protocol and transitions have independent fixtures; the release
    acceptance script exercises the real owner-local subscription.
    """
    async def no_native_connection(_self):
        pass
    monkeypatch.setattr('cao_control_plane.native_changes.NativeChangeFeed.start', no_native_connection)
    monkeypatch.setattr('cao_control_plane.native_changes.NativeChangeFeed.stop', no_native_connection)
