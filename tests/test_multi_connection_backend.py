from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.database import SCHEMA_VERSION
from cao_control_plane.errors import AuthenticationError, AuthorizationError, ConflictError
from cao_control_plane.models import (
    CloseCAOConversationInput,
    NewWorkerThreadInput,
    WorkAssignment,
)
from cao_control_plane.runtime_enrollment import ProcessIdentity

_CATALOG_A = "a" * 64
_CATALOG_B = "b" * 64
_RELEASE_A = "1" * 64
_RELEASE_B = "2" * 64
_PROJECT = "c" * 64


def _identity(pid: int, *, parent_pid: int, signature: str) -> ProcessIdentity:
    return ProcessIdentity(pid=pid, parent_pid=parent_pid, start_signature=signature)


def _attach_current(
    service: Any,
    *,
    peer: ProcessIdentity,
    catalog_digest: str,
    thread_id: str = "multi-connection-thread",
    lease_seconds: int = 86_400,
) -> dict[str, Any]:
    capability = service.issue_owner_local_attachment_bootstrap(
        peer,
        thread_id,
        _PROJECT,
        catalog_digest,
        CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    return service.attach_cao_session(
        service.authenticate(capability),
        current_cao_session_attachment(
            native_thread_id=thread_id,
            project_digest=_PROJECT,
            proxy_catalog_digest=catalog_digest,
            proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
            lease_seconds=lease_seconds,
        ),
    )


def _install_process_identities(
    monkeypatch: pytest.MonkeyPatch, *identities: ProcessIdentity
) -> None:
    by_pid = {identity.pid: identity for identity in identities}
    monkeypatch.setattr("cao_control_plane.service._process_identity", lambda pid: by_pid[pid])


def test_two_authentic_bridges_are_independent_connections(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    root_one = _identity(4101, parent_pid=1, signature="root-one")
    bridge_one = _identity(4201, parent_pid=root_one.pid, signature="bridge-one")
    root_two = _identity(4102, parent_pid=1, signature="root-two")
    bridge_two = _identity(4202, parent_pid=root_two.pid, signature="bridge-two")
    _install_process_identities(monkeypatch, root_one, bridge_one, root_two, bridge_two)

    first = _attach_current(service, peer=bridge_one, catalog_digest=_CATALOG_A)
    runtime_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
        )
    )
    wake_credentials_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_credentials WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ]
    wake_tickets_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_tickets WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ]

    second = _attach_current(service, peer=bridge_two, catalog_digest=_CATALOG_A)

    assert second["id"] == first["id"]
    assert second["generation"] == first["generation"]
    assert (first["connection_generation"], second["connection_generation"]) == (1, 2)
    assert first["connection_id"] != second["connection_id"]
    assert (
        service.authenticate(first["context_token"])["_cao_connection_id"] == first["connection_id"]
    )
    assert (
        service.authenticate(second["context_token"])["_cao_connection_id"]
        == second["connection_id"]
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM cao_attachment_connections "
            "WHERE attachment_id = ? AND state = 'active'",
            (first["id"],),
        )["count"]
        == 2
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
            )
        )
        == runtime_before
    )
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_credentials WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ] == wake_credentials_before
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_tickets WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ] == wake_tickets_before

    rotated_second = _attach_current(service, peer=bridge_two, catalog_digest=_CATALOG_A)
    assert rotated_second["connection_id"] == second["connection_id"]
    assert rotated_second["connection_generation"] == second["connection_generation"]
    assert (
        service.authenticate(first["context_token"])["_cao_connection_id"] == first["connection_id"]
    )
    with pytest.raises(AuthenticationError):
        service.authenticate(second["context_token"])
    assert (
        service.authenticate(rotated_second["context_token"])["_cao_connection_id"]
        == second["connection_id"]
    )


def test_concurrent_no_row_bootstraps_create_one_attachment_and_one_connection(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    peers = (
        _identity(5301, parent_pid=1, signature="no-row-peer-one"),
        _identity(5302, parent_pid=1, signature="no-row-peer-two"),
    )
    _install_process_identities(monkeypatch, *peers)
    capabilities = [
        service.issue_owner_local_attachment_bootstrap(
            peer,
            "concurrent-no-row-thread",
            _PROJECT,
            _CATALOG_A,
            CAO_CONVERSATION_PROXY_ABI_VERSION,
        )
        for peer in peers
    ]
    actors = [service.authenticate(capability) for capability in capabilities]
    request = current_cao_session_attachment(
        native_thread_id="concurrent-no-row-thread",
        project_digest=_PROJECT,
        proxy_catalog_digest=_CATALOG_A,
        proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    ready = Barrier(2)

    def attach(actor: dict[str, Any]) -> dict[str, Any] | Exception:
        ready.wait(timeout=5)
        try:
            return service.attach_cao_session(actor, request)
        except (AuthorizationError, ConflictError) as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(attach, actors))

    attached = [outcome for outcome in outcomes if isinstance(outcome, dict)]
    rejected = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
    assert len(attached) == len(rejected) == 1
    assert isinstance(rejected[0], (AuthorizationError, ConflictError))
    assert "peer_pid" not in attached[0]
    assert "peer_start_signature" not in attached[0]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM cao_session_attachments WHERE native_thread_id = ?",
            (request.native_thread_id,),
        )["count"]
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM cao_attachment_connections WHERE state = 'active'"
        )["count"]
        == 1
    )
    assert {
        str(row["state"])
        for row in service.db.fetchall(
            "SELECT state FROM cao_attachment_bootstrap_credentials WHERE native_thread_id = ?",
            (request.native_thread_id,),
        )
    } == {"revoked"}


def test_post_close_bootstrap_reopens_same_row_once_with_fresh_runtime(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    peers = (
        _identity(5401, parent_pid=1, signature="close-peer"),
        _identity(5402, parent_pid=1, signature="reopen-peer-one"),
        _identity(5403, parent_pid=1, signature="reopen-peer-two"),
    )
    _install_process_identities(monkeypatch, *peers)
    first = _attach_current(
        service,
        peer=peers[0],
        catalog_digest=_CATALOG_A,
        thread_id="post-close-reopen-thread",
    )
    actor = service.authenticate(first["context_token"])
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Retained post-close history",
            objective="Remain terminal while the conversation attachment reopens.",
            acceptance=["Reopening does not reactivate this WorkItem."],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key="post-close-retained-work",
        ),
    )
    service.cancel_work(
        actor,
        work["id"],
        "End the fixture before closing the conversation.",
        "post-close-retained-work-cancel",
    )
    pre_close_capability = service.issue_owner_local_attachment_bootstrap(
        peers[1],
        "post-close-reopen-thread",
        _PROJECT,
        _CATALOG_A,
        CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    closed = service.close_cao_conversation(
        actor,
        CloseCAOConversationInput(idempotency_key="post-close-reopen-close"),
    )
    assert closed["status"] == "closed"
    closed_row = service.db.fetchone(
        "SELECT generation, runtime_session_id FROM cao_session_attachments WHERE id = ?",
        (first["id"],),
    )
    assert closed_row is not None
    with pytest.raises(AuthenticationError):
        service.authenticate(pre_close_capability)

    capabilities = [
        service.issue_owner_local_attachment_bootstrap(
            peer,
            "post-close-reopen-thread",
            _PROJECT,
            _CATALOG_A,
            CAO_CONVERSATION_PROXY_ABI_VERSION,
        )
        for peer in peers[1:]
    ]
    cab_actors = [service.authenticate(capability) for capability in capabilities]
    runtime_count_before = int(
        service.db.fetchone("SELECT COUNT(*) AS count FROM runtime_sessions")["count"]
    )
    request = current_cao_session_attachment(
        native_thread_id="post-close-reopen-thread",
        project_digest=_PROJECT,
        proxy_catalog_digest=_CATALOG_A,
        proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    ready = Barrier(2)

    def reopen(cab_actor: dict[str, Any]) -> dict[str, Any] | Exception:
        ready.wait(timeout=5)
        try:
            return service.attach_cao_session(cab_actor, request)
        except (AuthorizationError, ConflictError) as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(reopen, cab_actors))

    reopened = [outcome for outcome in outcomes if isinstance(outcome, dict)]
    rejected = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
    assert len(reopened) == len(rejected) == 1
    current = reopened[0]
    assert current["id"] == first["id"]
    assert current["runtime_session_id"] != first["runtime_session_id"]
    assert int(current["generation"]) == int(closed_row["generation"]) + 1
    assert int(current["connection_generation"]) == int(first["connection_generation"]) + 1
    assert (
        service.db.fetchone("SELECT COUNT(*) AS count FROM runtime_sessions")["count"]
        == runtime_count_before + 1
    )
    assert service.get_work(work["id"])["state"] == "canceled"
    assert (
        service.db.fetchone(
            "SELECT state FROM runtime_sessions WHERE id = ?",
            (first["runtime_session_id"],),
        )["state"]
        == "stopped"
    )
    assert {
        str(row["state"])
        for row in service.db.fetchall(
            "SELECT state FROM cao_attachment_connections WHERE attachment_id = ?",
            (first["id"],),
        )
    } == {"active", "revoked"}
    assert {
        str(row["state"])
        for row in service.db.fetchall(
            "SELECT state FROM cao_attachment_bootstrap_credentials WHERE attachment_id = ?",
            (first["id"],),
        )
    } == {"revoked"}


def test_two_connections_linearize_one_idempotent_mutation(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    root_one = _identity(4701, parent_pid=1, signature="root-one")
    bridge_one = _identity(4801, parent_pid=root_one.pid, signature="bridge-one")
    root_two = _identity(4702, parent_pid=1, signature="root-two")
    bridge_two = _identity(4802, parent_pid=root_two.pid, signature="bridge-two")
    _install_process_identities(monkeypatch, root_one, bridge_one, root_two, bridge_two)
    first = _attach_current(service, peer=bridge_one, catalog_digest=_CATALOG_A)
    second = _attach_current(service, peer=bridge_two, catalog_digest=_CATALOG_A)
    actors = (
        service.authenticate(first["context_token"]),
        service.authenticate(second["context_token"]),
    )
    request = WorkAssignment(
        worker_id=system["worker"]["id"],
        title="one idempotent mutation",
        objective="Create exactly one WorkItem from concurrent authentic connections.",
        acceptance=["Both calls resolve to the same durable WorkItem."],
        runtime_session_id=system["runtime"]["id"],
        idempotency_key="two-connections-one-mutation",
    )
    ready = Barrier(2)

    def submit(actor: dict[str, Any]) -> dict[str, Any]:
        ready.wait(timeout=5)
        return service.assign_work(actor, request)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, actors))

    assert results[0]["id"] == results[1]["id"]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM work_items WHERE title = ?",
            (request.title,),
        )["count"]
        == 1
    )


def test_failed_new_connection_validation_is_non_mutating(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    identities = (
        _identity(4901, parent_pid=1, signature="root-one"),
        _identity(5001, parent_pid=4901, signature="bridge-one"),
        _identity(4902, parent_pid=1, signature="root-two"),
        _identity(5002, parent_pid=4902, signature="bridge-two"),
        _identity(4903, parent_pid=1, signature="root-three"),
        _identity(5003, parent_pid=4903, signature="bridge-three"),
    )
    _install_process_identities(monkeypatch, *identities)
    first = _attach_current(service, peer=identities[1], catalog_digest=_CATALOG_A)
    second = _attach_current(service, peer=identities[3], catalog_digest=_CATALOG_A)
    second_actor = service.authenticate(second["context_token"])
    work = service.assign_work(
        second_actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="preserved across failed attach",
            objective="Remain byte-for-byte unchanged after a rejected connection.",
            acceptance=["The rejected connection cannot mutate durable authority."],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key="failed-attach-preserves-work",
        ),
    )
    invalid_capability = service.issue_owner_local_attachment_bootstrap(
        identities[5],
        "multi-connection-thread",
        _PROJECT,
        _CATALOG_A,
        CAO_CONVERSATION_PROXY_ABI_VERSION,
    )

    attachment_before = dict(
        service.db.fetchone("SELECT * FROM cao_session_attachments WHERE id = ?", (first["id"],))
    )
    runtime_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
        )
    )
    work_before = dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
    connections_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_attachment_connections WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ]
    credentials_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_conversation_credentials WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ]
    wake_credentials_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_credentials WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ]
    wake_tickets_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_tickets WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ]

    with pytest.raises(AuthorizationError):
        service.attach_cao_session(
            service.authenticate(invalid_capability),
            current_cao_session_attachment(
                native_thread_id="multi-connection-thread",
                project_digest=_PROJECT,
                proxy_catalog_digest=_CATALOG_B,
                proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
            ),
        )

    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?", (first["id"],)
            )
        )
        == attachment_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
            )
        )
        == runtime_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
        == work_before
    )
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_attachment_connections WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ] == connections_before
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_conversation_credentials WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ] == credentials_before
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_credentials WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ] == wake_credentials_before
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_tickets WHERE attachment_id = ? ORDER BY id",
            (first["id"],),
        )
    ] == wake_tickets_before
    assert (
        service.authenticate(first["context_token"])["_cao_connection_id"] == first["connection_id"]
    )
    assert (
        service.authenticate(second["context_token"])["_cao_connection_id"]
        == second["connection_id"]
    )


def test_connection_expiry_and_catalog_rollover_do_not_mutate_attachment_or_wake(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    identities = (
        _identity(4301, parent_pid=1, signature="root-one"),
        _identity(4401, parent_pid=4301, signature="bridge-one"),
        _identity(4302, parent_pid=1, signature="root-two"),
        _identity(4402, parent_pid=4302, signature="bridge-two"),
        _identity(4303, parent_pid=1, signature="root-three"),
        _identity(4403, parent_pid=4303, signature="bridge-three"),
    )
    _install_process_identities(monkeypatch, *identities)
    first = _attach_current(service, peer=identities[1], catalog_digest=_CATALOG_A)
    second = _attach_current(service, peer=identities[3], catalog_digest=_CATALOG_A)
    attachment_before = dict(
        service.db.fetchone("SELECT * FROM cao_session_attachments WHERE id = ?", (first["id"],))
    )
    runtime_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
        )
    )

    service.db.execute(
        "UPDATE cao_attachment_connections SET lease_expires_at = ? WHERE id = ?",
        ("2000-01-01T00:00:00Z", first["connection_id"]),
    )
    service.expire_runtime_leases()
    with pytest.raises(AuthenticationError):
        service.authenticate(first["context_token"])
    assert (
        service.authenticate(second["context_token"])["_cao_connection_id"]
        == second["connection_id"]
    )
    assert (
        service.db.fetchone(
            "SELECT state FROM cao_attachment_connections WHERE id = ?",
            (first["connection_id"],),
        )["state"]
        == "expired"
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?", (first["id"],)
            )
        )
        == attachment_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
            )
        )
        == runtime_before
    )

    reconciled = service.reconcile_conversation_tool_catalog(_CATALOG_B, _RELEASE_B)
    assert reconciled["revoked_credential_count"] == 1
    with pytest.raises(AuthenticationError):
        service.authenticate(second["context_token"])
    assert (
        service.db.fetchone(
            "SELECT state FROM cao_attachment_connections WHERE id = ?",
            (second["connection_id"],),
        )["state"]
        == "stale"
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?", (first["id"],)
            )
        )
        == attachment_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (first["runtime_session_id"],)
            )
        )
        == runtime_before
    )

    current = _attach_current(service, peer=identities[5], catalog_digest=_CATALOG_B)
    assert current["id"] == first["id"]
    assert current["generation"] == first["generation"]
    assert current["connection_generation"] == 3
    assert (
        service.authenticate(current["context_token"])["_cao_connection_id"]
        == current["connection_id"]
    )


def test_short_connection_cannot_shorten_attachment_or_disable_long_connection(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    root_one = _identity(5101, parent_pid=1, signature="root-one")
    bridge_one = _identity(5201, parent_pid=root_one.pid, signature="bridge-one")
    root_two = _identity(5102, parent_pid=1, signature="root-two")
    bridge_two = _identity(5202, parent_pid=root_two.pid, signature="bridge-two")
    _install_process_identities(monkeypatch, root_one, bridge_one, root_two, bridge_two)
    long_connection = _attach_current(
        service,
        peer=bridge_one,
        catalog_digest=_CATALOG_A,
        lease_seconds=3_600,
    )
    long_actor = service.authenticate(long_connection["context_token"])
    preserved_work = service.assign_work(
        long_actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="preserved long-connection work",
            objective="Remain unchanged when another connection expires.",
            acceptance=["The short connection has no authority over this WorkItem."],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key="long-connection-preserved-work",
        ),
    )
    attachment_before = dict(
        service.db.fetchone(
            "SELECT * FROM cao_session_attachments WHERE id = ?",
            (long_connection["id"],),
        )
    )
    runtime_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?",
            (long_connection["runtime_session_id"],),
        )
    )
    work_before = dict(
        service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (preserved_work["id"],))
    )
    worker_before = dict(
        service.db.fetchone("SELECT * FROM principals WHERE id = ?", (system["worker"]["id"],))
    )
    wake_credentials_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_credentials WHERE attachment_id = ? ORDER BY id",
            (long_connection["id"],),
        )
    ]
    wake_tickets_before = [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_tickets WHERE attachment_id = ? ORDER BY id",
            (long_connection["id"],),
        )
    ]

    short_connection = _attach_current(
        service,
        peer=bridge_two,
        catalog_digest=_CATALOG_A,
        lease_seconds=15,
    )
    assert (
        service.db.fetchone(
            "SELECT lease_expires_at FROM cao_session_attachments WHERE id = ?",
            (long_connection["id"],),
        )["lease_expires_at"]
        == attachment_before["lease_expires_at"]
    )
    attachment_after_short_attach = dict(
        service.db.fetchone(
            "SELECT * FROM cao_session_attachments WHERE id = ?",
            (long_connection["id"],),
        )
    )
    service.db.execute(
        "UPDATE cao_attachment_connections SET lease_expires_at = ? WHERE id = ?",
        ("2000-01-01T00:00:00Z", short_connection["connection_id"]),
    )
    service.expire_runtime_leases()

    with pytest.raises(AuthenticationError):
        service.authenticate(short_connection["context_token"])
    long_actor = service.authenticate(long_connection["context_token"])
    assert service.list_managed_workers(long_actor) == []
    follow_up = service.assign_work(
        long_actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="long connection remains mutable",
            objective="Prove the surviving connection retains mutation authority.",
            acceptance=["A new WorkItem is created through the long connection."],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key="long-connection-follow-up",
        ),
    )
    assert follow_up["supervisor_attachment_id"] == long_connection["id"]
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?",
                (long_connection["id"],),
            )
        )
        == attachment_after_short_attach
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (preserved_work["id"],)))
        == work_before
    )
    assert (
        dict(
            service.db.fetchone("SELECT * FROM principals WHERE id = ?", (system["worker"]["id"],))
        )
        == worker_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?",
                (long_connection["runtime_session_id"],),
            )
        )
        == runtime_before
    )
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_credentials WHERE attachment_id = ? ORDER BY id",
            (long_connection["id"],),
        )
    ] == wake_credentials_before
    assert [
        tuple(row)
        for row in service.db.fetchall(
            "SELECT * FROM cao_runtime_tickets WHERE attachment_id = ? ORDER BY id",
            (long_connection["id"],),
        )
    ] == wake_tickets_before


def test_missing_wake_runtime_does_not_gate_connection_inventory_or_close(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    root_one = _identity(4501, parent_pid=1, signature="root-one")
    bridge_one = _identity(4601, parent_pid=root_one.pid, signature="bridge-one")
    root_two = _identity(4502, parent_pid=1, signature="root-two")
    bridge_two = _identity(4602, parent_pid=root_two.pid, signature="bridge-two")
    _install_process_identities(monkeypatch, root_one, bridge_one, root_two, bridge_two)
    first = _attach_current(service, peer=bridge_one, catalog_digest=_CATALOG_A)
    second = _attach_current(service, peer=bridge_two, catalog_digest=_CATALOG_A)
    actor = service.authenticate(second["context_token"])
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing', lease_expires_at = ? WHERE id = ?",
        ("2000-01-01T00:00:00Z", first["runtime_session_id"]),
    )

    assert service.list_managed_workers(actor) == []
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="wake-independent assignment",
            objective="Prove an authenticated connection can still submit work.",
            acceptance=["The WorkItem is durably bound to this attachment."],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key="wake-independent-assignment",
        ),
    )
    assert work["supervisor_attachment_id"] == first["id"]
    service.cancel_work(
        actor,
        work["id"],
        "End the regression fixture before conversation close.",
        "wake-independent-cancel",
    )
    closed = service.close_cao_conversation(
        actor, CloseCAOConversationInput(idempotency_key="close-all-connections")
    )
    assert closed["status"] == "closed"
    assert {
        str(row["state"])
        for row in service.db.fetchall(
            "SELECT state FROM cao_attachment_connections WHERE attachment_id = ?",
            (first["id"],),
        )
    } == {"revoked"}
    assert {
        str(row["state"])
        for row in service.db.fetchall(
            "SELECT state FROM cao_conversation_credentials WHERE attachment_id = ?",
            (first["id"],),
        )
    } == {"revoked"}
    with pytest.raises(AuthenticationError):
        service.authenticate(first["context_token"])
    with pytest.raises(AuthenticationError):
        service.authenticate(second["context_token"])


def test_v31_active_csc_migrates_to_stale_untrusted_connection(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="legacy-v31-connection",
            project_digest=_PROJECT,
            lease_seconds=3600,
        ),
    )
    attached_actor = service.authenticate(str(attachment["context_token"]))
    directory = tmp_path / "v31-worker"
    directory.mkdir()
    managed = service.new_worker_thread(
        attached_actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            runner="codex",
            model="gpt-5.6-terra",
            reasoning_effort="medium",
            name="Historical migration Worker",
            idempotency_key="legacy-v31-worker",
        ),
    )
    epoch_before = dict(
        service.db.fetchone(
            "SELECT id, thread_id, generation, runtime_session_id, enrollment_id, "
            "created_at, retired_at FROM managed_worker_thread_epochs "
            "WHERE thread_id = ?",
            (managed["worker_thread_id"],),
        )
    )
    attachment_before = dict(
        service.db.fetchone(
            "SELECT * FROM cao_session_attachments WHERE id = ?", (attachment["id"],)
        )
    )
    runtime_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (attachment["runtime_session_id"],)
        )
    )
    with service.db.transaction() as connection:
        connection.execute("DROP TRIGGER IF EXISTS cao_conversation_credentials_connection_insert")
        connection.execute("DROP TRIGGER IF EXISTS cao_conversation_credentials_connection_update")
        connection.execute(
            "UPDATE cao_conversation_credentials SET connection_id = NULL WHERE id = ?",
            (attachment["context_credential_id"],),
        )
        connection.execute(
            "DELETE FROM cao_attachment_connections WHERE id = ?",
            (attachment["connection_id"],),
        )
        connection.execute("DROP INDEX managed_worker_thread_epochs_one_current")
        connection.execute(
            "ALTER TABLE managed_worker_thread_epochs RENAME TO managed_worker_thread_epochs_v33"
        )
        connection.execute(
            """
            CREATE TABLE managed_worker_thread_epochs (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL
                    REFERENCES managed_worker_threads(id) ON DELETE CASCADE,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                runtime_session_id TEXT NOT NULL UNIQUE
                    REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
                enrollment_id TEXT NOT NULL UNIQUE
                    REFERENCES worker_enrollments(id) ON DELETE RESTRICT,
                created_at TEXT NOT NULL,
                retired_at TEXT,
                UNIQUE(thread_id, generation)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO managed_worker_thread_epochs(
                id, thread_id, generation, runtime_session_id,
                enrollment_id, created_at, retired_at
            )
            SELECT id, thread_id, generation, runtime_session_id,
                   enrollment_id, created_at, retired_at
            FROM managed_worker_thread_epochs_v33
            """
        )
        connection.execute("DROP TABLE managed_worker_thread_epochs_v33")
        connection.execute(
            "CREATE UNIQUE INDEX managed_worker_thread_epochs_one_current "
            "ON managed_worker_thread_epochs(thread_id) WHERE retired_at IS NULL"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 32")
        connection.execute("DELETE FROM schema_migrations WHERE version = 33")
        connection.execute("UPDATE metadata SET value = '31' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 31")

    service.db.initialize()

    credential = service.db.fetchone(
        "SELECT state, connection_id FROM cao_conversation_credentials WHERE id = ?",
        (attachment["context_credential_id"],),
    )
    assert credential is not None
    assert credential["state"] == "revoked"
    assert credential["connection_id"]
    migrated_connection = service.db.fetchone(
        "SELECT state, proxy_catalog_digest, proxy_abi_version, connection_generation "
        "FROM cao_attachment_connections WHERE id = ?",
        (credential["connection_id"],),
    )
    assert migrated_connection is not None
    assert dict(migrated_connection) == {
        "state": "stale",
        "proxy_catalog_digest": "",
        "proxy_abi_version": 0,
        "connection_generation": 1,
    }
    with pytest.raises(AuthenticationError):
        service.authenticate(attachment["context_token"])
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?", (attachment["id"],)
            )
        )
        == attachment_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (attachment["runtime_session_id"],)
            )
        )
        == runtime_before
    )
    assert service.db.fetchone("SELECT value FROM metadata WHERE key = 'schema_version'")[
        "value"
    ] == str(SCHEMA_VERSION)
    assert service.db.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    migrated_epoch = dict(
        service.db.fetchone(
            "SELECT id, thread_id, generation, connection_generation, "
            "runtime_session_id, enrollment_id, created_at, retired_at "
            "FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (managed["worker_thread_id"],),
        )
    )
    assert migrated_epoch.pop("connection_generation") == 1
    assert migrated_epoch == epoch_before
