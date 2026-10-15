from __future__ import annotations

import asyncio
from typing import Any, cast

import httpx
import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from fastapi.testclient import TestClient
from test_cao_session_wake import (
    _attach,
    _boundary_delivery,
    _CAOBrokerAdapter,
    _Registry,
    _terminal_cao_proof,
    _worker_boundary,
)
from test_dashboard_edge import _bootstrap, _settings

from cao_control_plane.dashboard import DashboardReadModel, build_operator_view
from cao_control_plane.dashboard_edge import create_dashboard_edge, render_dashboard_snapshot
from cao_control_plane.database import Database, utc_after
from cao_control_plane.projection import _operator_cao_supervision_projection, build_projection
from cao_control_plane.runtime import CodexAppServerAdapter, Dispatcher
from cao_control_plane.service import ControlPlane

_NEW_CATALOG = "d" * 64


def _upgrade(service: ControlPlane) -> None:
    service.reconcile_conversation_tool_catalog(_NEW_CATALOG, "e" * 64)


def _reconnect(service: ControlPlane, attachment: dict[str, Any]) -> dict[str, Any]:
    return attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=attachment["native_thread_id"],
            project_digest=attachment["project_scope_digest"],
            proxy_catalog_digest=_NEW_CATALOG,
        ),
    )


@pytest.mark.parametrize("delivery_state", ["queued", "delivered", "acknowledged"])
def test_upgrade_parks_wakes_without_changing_work_or_delivery(
    system: dict[str, Any], delivery_state: str
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = ?, updated_at = ? WHERE message_id = ?",
        (delivery_state, utc_after(-120), delivery["message_id"]),
    )
    before = service.get_work(work["id"])
    _upgrade(service)
    messages_before = service.db.fetchall("SELECT * FROM messages")
    deliveries_before = service.db.fetchall("SELECT * FROM message_deliveries")
    arguments = {
        "message_id": delivery["message_id"],
        "attachment_id": attachment["id"],
        "delivery_generation": delivery["generation"],
        **_terminal_cao_proof(attachment, delivery),
    }
    adapter = _CAOBrokerAdapter(service)
    dispatcher = Dispatcher(service, system["settings"], registry=cast(Any, _Registry(adapter)))
    for _ in range(4):
        assert dispatcher._claim_delivery() is None
        assert service.pending_cao_supervision_activations(updated_before=utc_after(1)) == []
        assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "not_required"
        assert service.recover_expired_reasoner_turns() == 0
        assert service.reconcile_cao_supervision_obligations() == 0
    assert service.db.fetchall("SELECT * FROM messages") == messages_before
    assert service.db.fetchall("SELECT * FROM message_deliveries") == deliveries_before
    after = service.get_work(work["id"])
    for key in ("state", "generation", "goal_version", "current_attempt", "open_boundaries"):
        assert after[key] == before[key]
    with service.db.connect() as connection:
        projection = _operator_cao_supervision_projection(
            connection, work_item_id=work["id"], as_of=utc_after(1)
        )
    assert projection["cao_supervision_state"] == "connection_required"

    # Reopening the same persisted database cannot reset the capability gate.
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    assert restarted.pending_cao_supervision_activations(updated_before=utc_after(1)) == []
    fresh = _reconnect(restarted, attachment)
    assert fresh["id"] == attachment["id"]
    assert fresh["generation"] == attachment["generation"]
    assert fresh["runtime_session_id"] == attachment["runtime_session_id"]
    if delivery_state == "queued":
        claimed = dispatcher._claim_delivery()
        assert claimed is not None and claimed["message_id"] == delivery["message_id"]
        assert dispatcher._claim_delivery() is None
    else:
        candidates = restarted.pending_cao_supervision_activations(updated_before=utc_after(1))
        assert [row["message_id"] for row in candidates] == [delivery["message_id"]]
        assert restarted.reconcile_incomplete_cao_provider_turn(**arguments) == "observed"
        assert restarted.reconcile_incomplete_cao_provider_turn(**arguments) == "scheduled"
        assert restarted.reconcile_incomplete_cao_provider_turn(**arguments) == "not_required"


def test_catalog_gate_covers_inflight_and_expired_reasoner_recovery(system: dict[str, Any]) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    actor = service.authenticate(attachment["context_token"])
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=service.get_work(work["id"])["open_boundaries"][0]["id"],
        expected_generation=work["generation"],
        idempotency_key="decision-before-upgrade",
    )
    service.db.execute("UPDATE reasoner_turns SET lease_expires_at = ?", (utc_after(-120),))
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    _upgrade(service)
    for _ in range(3):
        with service.db.transaction() as connection:
            assert (
                service._recover_incomplete_reasoner_delivery_tx(
                    connection,
                    message_id=delivery["message_id"],
                    recipient_id=system["cao"]["id"],
                    delivery_generation=delivery["generation"],
                    boundary_id=turn["boundary_id"],
                )
                == "not_required"
            )
        assert service.recover_expired_reasoner_turns() == 0
    _reconnect(service, attachment)
    assert service.recover_expired_reasoner_turns() == 1
    assert service.recover_expired_reasoner_turns() == 0


def test_unrelated_attachment_still_dispatches_while_catalog_recovery_waits(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    old = _attach(system, thread_id="existing-supervisor")
    old_work, _ = _worker_boundary(system, old)
    _upgrade(service)
    other = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="independent-supervisor",
            project_digest=old["project_scope_digest"],
            proxy_catalog_digest=_NEW_CATALOG,
        ),
    )
    other_work, _ = _worker_boundary(system, other)
    adapter = _CAOBrokerAdapter(service)
    dispatcher = Dispatcher(service, system["settings"], registry=cast(Any, _Registry(adapter)))
    assert asyncio.run(dispatcher.run_once()) == 1
    assert [call["message"]["work_item_id"] for call in adapter.calls] == [other_work["id"]]
    assert _boundary_delivery(system, old_work["id"])["state"] == "queued"


def test_catalog_change_after_claim_parks_only_the_owned_unsubmitted_lease(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    adapter = _CAOBrokerAdapter(service)
    dispatcher = Dispatcher(service, system["settings"], registry=cast(Any, _Registry(adapter)))
    claimed = dispatcher._claim_delivery()
    assert claimed is not None
    _upgrade(service)
    assert asyncio.run(dispatcher._process_delivery(claimed)) is None
    assert not adapter.calls
    parked = _boundary_delivery(system, work["id"])
    assert parked["message_id"] == claimed["message_id"]
    assert parked["state"] == "queued"
    assert parked["generation"] == claimed["generation"] + 1
    assert parked["attempts"] == claimed["attempts"]
    assert parked["owner_token"] == ""
    assert parked["lease_until"] is None
    _reconnect(service, attachment)
    fresh_claim = dispatcher._claim_delivery()
    assert fresh_claim is not None
    # A stale task cannot reclaim, release or execute the newer owned lease.
    _upgrade(service)
    with service.db.transaction() as connection:
        connection.execute("UPDATE cao_attachment_connections SET state = 'stale'")
    assert asyncio.run(dispatcher._process_delivery(claimed)) is None
    assert _boundary_delivery(system, work["id"])["state"] == "leased"
    assert not adapter.calls


def test_rearm_scan_skips_unavailable_catalog_before_its_page_limit(system: dict[str, Any]) -> None:
    service = system["service"]
    old = _attach(system, thread_id="old-catalog")
    old_work, _ = _worker_boundary(system, old)
    _upgrade(service)
    other = _reconnect(service, {**old, "native_thread_id": "current-catalog"})
    other_work, _ = _worker_boundary(system, other)
    service.db.execute("UPDATE message_deliveries SET state = 'handled'")
    assert service.reconcile_cao_supervision_obligations(limit=1) == 1
    assert _boundary_delivery(system, old_work["id"])["state"] == "handled"
    latest = service.db.fetchone("SELECT work_item_id FROM messages ORDER BY sequence DESC LIMIT 1")
    assert latest["work_item_id"] == other_work["id"]


@pytest.mark.parametrize("connection_state", ["active", "expired"])
def test_current_catalog_survives_credential_lease_expiry(
    system: dict[str, Any], connection_state: str
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    _upgrade(service)
    _reconnect(service, attachment)
    service.db.execute(
        "UPDATE cao_attachment_connections SET state = ?, lease_expires_at = ? "
        "WHERE proxy_catalog_digest = ?",
        (connection_state, utc_after(-120), _NEW_CATALOG),
    )
    # A current process can refresh its credentials on cold wake. Its expired
    # credential does not erase the admitted process-loaded catalog contract.
    delivery = Dispatcher(service, system["settings"])._claim_delivery()
    assert (
        delivery is not None
        and delivery["message_id"] == _boundary_delivery(system, work["id"])["message_id"]
    )


@pytest.mark.parametrize("invalidated", ["revoked", "failed", "stale", "generation", "abi"])
def test_invalid_connection_evidence_cannot_release_the_wake(
    system: dict[str, Any], invalidated: str
) -> None:
    service = system["service"]
    attachment = _attach(system)
    _worker_boundary(system, attachment)
    if invalidated == "generation":
        service.db.execute("UPDATE cao_attachment_connections SET generation = generation + 1")
    elif invalidated == "abi":
        service.db.execute(
            "UPDATE metadata SET value = CAST(value AS INTEGER) + 1 "
            "WHERE key = 'conversation_mcp_proxy_abi_version'"
        )
    else:
        service.db.execute("UPDATE cao_attachment_connections SET state = ?", (invalidated,))
    assert Dispatcher(service, system["settings"])._claim_delivery() is None


def test_operator_view_reports_connection_wait_without_requester_decision(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    service.db.execute(
        "UPDATE principals SET operator_scope = 'production', operator_label = 'Worker' WHERE id = ?",
        (system["worker"]["id"],),
    )
    attachment = _attach(system)
    _worker_boundary(system, attachment)
    _upgrade(service)
    view = build_operator_view(build_projection(service.db).snapshot)
    assert not view["user_confirmation"]
    assert not view["cao_processing"]
    worker = view["stopped_or_failed"][0]
    assert worker["attention_reason"] == "cao-connection-required"
    assert worker["current_work_items"][0]["cao_supervision_state"] == "connection_required"
    model = DashboardReadModel(service)
    edge = create_dashboard_edge(
        _settings(
            [],
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=model.snapshot())),
        )
    )
    with TestClient(edge, base_url="http://localhost") as browser:
        _bootstrap(browser)
        public = browser.get("/dashboard/api/snapshot").json()["operator"]
        assert not public["user_confirmation"]
        assert not public["cao_processing"]
        public_worker = public["stopped_or_failed"][0]
        assert public_worker["attention_reason"] == "cao-connection-required"
        assert public_worker["current_work_items"][0]["cao_supervision_state"] == "connection_required"
        rendered = render_dashboard_snapshot(model.snapshot())
        assert "attention reason cao-connection-required" in rendered
        assert "CAO supervision connection_required" in rendered
        _reconnect(service, attachment)
        restored_public = browser.get("/dashboard/api/snapshot").json()["operator"]
        assert not restored_public["stopped_or_failed"]
        restored_worker = restored_public["cao_processing"][0]
        assert restored_worker["attention_reason"] == "cao-processing"
        assert restored_worker["current_work_items"][0]["cao_supervision_state"] == "scheduled"
    restored = build_operator_view(build_projection(service.db).snapshot)
    assert (
        restored["cao_processing"][0]["current_work_items"][0]["cao_supervision_state"]
        == "scheduled"
    )


def test_catalog_change_after_activation_scan_does_not_resume_the_stale_thread(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)
    delivery = _boundary_delivery(system, work["id"])
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (delivery["message_id"],),
    )
    candidates = service.pending_cao_supervision_activations(updated_before=utc_after(1))
    assert candidates
    _upgrade(service)
    # The normal registry would access the provider if the stale candidate were
    # used. The capability is rechecked before that external operation.
    assert (
        asyncio.run(
            Dispatcher(service, system["settings"])._activate_cao_supervision_threads(candidates)
        )
        == 0
    )


def test_catalog_change_during_launch_preparation_is_fenced_before_submission(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = _attach(system)
    work, _ = _worker_boundary(system, attachment)

    class UpgradeDuringPreparation:
        def prepare_launch(self, name: str, runtime: Any) -> Any:
            _upgrade(service)
            return runtime

        def get(self, name: str) -> CodexAppServerAdapter:
            return CodexAppServerAdapter(system["settings"])

    dispatcher = Dispatcher(
        service, system["settings"], registry=cast(Any, UpgradeDuringPreparation())
    )
    runtime_state = service.get_runtime(attachment["runtime_session_id"])["state"]
    claimed = dispatcher._claim_delivery()
    assert claimed is not None
    asyncio.run(dispatcher._process_delivery(claimed))
    parked = _boundary_delivery(system, work["id"])
    assert parked["message_id"] == claimed["message_id"]
    assert parked["state"] == "queued"
    assert parked["generation"] == claimed["generation"] + 1
    assert service.get_runtime(attachment["runtime_session_id"])["state"] == runtime_state
    assert service.db.fetchone("SELECT COUNT(*) FROM cao_runtime_tickets")[0] == 0
