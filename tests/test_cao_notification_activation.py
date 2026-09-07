from __future__ import annotations

import asyncio
from typing import Any

import pytest
from test_worker_output_delivery import _capture_fixture, _event

from cao_control_plane.errors import ValidationError
from cao_control_plane.models import MessageKind, ReportInput
from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    DesktopCAOTerminalTurnEvidence,
    Dispatcher,
    _codex_delivery_client_user_message_id,
)

_PAST = "2000-01-01T00:00:00Z"
_FUTURE = "2100-01-01T00:00:00Z"


def _accepted(service: Any, message_id: str, *, state: str = "delivered") -> None:
    service.db.execute(
        "UPDATE message_deliveries SET state = ?, delivered_at = ?, updated_at = ?, "
        "owner_token = '', lease_until = NULL WHERE message_id = ?",
        (state, _PAST, _PAST, message_id),
    )


def _notification(system: dict[str, Any], actor: dict[str, Any], work: dict[str, Any], index: int):
    service = system["service"]
    with service.db.transaction() as connection:
        message = service._message(
            connection,
            sender_id=system["worker"]["id"],
            recipient_id=actor["id"],
            kind=MessageKind.SYSTEM,
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            goal_version=work["goal_version"],
            payload={"summary": "An independently accepted notification is available."},
            idempotency_key=f"activation:notification:{index}",
        )
    _accepted(service, message["id"])
    return message


@pytest.mark.parametrize("kind", ["first_output", "progress"])
@pytest.mark.parametrize("delivery_state", ["delivered", "acknowledged"])
def test_every_accepted_informational_notification_can_activate_without_a_boundary(
    system, kind: str, delivery_state: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    if kind == "first_output":
        output = service.observe_worker_output(**binding, event=_event())
        message_id = output["notification_message_id"]
    else:
        attempt = work["current_attempt"]
        service.report(
            system["worker"],
            attempt["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=work["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=work["generation"],
                summary="A bounded progress observation is available.",
                idempotency_key="activation:progress",
            ),
        )
        message_id = service.db.fetchone(
            "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'progress'",
            (attempt["id"],),
        )["id"]
    _accepted(service, message_id, state=delivery_state)
    before = dict(
        service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (message_id,))
    )
    event_count = service.db.fetchone("SELECT COUNT(*) FROM events")[0]

    candidates = service.pending_cao_supervision_activations(updated_before=_FUTURE)

    assert [candidate["message_id"] for candidate in candidates] == [message_id]
    candidate = candidates[0]
    assert candidate["message_sequence"] > 0
    assert candidate["attachment_id"] == actor["_cao_attachment_id"]
    attachment = service.get_cao_attachment(actor["_cao_attachment_id"])
    assert candidate["runtime_id"] == attachment["runtime"]["id"]
    assert candidate["native_thread_id"] == attachment["native_thread_id"]
    current = service.get_work(work["id"])
    assert current["state"] == "active"
    assert current["attention_owner"] == "worker"
    assert current["open_boundaries"] == []
    assert current["current_attempt"]["completion_claim"] == {}
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ?", (message_id,)
            )
        )
        == before
    )
    assert service.db.fetchone("SELECT COUNT(*) FROM events")[0] == event_count


def test_bounded_activation_pages_reach_later_cold_item_after_consumed_history(
    system, monkeypatch
) -> None:
    service = system["service"]
    actor, work, _ = _capture_fixture(system)
    accepted = [_notification(system, actor, work, index) for index in range(9)]
    exact_later_id = accepted[-1]["id"]
    before = [dict(row) for row in service.db.fetchall("SELECT * FROM message_deliveries")]
    event_count = service.db.fetchone("SELECT COUNT(*) FROM events")[0]
    selector = service.pending_cao_supervision_activations

    def bounded_selector(
        *,
        updated_before: str,
        after_sequence: int = 0,
        through_sequence: int | None = None,
        limit: int = 100,
    ):
        return selector(
            updated_before=updated_before,
            after_sequence=after_sequence,
            through_sequence=through_sequence,
            limit=2,
        )

    monkeypatch.setattr(service, "pending_cao_supervision_activations", bounded_selector)
    dispatcher = Dispatcher(service, system["settings"])
    adapter = dispatcher.registry.get("codex-app-server")
    assert isinstance(adapter, CodexAppServerAdapter)
    observed: list[str] = []
    cold_queue = {exact_later_id}

    async def activate(runtime, *, message_id: str):
        assert runtime["cao_attachment"]["id"] == actor["_cao_attachment_id"]
        observed.append(message_id)
        if message_id in cold_queue:
            cold_queue.remove(message_id)
            return "resumed"
        # Older accepted notifications have left the provider queue, but
        # absence supplies no terminal proof and never authorizes a replay.
        return "pending"

    monkeypatch.setattr(adapter, "activate_desktop_cao_thread", activate)
    visited_sequences: list[int] = []
    activated = 0
    for _ in range(5):
        candidates = dispatcher._cao_supervision_activation_candidates()
        assert 1 <= len(candidates) <= 2
        visited_sequences.extend(candidate["message_sequence"] for candidate in candidates)
        activated += asyncio.run(dispatcher._activate_cao_supervision_threads(candidates))

    assert observed == [message["id"] for message in accepted]
    assert visited_sequences == sorted(set(visited_sequences))
    assert activated == 1
    assert cold_queue == set()
    wrapped = dispatcher._cao_supervision_activation_candidates()
    assert [candidate["message_id"] for candidate in wrapped] == [
        message["id"] for message in accepted[:2]
    ]
    assert [dict(row) for row in service.db.fetchall("SELECT * FROM message_deliveries")] == before
    assert service.db.fetchone("SELECT COUNT(*) FROM events")[0] == event_count


def test_informational_terminal_proof_does_not_create_semantic_recovery(
    system, monkeypatch
) -> None:
    service = system["service"]
    actor, work, _ = _capture_fixture(system)
    message = _notification(system, actor, work, 1)
    dispatcher = Dispatcher(service, system["settings"])
    adapter = dispatcher.registry.get("codex-app-server")
    attachment = service.get_cao_attachment(actor["_cao_attachment_id"])
    before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ?", (message["id"],)
        )
    )
    event_count = service.db.fetchone("SELECT COUNT(*) FROM events")[0]

    async def observed_terminal(runtime, *, message_id: str):
        assert message_id == message["id"]
        return DesktopCAOTerminalTurnEvidence(
            native_thread_id=attachment["native_thread_id"],
            client_user_message_id=_codex_delivery_client_user_message_id({"id": message_id}),
            native_turn_id="activation-fixture-terminal-turn",
            terminal_status="completed",
            started_at=100,
            completed_at=101,
        )

    monkeypatch.setattr(adapter, "activate_desktop_cao_thread", observed_terminal)
    candidates = dispatcher._cao_supervision_activation_candidates()
    assert [candidate["message_id"] for candidate in candidates] == [message["id"]]
    assert asyncio.run(dispatcher._activate_cao_supervision_threads(candidates)) == 0
    assert service.get_work(work["id"])["open_boundaries"] == []
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ?", (message["id"],)
            )
        )
        == before
    )
    assert service.db.fetchone("SELECT COUNT(*) FROM events")[0] == event_count


def test_activation_scan_ceiling_bounds_round_without_implicit_wrap(system) -> None:
    service = system["service"]
    actor, work, _ = _capture_fixture(system)
    accepted = [_notification(system, actor, work, index) for index in range(3)]
    ceiling = service.cao_supervision_activation_scan_high_water()
    assert ceiling == accepted[-1]["sequence"]
    later = _notification(system, actor, work, 3)
    assert later["sequence"] > ceiling > 0
    before = service.db.commit_generation()

    candidates = service.pending_cao_supervision_activations(
        updated_before=_FUTURE, through_sequence=ceiling
    )
    assert [candidate["message_id"] for candidate in candidates] == [
        message["id"] for message in accepted
    ]
    assert (
        service.pending_cao_supervision_activations(
            updated_before=_FUTURE,
            after_sequence=ceiling,
            through_sequence=ceiling,
        )
        == []
    )
    assert (
        service.pending_cao_supervision_activations(
            updated_before=_FUTURE,
            after_sequence=later["sequence"],
            through_sequence=ceiling,
        )
        == []
    )
    assert (
        service.pending_cao_supervision_activations(updated_before=_FUTURE, through_sequence=0)
        == []
    )
    # Callers that do not opt into fixed rounds retain the historical wrap.
    wrapped = service.pending_cao_supervision_activations(
        updated_before=_FUTURE, after_sequence=later["sequence"]
    )
    assert [candidate["message_id"] for candidate in wrapped] == [
        message["id"] for message in [*accepted, later]
    ]
    assert service.cao_supervision_activation_scan_high_water() == later["sequence"]
    assert service.db.commit_generation() == before


def test_fixed_activation_rounds_revisit_pending_messages_during_continuous_arrivals(
    system, monkeypatch
) -> None:
    service = system["service"]
    actor, work, _ = _capture_fixture(system)
    initial = [_notification(system, actor, work, index) for index in range(5)]
    selector = service.pending_cao_supervision_activations
    ceilings: list[int | None] = []

    def bounded_selector(
        *,
        updated_before: str,
        after_sequence: int = 0,
        through_sequence: int | None = None,
        limit: int = 100,
    ):
        ceilings.append(through_sequence)
        return selector(
            updated_before=updated_before,
            after_sequence=after_sequence,
            through_sequence=through_sequence,
            limit=2,
        )

    monkeypatch.setattr(service, "pending_cao_supervision_activations", bounded_selector)
    dispatcher = Dispatcher(service, system["settings"])
    adapter = dispatcher.registry.get("codex-app-server")
    observed: list[str] = []

    async def still_pending(runtime, *, message_id: str):
        assert runtime["cao_attachment"]["id"] == actor["_cao_attachment_id"]
        observed.append(message_id)
        return "pending"

    monkeypatch.setattr(adapter, "activate_desktop_cao_thread", still_pending)
    pages: list[list[str]] = []
    arrivals = []
    for scan in range(12):
        if scan:
            arrivals.append(_notification(system, actor, work, 4 + scan))
        before = [dict(row) for row in service.db.fetchall("SELECT * FROM message_deliveries")]
        event_count = service.db.fetchone("SELECT COUNT(*) FROM events")[0]
        generation = service.db.commit_generation()
        candidates = dispatcher._cao_supervision_activation_candidates()
        assert 1 <= len(candidates) <= 2
        pages.append([candidate["message_id"] for candidate in candidates])
        assert asyncio.run(dispatcher._activate_cao_supervision_threads(candidates)) == 0
        assert service.db.commit_generation() == generation
        assert service.db.fetchone("SELECT COUNT(*) FROM events")[0] == event_count
        assert [dict(row) for row in service.db.fetchall("SELECT * FROM message_deliveries")] == (
            before
        )

    initial_ids = [message["id"] for message in initial]
    assert pages[:3] == [initial_ids[:2], initial_ids[2:4], initial_ids[4:]]
    assert ceilings[:4] == [initial[-1]["sequence"]] * 4
    # The first pending messages are revisited at the next round, even though
    # a fresh accepted notification arrived before every subsequent scan.
    assert pages[3] == initial_ids[:2]
    assert all(observed.count(message_id) >= 2 for message_id in initial_ids)
    assert arrivals[0]["id"] in observed
    assert all(isinstance(ceiling, int) and ceiling > 0 for ceiling in ceilings)


@pytest.mark.parametrize("state", ["queued", "leased", "dispatched", "handled", "dead"])
def test_activation_never_retries_unaccepted_unknown_or_handled_delivery(
    system, state: str
) -> None:
    service = system["service"]
    actor, work, _ = _capture_fixture(system)
    message = _notification(system, actor, work, 1)
    _accepted(service, message["id"], state=state)

    assert service.pending_cao_supervision_activations(updated_before=_FUTURE) == []
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ?", (message["id"],)
        )["state"]
        == state
    )


@pytest.mark.parametrize(
    "fence",
    [
        "revoked_attachment",
        "expired_attachment",
        "stopped_runtime",
        "wrong_thread",
        "wrong_principal_route",
    ],
)
def test_activation_keeps_current_attachment_and_native_runtime_fences(system, fence: str) -> None:
    service = system["service"]
    actor, work, _ = _capture_fixture(system)
    _notification(system, actor, work, 1)
    attachment = service.get_cao_attachment(actor["_cao_attachment_id"])
    if fence == "revoked_attachment":
        service.db.execute(
            "UPDATE cao_session_attachments SET state = 'revoked' WHERE id = ?", (attachment["id"],)
        )
    elif fence == "expired_attachment":
        service.db.execute(
            "UPDATE cao_session_attachments SET lease_expires_at = ? WHERE id = ?",
            (_PAST, attachment["id"]),
        )
    elif fence == "stopped_runtime":
        service.db.execute(
            "UPDATE runtime_sessions SET state = 'stopped' WHERE id = ?",
            (attachment["runtime"]["id"],),
        )
    elif fence == "wrong_thread":
        service.db.execute(
            "UPDATE runtime_sessions SET native_session_id = 'another-native-thread' WHERE id = ?",
            (attachment["runtime"]["id"],),
        )
    else:
        service.db.execute(
            "UPDATE cao_session_attachments SET runtime_session_id = ? WHERE id = ?",
            (system["runtime"]["id"], attachment["id"]),
        )

    assert service.pending_cao_supervision_activations(updated_before=_FUTURE) == []


@pytest.mark.parametrize("cursor", [-1, True, 1.5, "1"])
def test_activation_scan_rejects_invalid_cursor_without_mutation(system, cursor) -> None:
    service = system["service"]
    before = service.db.commit_generation()
    with pytest.raises(ValidationError, match="nonnegative integer"):
        service.pending_cao_supervision_activations(updated_before=_FUTURE, after_sequence=cursor)
    assert service.db.commit_generation() == before


@pytest.mark.parametrize("ceiling", [-1, True, 1.5, "1"])
def test_activation_scan_rejects_invalid_ceiling_without_mutation(system, ceiling) -> None:
    service = system["service"]
    before = service.db.commit_generation()
    with pytest.raises(ValidationError, match="nonnegative integer"):
        service.pending_cao_supervision_activations(
            updated_before=_FUTURE, through_sequence=ceiling
        )
    assert service.db.commit_generation() == before
