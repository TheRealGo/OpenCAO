from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest
from test_cao_session_wake import (
    _accept_websocket_client,
    _read_websocket_client_message,
    _write_websocket_server_message,
)
from test_worker_output_delivery import _capture_fixture, _legacy_output_notification, _terminal

from cao_control_plane.models import AckInput
from cao_control_plane.runtime import (
    CodexRpcError,
    Dispatcher,
    _codex_delivery_client_user_message_id,
    render_message,
)


def _queue_admission(service, actor, message):
    message_id = message["id"]
    attachment_id = actor["_cao_attachment_id"]
    attachment = service.get_cao_attachment(attachment_id)
    runtime_id = attachment["runtime"]["id"]
    service.db.execute(
        "UPDATE message_deliveries SET state='delivered', delivered_at='2026-01-01T00:00:00Z', "
        "owner_token='', lease_until=NULL WHERE message_id=?",
        (message_id,),
    )
    with service.db.transaction() as connection:
        service._event(
            connection,
            "runtime.message_delivered",
            "runtime",
            runtime_id,
            "",
            {
                "message_id": message_id,
                "adapter": "codex-app-server",
                "result": {
                    "success": True,
                    "diagnostics": {
                        "delivery_method": "thread_queue",
                        "delivery_acceptance": "queued",
                    },
                },
            },
            causation_id=message_id,
        )
    row = service.db.fetchone("SELECT * FROM messages WHERE id=?", (message_id,))
    message = dict(row)
    message["payload"] = json.loads(message["payload_json"])
    item = {
        "id": "queued-output",
        "clientUserMessageId": _codex_delivery_client_user_message_id(message),
        "input": [{"type": "text", "text": render_message(message)}],
    }
    return item


def _queued_output(system: dict[str, Any]) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = _legacy_output_notification(service, work, binding)
    message_id = output["notification_message_id"]
    row = service.db.fetchone("SELECT * FROM messages WHERE id=?", (message_id,))
    item = _queue_admission(service, actor, dict(row))
    return (
        actor,
        work["id"],
        actor["_cao_attachment_id"],
        {"item": item, "binding": binding, "message_id": message_id},
    )


class QueueHost:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = list(items)
        self.deleted: list[str] = []
        self.methods: list[str] = []
        self.before_delete: Any = None

    async def request(
        self, method: str, params: dict[str, Any], **kwargs: Any
    ) -> tuple[int, dict[str, Any]]:
        self.methods.append(method)
        if method == "initialize":
            return 1, {}
        if method == "thread/queue/list":
            return 2, {"data": list(self.items), "nextCursor": None}
        assert method == "thread/queue/delete"
        if self.before_delete:
            self.before_delete()
        matched = [x for x in self.items if x["id"] == params["queuedSubmissionId"]]
        self.items = [x for x in self.items if x not in matched]
        if matched:
            self.deleted.append(params["queuedSubmissionId"])
        return 3, {"deleted": bool(matched)}

    async def send(self, value: dict[str, Any]) -> None:
        assert value["method"] == "initialized"

    async def close(self) -> None:
        pass


def _host(monkeypatch: pytest.MonkeyPatch, host: QueueHost) -> None:
    async def connect(*args: Any, **kwargs: Any) -> QueueHost:
        return host

    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)


def _queued_purpose(system, tmp_path, decision):
    from test_purpose_continuation import _checkpoint, _next_request, _pending, _wake

    from cao_control_plane.database import utc_after
    from cao_control_plane.models import PurposeNextActionInput

    service, actor, thread, work, _, _ = _checkpoint(
        system, tmp_path, finite=decision == "achieved"
    )
    continuation = _pending(service, work)
    wake = _wake(service, continuation)
    item = _queue_admission(service, actor, wake)
    if decision == "assigned":
        service.instruct_worker_thread(actor, _next_request(thread, continuation))
    elif decision in {"waiting", "achieved"}:
        purpose = service.get_work(work["id"])["worker_purpose"]
        service.record_purpose_next_action(
            actor,
            PurposeNextActionInput(
                continuation_id=continuation["id"],
                expected_generation=continuation["generation"],
                expected_purpose_version=continuation["purpose_version"],
                kind="achieved" if decision == "achieved" else "wait_until",
                reason="The exact next purpose decision is recorded.",
                evidence="The fixed conditions and next dependency were verified.",
                resume_at=utc_after(3600) if decision == "waiting" else None,
                resume_condition="Observe the recorded dependency." if decision == "waiting" else "",
                criterion_assessments=[
                    {"criterion_id": x["id"], "evidence": "Verified fixed purpose result."}
                    for x in purpose["contract"]["criteria"]
                ] if decision == "achieved" else [],
                idempotency_key="decide-queued-purpose",
            ),
        )
    return service, work["id"], wake["id"], item


@pytest.mark.parametrize("decision", ["assigned", "waiting", "achieved"])
def test_decided_purpose_wake_is_removed_without_reopening_work(
    system, settings, tmp_path, monkeypatch, decision
):
    async def scenario():
        service, work_id, message_id, item = _queued_purpose(system, tmp_path, decision)
        before = service.get_work(work_id)
        host = QueueHost([item])
        _host(monkeypatch, host)

        assert await Dispatcher(service, settings).reconcile_cao_notification_queues() == 1
        assert host.deleted == [item["id"]]
        delivery = service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id=?", (message_id,)
        )
        assert delivery["state"] == "dead"
        assert delivery["delivered_at"] is not None
        assert delivery["acknowledged_at"] is None and delivery["handled_at"] is None
        after = service.get_work(work_id)
        for key in ["state", "generation", "goal_version", "reviews", "open_boundaries", "worker_outputs"]:
            assert after[key] == before[key]
        assert await Dispatcher(service, settings).reconcile_cao_notification_queues() == 0
        assert host.deleted == [item["id"]]

    asyncio.run(scenario())


@pytest.mark.parametrize("condition", ["pending", "unknown", "edited", "consumed"])
def test_unresolved_or_unconfirmed_purpose_wake_is_preserved(
    system, settings, tmp_path, monkeypatch, condition
):
    async def scenario():
        service, _, message_id, item = _queued_purpose(
            system, tmp_path, "pending" if condition == "pending" else "assigned"
        )
        if condition == "unknown":
            service.db.execute(
                "UPDATE message_deliveries SET state='dispatched' WHERE message_id=?", (message_id,)
            )
        if condition == "edited":
            item["input"][0]["text"] = "A user edited this queued input."
        host = QueueHost([item])
        if condition == "consumed":
            host.before_delete = lambda: host.items.clear()
        _host(monkeypatch, host)
        before = dict(service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id=?", (message_id,)
        ))

        assert await Dispatcher(service, settings).reconcile_cao_notification_queues() == 0
        assert host.deleted == []
        assert dict(service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id=?", (message_id,)
        )) == before

    asyncio.run(scenario())


def test_older_purpose_wake_is_removed_while_current_generation_remains_pending(
    system, settings, tmp_path, monkeypatch
):
    from test_purpose_continuation import _pending, _wake

    from cao_control_plane import purpose_continuation

    async def scenario():
        service, work_id, old_message_id, old_item = _queued_purpose(system, tmp_path, "waiting")
        monkeypatch.setattr(purpose_continuation, "utc_now", lambda: "2101-01-01T00:00:00Z")
        assert service.reconcile_cao_supervision_obligations() == 1
        work = service.get_work(work_id)
        current = _pending(service, work)
        assert current["generation"] == 3 and current["state"] == "pending"
        current_wake = _wake(service, current)
        attachment_id = service.db.fetchone(
            "SELECT supervisor_attachment_id FROM work_items WHERE id=?", (work_id,)
        )[0]
        current_item = _queue_admission(service, {"_cao_attachment_id": attachment_id}, current_wake)
        host = QueueHost([old_item, current_item])
        current_item["id"] = "current-purpose-wake"
        _host(monkeypatch, host)

        assert await Dispatcher(service, settings).reconcile_cao_notification_queues() == 1
        assert host.deleted == [old_item["id"]]
        assert host.items == [current_item]
        assert service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id=?", (old_message_id,)
        )[0] == "dead"
        assert service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id=?", (current_wake["id"],)
        )[0] == "delivered"
        assert _pending(service, work) == current

    asyncio.run(scenario())


@pytest.mark.parametrize("error", [
    {"code": -32600, "message": "session 00000000-0000-0000-0000-000000000001 is archived. Run `codex unarchive 00000000-0000-0000-0000-000000000001` to unarchive it first."},
    {"code": -32603, "message": "failed to read thread: invalid thread-store request: no rollout found for thread id 00000000-0000-0000-0000-000000000001"},
])
def test_historical_unavailable_queue_preserves_delivery_without_reporting_live_failure(
    system, settings, monkeypatch, error,
):
    async def scenario():
        service = system["service"]
        _, _, _, fixture = _queued_output(system)
        _terminal(service, fixture["binding"])
        before = [tuple(row) for row in service.db.fetchall("SELECT * FROM message_deliveries ORDER BY message_id, recipient_id")]
        host = QueueHost([fixture["item"]])
        request = host.request

        async def unavailable(method, params, **kwargs):
            if method == "thread/queue/list":
                host.methods.append(method)
                raise CodexRpcError(error)
            return await request(method, params, **kwargs)

        host.request = unavailable
        _host(monkeypatch, host)
        dispatcher = Dispatcher(service, settings)
        assert await dispatcher.reconcile_cao_notification_queues() == 0
        assert dispatcher.cao_notification_queue_error == ""
        assert dispatcher.cao_notification_queue_failure_counts == {}
        assert host.methods == ["initialize", "thread/queue/list"]
        assert host.deleted == []
        assert [tuple(row) for row in service.db.fetchall("SELECT * FROM message_deliveries ORDER BY message_id, recipient_id")] == before
        assert not service.db.fetchall("SELECT * FROM events WHERE event_type LIKE 'cao.notification_queue_remov%'")

    asyncio.run(scenario())


def test_handled_wake_is_removed_from_native_queue_without_rehandling(
    system, settings, monkeypatch
):
    async def scenario():
        service = system["service"]
        actor, _, _, fixture = _queued_output(system)
        message_id = fixture["message_id"]
        service.acknowledge(actor, AckInput(message_ids=[message_id]))
        service.mark_message_handled(
            actor, message_id, evidence="The exact output was incorporated."
        )
        before = dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id=?", (message_id,)
            )
        )
        unrelated = {"id": "user-input", "clientUserMessageId": "user-owned", "input": []}
        host = QueueHost([fixture["item"], unrelated])
        _host(monkeypatch, host)

        await Dispatcher(service, settings).run_once()

        assert host.deleted == [fixture["item"]["id"]]
        assert host.items == [unrelated]
        assert (
            dict(
                service.db.fetchone(
                    "SELECT * FROM message_deliveries WHERE message_id=?", (message_id,)
                )
            )
            == before
        )
        # A new Dispatcher after restart must not replay a removed notification.
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.deleted == [fixture["item"]["id"]]

    asyncio.run(scenario())


def test_later_boundary_coalesces_only_pending_informational_wake(system, settings, monkeypatch):
    async def scenario():
        service = system["service"]
        _, work_id, _, fixture = _queued_output(system)
        _terminal(service, fixture["binding"])
        before = service.get_work(work_id)
        host = QueueHost([fixture["item"]])
        _host(monkeypatch, host)

        await Dispatcher(service, settings).reconcile_cao_notification_queues()

        assert host.deleted == [fixture["item"]["id"]]
        delivery = service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id=?", (fixture["message_id"],)
        )
        assert delivery["state"] == "dead"
        assert delivery["acknowledged_at"] is None and delivery["handled_at"] is None
        after = service.get_work(work_id)
        assert after["open_boundaries"] == before["open_boundaries"]
        assert after["reviews"] == before["reviews"]
        assert after["worker_outputs"] == before["worker_outputs"]

    asyncio.run(scenario())


def test_unread_current_output_and_user_edited_queue_input_are_preserved(
    system, settings, monkeypatch
):
    async def scenario():
        service = system["service"]
        actor, _, _, fixture = _queued_output(system)
        host = QueueHost([fixture["item"]])
        _host(monkeypatch, host)
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.deleted == []
        service.acknowledge(actor, AckInput(message_ids=[fixture["message_id"]]))
        service.mark_message_handled(
            actor, fixture["message_id"], evidence="The output was incorporated."
        )
        fixture["item"]["input"][0]["text"] = "A user changed this queued input."
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.deleted == []

    asyncio.run(scenario())


def test_consumption_race_does_not_mark_unread_output_handled(system, settings, monkeypatch):
    async def scenario():
        service = system["service"]
        _, _, _, fixture = _queued_output(system)
        _terminal(service, fixture["binding"])
        host = QueueHost([fixture["item"]])
        host.before_delete = lambda: host.items.clear()
        _host(monkeypatch, host)
        before = dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id=?", (fixture["message_id"],)
            )
        )

        assert await Dispatcher(service, settings).reconcile_cao_notification_queues() == 0

        assert host.deleted == []
        assert (
            dict(
                service.db.fetchone(
                    "SELECT * FROM message_deliveries WHERE message_id=?", (fixture["message_id"],)
                )
            )
            == before
        )

    asyncio.run(scenario())


@pytest.mark.parametrize("wake_kind", ["output", "purpose"])
def test_unknown_deletion_is_observed_again_without_blind_retry(
    system, settings, tmp_path, monkeypatch, wake_kind
):
    async def scenario():
        service = system["service"]
        if wake_kind == "purpose":
            _, _, message_id, item = _queued_purpose(system, tmp_path, "assigned")
            fixture = {"message_id": message_id, "item": item}
        else:
            _, _, _, fixture = _queued_output(system)
            _terminal(service, fixture["binding"])
        host = QueueHost([fixture["item"]])
        request = host.request

        async def uncertain(method, params, **kwargs):
            result = await request(method, params, **kwargs)
            if method == "thread/queue/delete":
                raise TimeoutError("response lost after removal")
            return result

        host.request = uncertain
        _host(monkeypatch, host)
        dispatcher = Dispatcher(service, settings)
        await dispatcher.reconcile_cao_notification_queues()
        assert dispatcher.cao_notification_queue_error
        assert dispatcher.status()["cao_notification_queue_failure_counts"] == {"provider_timeout": 1}
        assert dispatcher.status()["cao_notification_queue_unknown_outcomes"] == 1
        assert host.deleted == [fixture["item"]["id"]]
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.methods.count("thread/queue/delete") == 1
        assert host.methods.count("thread/queue/list") == 2
        assert Dispatcher(service, settings).status()["cao_notification_queue_unknown_outcomes"] == 1
        assert (
            service.db.fetchone(
                "SELECT state FROM message_deliveries WHERE message_id=?", (fixture["message_id"],)
            )[0]
            == "delivered"
        )

    asyncio.run(scenario())


def test_delivery_generation_change_before_delete_is_fenced(system, settings, monkeypatch):
    async def scenario():
        service = system["service"]
        actor, _, _, fixture = _queued_output(system)
        service.acknowledge(actor, AckInput(message_ids=[fixture["message_id"]]))
        service.mark_message_handled(
            actor, fixture["message_id"], evidence="The result is incorporated."
        )
        host = QueueHost([fixture["item"]])
        request = host.request

        async def change_generation(method, params, **kwargs):
            result = await request(method, params, **kwargs)
            if method == "thread/queue/list":
                service.db.execute(
                    "UPDATE message_deliveries SET generation=generation+1 WHERE message_id=?",
                    (fixture["message_id"],),
                )
            return result

        host.request = change_generation
        _host(monkeypatch, host)
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.deleted == []

    asyncio.run(scenario())


def test_queued_automatic_output_coalesces_without_waiting_for_native_dispatch(system):
    service = system["service"]
    _, work_id, _, fixture = _queued_output(system)
    service.db.execute(
        "UPDATE message_deliveries SET state='queued', delivered_at=NULL WHERE message_id=?",
        (fixture["message_id"],),
    )
    _terminal(service, fixture["binding"])
    before = service.get_work(work_id)
    assert service.reconcile_cao_progress_wake_deliveries() == 1
    row = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id=?", (fixture["message_id"],)
    )
    assert row["state"] == "dead"
    assert row["acknowledged_at"] is None and row["handled_at"] is None
    assert service.get_work(work_id)["worker_outputs"] == before["worker_outputs"]


def test_unknown_dispatch_is_not_cancellation_authority(system, settings, monkeypatch):
    async def scenario():
        service = system["service"]
        _, _, _, fixture = _queued_output(system)
        _terminal(service, fixture["binding"])
        service.db.execute(
            "UPDATE message_deliveries SET state='dispatched' WHERE message_id=?",
            (fixture["message_id"],),
        )
        host = QueueHost([fixture["item"]])
        _host(monkeypatch, host)
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.deleted == []

    asyncio.run(scenario())


def test_official_socket_protocol_removes_only_the_exact_queued_submission(
    system, settings, monkeypatch
):
    async def scenario():
        service = system["service"]
        actor, _, attachment_id, fixture = _queued_output(system)
        service.acknowledge(actor, AckInput(message_ids=[fixture["message_id"]]))
        service.mark_message_handled(
            actor, fixture["message_id"], evidence="The output was incorporated."
        )
        thread_id = service.get_cao_attachment(attachment_id)["native_thread_id"]
        requests = []
        errors = []

        async def handle(reader, writer):
            try:
                await _accept_websocket_client(reader, writer)
                initialize = await _read_websocket_client_message(reader)
                assert initialize["method"] == "initialize"
                await _write_websocket_server_message(
                    writer, {"id": initialize["id"], "result": {}}
                )
                initialized = await _read_websocket_client_message(reader)
                assert initialized["method"] == "initialized"
                for page in range(2):
                    request = await _read_websocket_client_message(reader)
                    requests.append(request["method"])
                    assert request["method"] == "thread/queue/list"
                    assert request["params"]["threadId"] == thread_id
                    if page:
                        assert request["params"]["cursor"] == "next-page"
                    await _write_websocket_server_message(
                        writer,
                        {
                            "id": request["id"],
                            "result": {
                                "data": [fixture["item"]]
                                if page
                                else [
                                    {
                                        "id": "user-input",
                                        "clientUserMessageId": "user-owned",
                                        "input": [],
                                    }
                                ],
                                "nextCursor": None if page else "next-page",
                            },
                        },
                    )
                request = await _read_websocket_client_message(reader)
                requests.append(request["method"])
                assert request["method"] == "thread/queue/delete"
                assert request["params"] == {
                    "threadId": thread_id,
                    "queuedSubmissionId": fixture["item"]["id"],
                }
                await _write_websocket_server_message(
                    writer, {"id": request["id"], "result": {"deleted": True}}
                )
            except Exception as error:
                errors.append(error)
            finally:
                writer.close()
                await writer.wait_closed()

        with tempfile.TemporaryDirectory(prefix="cao-queue-", dir="/tmp") as directory:
            socket_path = Path(directory) / "queue.sock"
            server = await asyncio.start_unix_server(handle, path=socket_path)
            socket_path.chmod(0o600)
            monkeypatch.setattr(
                "cao_control_plane.runtime._CODEX_DESKTOP_CONTROL_SOCKET", socket_path
            )
            async with server:
                removed = await Dispatcher(service, settings).reconcile_cao_notification_queues()
            assert not errors
            assert removed == 1
            assert requests == ["thread/queue/list", "thread/queue/list", "thread/queue/delete"]

    asyncio.run(scenario())


def test_handling_commit_wakes_cleanup_without_waiting_for_periodic_scan(
    system, settings, monkeypatch
):
    async def scenario():
        service = system["service"]
        actor, _, _, fixture = _queued_output(system)
        host = QueueHost([fixture["item"]])
        _host(monkeypatch, host)
        deleted = asyncio.Event()
        host.before_delete = deleted.set
        dispatcher = Dispatcher(service, settings)
        dispatcher._cao_queue_next_at = asyncio.get_running_loop().time() + 3600
        dispatcher._cao_queue_cleanup_needed = False
        service.acknowledge(actor, AckInput(message_ids=[fixture["message_id"]]))
        service.mark_message_handled(
            actor, fixture["message_id"], evidence="The result was incorporated."
        )
        await dispatcher.start()
        try:
            await asyncio.wait_for(deleted.wait(), timeout=2)
            assert host.deleted == [fixture["item"]["id"]]
        finally:
            await dispatcher.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("field", ["goal_version", "goal_packet_digest", "task_packet_digest"])
def test_informational_wake_is_not_superseded_by_a_different_sealed_task(
    system, settings, monkeypatch, field
):
    async def scenario():
        service = system["service"]
        _, work_id, _, fixture = _queued_output(system)
        _terminal(service, fixture["binding"])
        # Preserve anomalous or independently revised packet evidence instead
        # of treating a familiar Work/Attempt identity as equivalence.
        replacement = 2 if field == "goal_version" else "f" * 64
        service.db.execute(
            f"UPDATE messages SET {field}=? WHERE work_item_id=? AND id<>?",
            (replacement, work_id, fixture["message_id"]),
        )
        host = QueueHost([fixture["item"]])
        _host(monkeypatch, host)
        await Dispatcher(service, settings).reconcile_cao_notification_queues()
        assert host.deleted == []

    asyncio.run(scenario())
