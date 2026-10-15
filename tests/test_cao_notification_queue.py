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
    Dispatcher,
    _codex_delivery_client_user_message_id,
    render_message,
)


def _queued_output(system: dict[str, Any]) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = _legacy_output_notification(service, work, binding)
    message_id = output["notification_message_id"]
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
    return (
        actor,
        work["id"],
        attachment_id,
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


def test_unknown_deletion_is_observed_again_without_blind_retry(system, settings, monkeypatch):
    async def scenario():
        service = system["service"]
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
