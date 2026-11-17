from __future__ import annotations

import pytest
from test_cao_session_wake import _attach, _worker_boundary
from test_report_first_supervision import _registered, _report, _structured_claim
from test_verified_artifact_content_read import _attached_cao
from test_worker_output_delivery import _capture_fixture, _dispose, _event, _terminal

from cao_control_plane.errors import AuthorizationError, NotFoundError
from cao_control_plane.mcp import CAO_TOOLS, COMMON_TOOLS, MCPServer
from cao_control_plane.models import AckInput
from cao_control_plane.notifications import notification_message_id
from cao_control_plane.runtime import render_message, render_message_details


def test_compact_wake_identifies_worker_task_and_requested_decision(system):
    service = system["service"]
    service.db.execute("UPDATE principals SET operator_scope='acceptance-test', operator_label='検証担当' WHERE id=?", (system["worker"]["id"],))
    actor, work, _ = _registered(system)
    _structured_claim(system, actor, work, _report())
    [notice] = service.get_inbox(actor)["items"]
    prompt = render_message(notice)
    assert prompt.splitlines() == [
        "検証担当 | 完了申告",
        "作業: Verified artifact read report-record",
        "CAOへ: 報告を確認し、受理または継続を判断してください。",
        f'報告と処理手順: cao_get_inbox(message_id="{notice["id"]}") を読んでください。',
    ]
    assert notification_message_id(prompt) == notice["id"]
    assert work["id"] not in prompt
    assert "Goal packet digest" not in prompt
    exact = MCPServer(service).call_tool(actor, "cao_get_inbox", {"message_id": notice["id"]})
    [report] = exact["items"]
    assert report["payload"]["decision_report"] == _report().model_dump(mode="json")
    assert report["processing_instructions"] == render_message_details(notice)
    assert "dispose this boundary exactly once" in report["processing_instructions"]
    assert "do not drain" in report["processing_instructions"].lower()
    assert not report["acknowledged"]
    assert not service.get_work(work["id"])["reviews"]
    assert service.db.fetchone("SELECT COUNT(*) FROM boundary_dispositions")[0] == 0


def test_exact_lookup_cannot_select_another_attachment_or_bypass_worker_fifo(system):
    service = system["service"]
    attachment = _attach(system, thread_id="notification-owner")
    _worker_boundary(system, attachment)
    owner = service.authenticate(attachment["context_token"])
    [notice] = service.get_inbox(owner)["items"]
    _, other = _attached_cao(system, thread_id="notification-other", project_marker="c")
    with pytest.raises(NotFoundError):
        MCPServer(service).call_tool(other, "cao_get_inbox", {"message_id": notice["id"]})
    with pytest.raises(AuthorizationError):
        service.get_inbox(system["worker"], message_id=notice["id"])
    assert "message_id" in CAO_TOOLS["cao_get_inbox"]["inputSchema"]["properties"]
    assert "message_id" not in COMMON_TOOLS["cao_get_inbox"]["inputSchema"]["properties"]


def test_exact_lookup_retains_old_report_after_later_delivery_and_handling(system):
    service = system["service"]
    attachment = _attach(system, thread_id="notification-history")
    first, _ = _worker_boundary(system, attachment)
    actor = service.authenticate(attachment["context_token"])
    [notice] = service.get_inbox(actor)["items"]
    service.acknowledge(actor, AckInput(message_ids=[notice["id"]]))
    _dispose(service, actor, first, kind="correct")
    service.mark_message_handled(actor, notice["id"], evidence="The correction committed.")
    second, _ = _worker_boundary(system, attachment)
    result = MCPServer(service).call_tool(actor, "cao_get_inbox", {"message_id": notice["id"]})
    [exact] = result["items"]
    assert exact["work_item_id"] == first["id"] != second["id"]
    assert exact["delivery_state"] == "handled"
    assert first["id"] in exact["processing_instructions"]
    assert second["id"] not in exact["processing_instructions"]
    assert result["next_cursor"] is None


def test_provider_report_lookup_preserves_exact_final_references(system):
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event(text="A complete provider answer."))
    terminal = _terminal(service, binding)
    message_id = terminal["notification_message_id"]
    [notice] = MCPServer(service).call_tool(actor, "cao_get_inbox", {"message_id": message_id})["items"]
    assert notice["payload"]["output_refs"][0]["output_id"] == output["id"]
    assert output["id"] in notice["processing_instructions"]
    assert output["digest"] in notice["processing_instructions"]
    assert "A complete provider answer." not in render_message(service.get_inbox(actor)["items"][0])
    assert notice["work_item_id"] == work["id"]


@pytest.mark.parametrize(("kind", "action", "status", "expected_request"), [
    ("question", "", "質問", "回答または次の対応"),
    ("blocker", "", "停止要因", "次の対応"),
    ("progress", "", "進捗報告", "報告を確認"),
    ("system", "worker_output", "未検証", "受理または継続"),
    ("system", "purpose_continuation", "次の目的判断待ち", "目的への次の対応"),
    ("system", "worker_start_diagnosed", "診断結果", "必要な対応"),
])
def test_each_notice_states_its_event_and_required_action(kind, action, status, expected_request):
    message = {"id": "msg_example", "kind": kind, "payload": {
        "action": action, "notification_presentation": {"worker_label": "Worker", "task_title": "Measured task"},
    }}
    prompt = render_message(message)
    assert len(prompt.splitlines()) == 4
    assert status in prompt and expected_request in prompt
    assert notification_message_id(prompt) == message["id"]
