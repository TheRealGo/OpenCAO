"""Compact presentation of durable supervisor notifications.

Presentation is sealed with the Message so labels cannot change an already
queued input. Reports, output text and processing authority stay in their
existing records; this module only formats the lookup invitation.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Mapping
from typing import Any

from .projection import sanitize_operator_text

_REPORTS = {
    "completion_claim": ("完了申告", "報告を確認し、受理または継続を判断してください。"),
    "question": ("質問", "質問を確認し、回答または次の対応を決めてください。"),
    "blocker": ("停止要因", "停止要因を確認し、次の対応を決めてください。"),
    "progress": ("進捗報告", "報告を確認してください。"),
    "artifact": ("成果物報告", "成果物の報告を確認してください。"),
}
_ACTIONS = {
    "worker_output": ("回答を自動回収(未検証)", "回答を確認し、受理または継続を判断してください。"),
    "purpose_continuation": ("次の目的判断待ち", "受理済みの結果を踏まえ、目的への次の対応を決めてください。"),
    "worker_start_unconfirmed": ("実行開始を確認できません", "実行状況を確認し、必要な対応を行ってください。"),
    "worker_start_diagnosed": ("実行状況の診断結果", "診断を確認し、必要な対応を行ってください。"),
    "cao_command_result_available": ("操作結果を取得しました", "保存された操作結果を確認してください。"),
    "requester_escalation": ("依頼者への相談", "現在の未解決事項と必要な判断を依頼者に伝えてください。"),
}


def is_supervisor_notification(kind: str, payload: Mapping[str, Any]) -> bool:
    return kind in _REPORTS or (
        kind == "system" and (bool(payload.get("boundary_id")) or payload.get("action") in _ACTIONS)
    )


def presentation_tx(
    connection: sqlite3.Connection, *, work_id: str | None, goal_version: int | None,
) -> dict[str, str]:
    row = connection.execute(
        "SELECT p.operator_label, g.title FROM work_items w "
        "JOIN principals p ON p.id=w.assigned_worker_id "
        "JOIN goal_revisions g ON g.work_item_id=w.id AND g.version=? WHERE w.id=?",
        (goal_version, work_id),
    ).fetchone()
    return {
        "worker_label": (sanitize_operator_text(row["operator_label"], limit=80) or "Worker")
        if row is not None else "CAOシステム",
        "task_title": (sanitize_operator_text(row["title"], limit=100) or "作業の報告")
        if row is not None else "操作結果の通知",
    }


def render_notification(message: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    presentation = payload["notification_presentation"]
    kind = str(message.get("kind", "system"))
    action = str(payload.get("action") or "")
    status, request = _REPORTS.get(kind, _ACTIONS.get(action, (
        "CAOの対応が必要", "状況を確認し、次の対応を決めてください。",
    )))
    if action == "requester_escalation" and payload.get("disposition") == "wait_system":
        status, request = "CAOが復旧対応中", "停止原因と影響を伝え、復旧対応を進めてください。"
    elif message.get("recovery_action") or action == "recover_terminal_worker_attempt":
        status, request = "実行の復旧判断待ち", "保存された状況と復旧手順を確認し、対応してください。"
    label = sanitize_operator_text(presentation.get("worker_label"), limit=80) or "Worker"
    title = sanitize_operator_text(presentation.get("task_title"), limit=100) or "作業の報告"
    return "\n".join((
        f"{label} | {status}",
        f"作業: {title}",
        f"CAOへ: {request}",
        f'報告と処理手順: cao_get_inbox(message_id="{message["id"]}") を読んでください。',
    ))


def notification_message_id(prompt: str) -> str | None:
    match = re.search(
        r'(?m)^(?:Delivery message ID: (msg_[a-zA-Z0-9]+)$|'
        r'報告と処理手順: cao_get_inbox\(message_id="(msg_[a-zA-Z0-9]+)"\) を読んでください。$)',
        prompt,
    )
    return next((value for value in match.groups() if value), None) if match else None
