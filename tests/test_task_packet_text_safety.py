from __future__ import annotations

# ruff: noqa: RUF001 -- Unicode punctuation is the input under regression.
import pytest

from cao_control_plane.errors import ValidationError
from cao_control_plane.service import _require_worker_safe_task_payload


@pytest.mark.parametrize("text", [
    "共有利用枠70%/30%を維持する。",
    "直近49%/10080分枠は単時点値である。",
    "現在0%/ordinaryUsageAllowed=true、上限を維持する。",
    "確認OK（内容を確認済み）／成果物PRを記録する。",
    "資料の作成」／成果物は社内20ページ版・提示用19ページ版のPPTX/PDF。",
    "PPTX/PDFとgain/lossを確認する。",
])
def test_task_packet_accepts_measurement_and_enumeration_prose(text):
    _require_worker_safe_task_payload({"instruction": text})


@pytest.mark.parametrize("text", [
    "/owner/private/evidence.txt",
    "確認済み)/owner/private/evidence.txt",
    "50%/owner/private/evidence.txt",
    "0%/ordinaryUsageAllowed=true/private/evidence.txt",
    "記録済み）／秘密／証拠.txt",
    "読む「／秘密／証拠.txt」",
    "読む /秘密・資料/証拠.txt",
    "読む ~/private/evidence.txt",
    "読む file:///owner/private/evidence.txt",
    "読む https://localhost/private/evidence.txt",
])
def test_task_packet_still_rejects_concrete_private_locators(text):
    with pytest.raises(ValidationError):
        _require_worker_safe_task_payload({"instruction": text})
