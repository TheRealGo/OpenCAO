from __future__ import annotations

from pathlib import Path

from cao_control_plane.canonical import canonical_json, canonical_json_bytes, canonical_sha256
from cao_control_plane.goal_packets import canonical_digest as goal_packet_digest


def test_canonical_json_contract_is_byte_stable() -> None:
    value = {"z": "白", "a": [True, None, 1]}

    assert canonical_json(value) == r'{"a":[true,null,1],"z":"\u767d"}'
    assert canonical_json_bytes(value) == b'{"a":[true,null,1],"z":"\\u767d"}'
    assert canonical_sha256(value) == "a0972894eee490735175b66d15f0af27ac86b203c555061cdc8c3c05b5a032f6"


def test_goal_packet_digest_shares_the_persisted_contract() -> None:
    value = {"sequence": 7, "format": "cao-test/v1"}

    expected = canonical_sha256(value)
    assert goal_packet_digest(value) == expected


def test_ascii_canonical_json_has_one_implementation() -> None:
    package = Path(__file__).parents[1] / "src" / "cao_control_plane"
    duplicates = [
        path.name
        for path in package.glob("*.py")
        if path.name != "canonical.py"
        and "ensure_ascii=True" in path.read_text(encoding="utf-8")
    ]

    assert duplicates == []
