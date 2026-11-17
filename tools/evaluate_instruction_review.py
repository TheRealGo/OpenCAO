"""Run fixed synthetic instruction cases through the real Codex reviewer.

This reports observed case decisions, latency and token usage. It does not
estimate general production accuracy or grant any instruction/effect authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from cao_control_plane.canonical import canonical_sha256
from cao_control_plane.config import Settings
from cao_control_plane.instruction_review import REVIEW_POLICY_DIGEST, CodexInstructionReviewer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--split", choices=("development", "evaluation"), required=True)
    args = parser.parse_args()
    encoded = args.cases.read_bytes()
    cases = [item for item in json.loads(encoded) if item["split"] == args.split]
    settings = Settings.load(args.config)

    def run(case: dict[str, Any]) -> dict[str, Any]:
        try:
            result = CodexInstructionReviewer(settings).review(case["snapshot"])
            return {
                "id": case["id"],
                "expected": case["expected"],
                "observed": result.decision.verdict,
                "matches": result.decision.verdict == case["expected"],
                "elapsed_ms": result.elapsed_ms,
                "requested_model": result.model,
                "usage": result.usage,
                "snapshot_digest": canonical_sha256(case["snapshot"]),
                "decision": result.decision.model_dump(mode="json"),
            }
        except (ValueError, OSError, TimeoutError, subprocess.SubprocessError) as error:
            return {
                "id": case["id"],
                "expected": case["expected"],
                "observed": "unavailable",
                "matches": False,
                "error_type": type(error).__name__,
                "snapshot_digest": canonical_sha256(case["snapshot"]),
            }

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, cases))
    report = {
        "format": "cao-instruction-review-evaluation/v1",
        "review_policy_digest": REVIEW_POLICY_DIGEST,
        "split": args.split,
        "cases_digest": hashlib.sha256(encoded).hexdigest(),
        "case_count": len(cases),
        "matches": sum(bool(item["matches"]) for item in results),
        "results": results,
        "limitation": "Fixed synthetic cases; not a general accuracy estimate or effect authorization.",
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(report, destination, ensure_ascii=False, indent=2)
        destination.write("\n")
    print(json.dumps({key: report[key] for key in ("split", "case_count", "matches")}))


if __name__ == "__main__":
    main()
