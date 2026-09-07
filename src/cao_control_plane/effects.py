"""Crash-safe execution edge for effects authorized by the control-plane kernel.

The database owns authority and outcome state.  This adapter only validates the
exact local invocation, reserves it transactionally, executes without a shell,
and records a terminal result.  A process loss after reservation is reconciled
to ``unknown`` and is never retried blindly.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .errors import ValidationError
from .models import EffectCheckInput, EffectResolveInput, EffectStatus
from .security import sha256_json, sha256_text
from .service import ControlPlane


def effect_argv_digest(argv: Sequence[str]) -> str:
    return sha256_json(list(argv))


def effect_workdir_digest(workdir: Path) -> str:
    return sha256_text(str(workdir.resolve(strict=True)))


def _evidence(*, returncode: int, stdout: bytes, stderr: bytes) -> str:
    output_digest = hashlib.sha256(stdout + b"\x00" + stderr).hexdigest()
    return f"exit_code={returncode};output_sha256={output_digest}"


class EffectExecutor:
    """Execute exact argv only after the durable authority reservation commits."""

    def __init__(self, service: ControlPlane) -> None:
        self.service = service

    def recover_incomplete(self, actor: dict[str, Any]) -> int:
        """Fence operations left STARTED by a prior process crash as UNKNOWN."""

        rows = self.service.db.fetchall(
            "SELECT id FROM effect_operations WHERE status = 'started' ORDER BY created_at"
        )
        for row in rows:
            self.service.resolve_effect(
                actor,
                str(row["id"]),
                EffectResolveInput(
                    status=EffectStatus.UNKNOWN,
                    evidence="executor restarted after durable reservation; verify target state",
                ),
            )
        return len(rows)

    def run(
        self,
        actor: dict[str, Any],
        request: EffectCheckInput,
        argv: Sequence[str],
        *,
        workdir: Path,
        timeout_seconds: float = 300.0,
        environment: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        values = tuple(str(value) for value in argv)
        if not values or not values[0]:
            raise ValidationError("effect execution requires a non-empty argv")
        if any("\x00" in value for value in values):
            raise ValidationError("effect argv contains a NUL byte")
        if timeout_seconds <= 0:
            raise ValidationError("effect timeout must be positive")
        resolved_workdir = workdir.resolve(strict=True)
        if not resolved_workdir.is_dir():
            raise ValidationError("effect workdir must be a directory")
        actual_argv_digest = effect_argv_digest(values)
        actual_workdir_digest = effect_workdir_digest(resolved_workdir)
        if request.argv_digest != actual_argv_digest:
            raise ValidationError(
                "effect argv does not match the authorized request digest",
                expected=request.argv_digest,
                actual=actual_argv_digest,
            )
        if request.workdir_digest != actual_workdir_digest:
            raise ValidationError(
                "effect workdir does not match the authorized request digest",
                expected=request.workdir_digest,
                actual=actual_workdir_digest,
            )

        env = os.environ.copy()
        if environment:
            for key, value in environment.items():
                if not key or "=" in key or "\x00" in key or "\x00" in value:
                    raise ValidationError("effect environment contains an invalid entry")
                env[key] = value
        operation = self.service.start_effect(actor, request)
        try:
            completed = subprocess.run(
                values,
                cwd=resolved_workdir,
                env=env,
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=timeout_seconds,
            )
        except BaseException as error:
            self._resolve_unknown(
                actor,
                operation["id"],
                f"executor interrupted after reservation;type={type(error).__name__}",
            )
            raise

        status = (
            EffectStatus.SUCCEEDED
            if completed.returncode == 0
            else EffectStatus.FAILED
        )
        return self.service.resolve_effect(
            actor,
            operation["id"],
            EffectResolveInput(
                status=status,
                evidence=_evidence(
                    returncode=completed.returncode,
                    stdout=completed.stdout,
                    stderr=completed.stderr,
                ),
            ),
        )

    def _resolve_unknown(self, actor: dict[str, Any], operation_id: str, evidence: str) -> None:
        self.service.resolve_effect(
            actor,
            operation_id,
            EffectResolveInput(status=EffectStatus.UNKNOWN, evidence=evidence),
        )
