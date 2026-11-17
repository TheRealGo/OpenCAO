"""Task-owned command capture; no Control Plane credentials or effect authority."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_preservation_edge import ARTIFACT_CONTENT_MAX_BYTES
from .security import contains_control_plane_secret, contains_generic_credential_text


def _write(directory: int, name: str, content: bytes) -> None:
    descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=directory)
    with os.fdopen(descriptor, "wb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


def _receipt(directory: int, value: dict[str, Any], *, initial: bool = False) -> None:
    name = "manifest.json" if initial else "manifest.next"
    _write(directory, name, (json.dumps(value, indent=2) + "\n").encode())
    if not initial:
        os.replace(name, "manifest.json", src_dir_fd=directory, dst_dir_fd=directory)
    os.fsync(directory)


async def _read(stream: asyncio.StreamReader, limit: int) -> dict[str, Any]:
    captured = bytearray()
    observed = 0
    digest = hashlib.sha256()
    while block := await stream.read(65536):
        observed += len(block)
        digest.update(block)
        captured.extend(block[:max(0, limit - len(captured))])
    text = captured.decode("utf-8", errors="replace")
    sensitive = contains_control_plane_secret(text) or contains_generic_credential_text(text)
    return {"content": bytes(captured) if not sensitive and observed <= limit else None,
            "state": "withheld" if sensitive else "truncated" if observed > limit else "complete",
            "observed_byte_count": observed, "observed_sha256": digest.hexdigest()}


async def capture_command(
    command: list[str], output_directory: Path, *,
    max_stream_bytes: int = ARTIFACT_CONTENT_MAX_BYTES, timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Execute once under the caller's authority; an incomplete receipt never permits replay."""
    if not command or not 1 <= max_stream_bytes <= 64 * 1024 * 1024:
        raise ValueError("invalid_capture_request")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("invalid_capture_timeout")
    target = output_directory.parent.resolve(strict=True) / output_directory.name
    target.mkdir(mode=0o700)  # Exclusive: never overwrite an older run or follow its symlink.
    directory = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    started = time.monotonic()
    receipt: dict[str, Any] = {
        "format": "cao-command-capture/v1", "state": "started", "capture_complete": False,
        "started_at": datetime.now(UTC).isoformat(), "returncode": None,
        "effect_outcome": "not_assessed", "stdout": None, "stderr": None,
    }
    process = None
    readers: list[asyncio.Task[dict[str, Any]]] = []
    try:
        _receipt(directory, receipt, initial=True)
        try:
            process = await asyncio.create_subprocess_exec(
                *command, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as error:
            receipt.update(state="launch_failed", failure_errno=error.errno)
        else:
            assert process.stdout is not None and process.stderr is not None
            readers = [asyncio.create_task(_read(stream, max_stream_bytes))
                       for stream in (process.stdout, process.stderr)]
            try:
                await asyncio.wait_for(process.wait(), timeout=timeout_seconds)
                receipt["state"] = "exited"
            except TimeoutError:
                from .runtime import _terminate_runtime_process

                await _terminate_runtime_process(process)
                receipt["state"] = "timed_out"
            receipt["returncode"] = process.returncode
            for name, captured in zip(("stdout", "stderr"), await asyncio.gather(*readers), strict=True):
                content = captured.pop("content")
                captured["file"] = None
                if content is not None:
                    filename = name + ".txt"
                    _write(directory, filename, content)
                    captured.update(file=filename, captured_byte_count=len(content),
                                    sha256=hashlib.sha256(content).hexdigest())
                receipt[name] = captured
            receipt["capture_complete"] = receipt["state"] == "exited" and all(
                receipt[name]["state"] == "complete" for name in ("stdout", "stderr")
            )
    except BaseException:
        receipt["state"] = "interrupted"
        receipt["capture_complete"] = False
        if process is not None and process.returncode is None:
            from .runtime import _terminate_runtime_process

            await _terminate_runtime_process(process)
        for reader in readers:
            if not reader.done():
                reader.cancel()
        if readers:
            await asyncio.gather(*readers, return_exceptions=True)
        raise
    finally:
        receipt["finished_at"] = datetime.now(UTC).isoformat()
        receipt["elapsed_seconds"] = round(time.monotonic() - started, 6)
        try:
            _receipt(directory, receipt)
        finally:
            os.close(directory)
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--max-stream-bytes", type=int, default=ARTIFACT_CONTENT_MAX_BYTES)
    parser.add_argument("--timeout-seconds", type=float)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command after -- is required")
    try:
        receipt = asyncio.run(capture_command(command, args.output_directory,
            max_stream_bytes=args.max_stream_bytes, timeout_seconds=args.timeout_seconds))
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError):
        print("cao-capture: capture request failed; no automatic retry", file=sys.stderr)
        return 125
    print(json.dumps(receipt))  # Only bounded metadata, never output, argv, environment or paths.
    status = receipt["returncode"]
    if type(status) is int and status != 0:
        return min(status, 255) if status > 0 else min(128 - status, 255)
    return 0 if receipt["capture_complete"] else 125


if __name__ == "__main__":
    raise SystemExit(main())
