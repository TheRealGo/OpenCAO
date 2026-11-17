from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from cao_control_plane.execution_capture import capture_command


def run_capture(path, code, **options):
    return asyncio.run(capture_command([sys.executable, "-c", code], path, **options))


def test_actual_stdout_stderr_exit_and_empty_stream_survive_execution(tmp_path):
    path = tmp_path / "run"
    result = run_capture(path, "import sys;print('RESULT');sys.exit(7)")
    assert result["returncode"] == 7 and result["state"] == "exited"
    assert result["capture_complete"] is True
    assert (path / "stdout.txt").read_bytes() == b"RESULT\n"
    assert (path / "stderr.txt").read_bytes() == b""
    for name in ("stdout", "stderr"):
        data = (path / result[name]["file"]).read_bytes()
        assert result[name]["sha256"] == result[name]["observed_sha256"] == hashlib.sha256(data).hexdigest()
    assert json.loads((path / "manifest.json").read_text()) == result
    assert path.stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in path.iterdir())
    encoded = json.dumps(result)
    assert str(tmp_path) not in encoded and "sys.exit" not in encoded and sys.executable not in encoded


def test_both_pipes_are_drained_concurrently_without_losing_bytes(tmp_path):
    path = tmp_path / "run"
    result = run_capture(path, "import sys;sys.stdout.write('a'*200000);sys.stderr.write('b'*200000)")
    assert result["capture_complete"]
    assert (path / "stdout.txt").read_bytes() == b"a" * 200000
    assert (path / "stderr.txt").read_bytes() == b"b" * 200000


@pytest.mark.parametrize("text", [
    "cao.rtc_abcdefgh.abcdefghijklmnop",
    "Bearer " + "x" * 30,
    "https://example.invalid/api?service-runtime-proxy-token=" + "P" * 32,
])
def test_sensitive_output_is_withheld_before_any_output_file_is_written(tmp_path, text):
    path = tmp_path / "run"
    result = run_capture(path, f"print({text!r})")
    assert result["stdout"]["state"] == "withheld" and result["stdout"]["file"] is None
    assert not result["capture_complete"] and not (path / "stdout.txt").exists()
    assert text not in (path / "manifest.json").read_text()
    assert result["stdout"]["observed_sha256"] == hashlib.sha256((text + "\n").encode()).hexdigest()


def test_limit_never_turns_partial_output_into_complete_evidence(tmp_path):
    path = tmp_path / "run"
    result = run_capture(path, "print('a'*100)", max_stream_bytes=8)
    assert result["returncode"] == 0 and not result["capture_complete"]
    assert result["stdout"]["state"] == "truncated"
    assert result["stdout"]["observed_byte_count"] == 101
    assert result["stdout"]["file"] is None and not (path / "stdout.txt").exists()


def test_a_timeout_stops_only_the_owned_process_and_preserves_uncertainty(tmp_path):
    path = tmp_path / "run"
    result = run_capture(path, "import time;print('started',flush=True);time.sleep(60)", timeout_seconds=0.2)
    assert result["state"] == "timed_out" and not result["capture_complete"]
    assert result["returncode"] is not None and result["returncode"] != 0
    assert result["effect_outcome"] == "not_assessed"
    assert (path / "stdout.txt").read_text() == "started\n"


def test_launch_failure_does_not_save_the_private_command_or_claim_execution(tmp_path):
    path = tmp_path / "run"
    result = asyncio.run(capture_command([str(tmp_path / "missing-secret-command")], path))
    assert result["state"] == "launch_failed" and result["returncode"] is None
    assert not result["capture_complete"]
    assert "missing-secret-command" not in (path / "manifest.json").read_text()


@pytest.mark.parametrize("symlink", [False, True])
def test_an_existing_run_or_symlink_is_never_replaced_or_executed(tmp_path, symlink):
    original = tmp_path / "original"
    original.mkdir()
    marker = original / "keep"
    marker.write_text("retained")
    destination = tmp_path / "run"
    if symlink:
        destination.symlink_to(original, target_is_directory=True)
    else:
        destination.mkdir()
    with pytest.raises(FileExistsError):
        run_capture(destination, f"from pathlib import Path;Path({str(marker)!r}).unlink()")
    assert marker.read_text() == "retained"


def test_capture_at_the_actual_program_not_just_the_outer_client(tmp_path):
    # Model a remote client exposing only the process status, as some notebook displays do.
    baseline = subprocess.run([sys.executable, "-c",
        "import subprocess,sys;r=subprocess.run([sys.executable,'-c',\"print('INNER_RESULT')\"],capture_output=True);print(subprocess.CompletedProcess(r.args,r.returncode))"],
        capture_output=True, check=True)
    assert baseline.stdout.startswith(b"CompletedProcess(") and b"stdout=" not in baseline.stdout
    assert not list(tmp_path.iterdir())
    path = tmp_path / "inner-run"
    result = run_capture(path, "print('INNER_RESULT')")
    assert result["capture_complete"] and (path / "stdout.txt").read_bytes() == b"INNER_RESULT\n"


def test_cli_reports_only_metadata_and_propagates_the_real_nonzero_exit(tmp_path):
    result = subprocess.run([sys.executable, "-m", "cao_control_plane.execution_capture",
        "--output-directory", str(tmp_path / "run"), "--", sys.executable,
        "-c", "import sys;print('PRIVATE_RESULT');sys.exit(9)"], capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")})
    assert result.returncode == 9
    assert "PRIVATE_RESULT" not in result.stdout and str(tmp_path) not in result.stdout
    assert json.loads(result.stdout)["returncode"] == 9
