#!/usr/bin/env python3
"""
tests/test_cli_end_to_end.py

End-to-end test for the telemetry client CLI.

This test:
 - starts an in-process TelemetryServer (TelemetryServer + TelemetryStore + MissionStore)
 - launches the CLI script src/rover/telemetry_client.py as a subprocess with PYTHONPATH=src
 - waits until the server registers the rover id
 - terminates the CLI subprocess and stops the server
"""
import asyncio
import os
import sys
import time
from pathlib import Path

import pytest

from common import binary_proto
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_hooks import register_telemetry_hooks


def _get_free_port():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_cli_end_to_end_registers_with_server(tmp_path):
    host = "127.0.0.1"
    port = _get_free_port()
    rover_id = "R-CLI-E2E"

    # start server and stores
    ms = MissionStore()
    ts = TelemetryStore(history_size=10)
    register_telemetry_hooks(ms, ts)

    srv = TelemetryServer(mission_store=ms, telemetry_store=ts, host=host, port=port)
    await srv.start()

    # locate the CLI script path relative to repository root
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "src" / "rover" / "telemetry_client.py"
    assert script_path.exists(), f"telemetry_client.py not found at {script_path}"

    # Prepare environment for subprocess so that 'src' is on PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src")

    # Launch CLI subprocess (persistent mode). Use a short interval for quick registration.
    # IMPORTANT: pass --host and --port so the CLI connects to our test server port.
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script_path),
        "--rover-id",
        rover_id,
        "--host",
        host,
        "--port",
        str(port),
        "--interval",
        "0.05",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )

    try:
        # Wait until server registers the rover (give some generous timeout)
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if rover_id in srv.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            # If it didn't register, capture subprocess output for debugging
            stderr = await proc.stderr.read()
            stdout = await proc.stdout.read()
            stderr_text = stderr.decode(errors="replace") if stderr else "<no stderr>"
            stdout_text = stdout.decode(errors="replace") if stdout else "<no stdout>"
            pytest.fail(
                f"CLI subprocess did not register rover {rover_id} within timeout. "
                f"Connected: {srv.get_connected_rovers()}\n"
                f"subprocess stdout:\n{stdout_text}\nsubprocess stderr:\n{stderr_text}"
            )

        # Success: the CLI process connected and the server saw the rover.
        assert rover_id in srv.get_connected_rovers()

    finally:
        # Terminate CLI subprocess cleanly
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        # wait for it to exit, otherwise kill
        try:
            await asyncio.wait_for(proc.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except Exception:
                pass

        # stop server
        await srv.stop()