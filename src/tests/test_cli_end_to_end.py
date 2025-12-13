#!/usr/bin/env python3
"""
End-to-end test for the telemetry client CLI.
This test starts an in-process TelemetryServer and launches the CLI script 
src/rover/telemetry_client.py as a subprocess.
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

    # 1. Iniciar Stores
    ms = MissionStore()
    # CORREÇÃO ANTERIOR MANTIDA: TelemetryStore não aceita history_size
    ts = TelemetryStore(mission_store=ms) 

    # 2. Iniciar Telemetry Server
    srv = TelemetryServer(mission_store=ms, telemetry_store=ts, host=host, port=port)
    await srv.start()

    # 3. Localizar o Script CLI
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "src" / "rover" / "telemetry_client.py"
    assert script_path.exists(), f"telemetry_client.py not found at {script_path}"

    # Prepare environment for subprocess so that 'src' is on PYTHONPATH
    env = os.environ.copy()
    
    # CORREÇÃO FINAL: Usa os.pathsep (';' no Windows)
    new_path = str(repo_root / "src") 
    existing_path = env.get("PYTHONPATH", "")
    
    if existing_path:
        env["PYTHONPATH"] = new_path + os.pathsep + existing_path
    else:
        env["PYTHONPATH"] = new_path


    # 4. Iniciar Subprocesso CLI
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
        # 5. Esperar pelo Registo do Rover no Servidor (Max 10.0s)
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if rover_id in srv.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            # Se falhar, captura o output do subprocesso para debug
            stderr_bytes = await asyncio.wait_for(proc.stderr.read(), timeout=5.0) 
            stdout_bytes = await asyncio.wait_for(proc.stdout.read(), timeout=5.0)
            
            stderr_text = stderr_bytes.decode(errors="replace") if stderr_bytes else "<no stderr>"
            stdout_text = stdout_bytes.decode(errors="replace") if stdout_bytes else "<no stdout>"

            pytest.fail(
                f"CLI subprocess did not register rover {rover_id} within timeout. "
                f"Connected: {srv.get_connected_rovers()}\n"
                f"subprocess stdout:\n{stdout_text}\nsubprocess stderr:\n{stderr_text}"
            )

        # 6. Sucesso
        assert rover_id in srv.get_connected_rovers()

    finally:
        # 7. Limpeza (Kill Process e Stop Server)
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        # espera que termine
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
