#!/usr/bin/env python3
"""
tests/test_e2e.py

End-to-end integration test (pytest) for the MissionLink implementation.

What it does
- Starts an in-process ML server (MLServerProtocol) bound to localhost on a free UDP port,
  using a MissionStore instance created by the test.
- Populates the MissionStore with demo missions (create_demo_missions()).
- Starts the ml_client.py as a subprocess pointing to the test server with --exit-on-complete.
- Waits (with timeout) for the mission assigned to the client to reach state "COMPLETED"
  in the same MissionStore instance used by the server.
- Cleans up client subprocess and server asyncio loop/thread.

Notes
- The test requires PYTHONPATH to include "src" so that imports like `nave_mae.ml_server`
  and `rover.ml_client` resolve. Pytest runner can be invoked with the env var set:
    PYTHONPATH=src pytest -q
  On Windows PowerShell:
    $env:PYTHONPATH = "src"; pytest -q

- The test uses sys.executable to run the client subprocess to ensure the same Python
  interpreter is used.

- The client subprocess is terminated after mission completion to keep the test deterministic.

- Timeout values are conservative for CI; adjust if your environment is slow.

Run:
  PYTHONPATH=src pytest tests/test_e2e.py -q

"""

import os
import sys
import time
import socket
import subprocess
import threading
import asyncio
from typing import Dict, Any

import pytest

# Import the server classes and mission_store
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore


def _get_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _start_server_in_thread(mission_store: MissionStore, host: str, port: int):
    """
    Start asyncio DatagramProtocol server in a background thread.
    Returns a dict with 'thread' and 'server_info' where server_info contains the loop and protocol.
    """
    ready = threading.Event()
    server_info: Dict[str, Any] = {}

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        proto = MLServerProtocol(mission_store)
        listen = loop.create_datagram_endpoint(lambda: proto, local_addr=(host, port))
        transport, _ = loop.run_until_complete(listen)
        server_info["loop"] = loop
        server_info["transport"] = transport
        server_info["protocol"] = proto
        ready.set()
        try:
            loop.run_forever()
        finally:
            try:
                transport.close()
            except Exception:
                pass
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    thr = threading.Thread(target=_run, daemon=True)
    thr.start()
    # wait for server to signal ready (or timeout)
    if not ready.wait(timeout=5.0):
        raise RuntimeError("Server thread did not become ready in time")
    return {"thread": thr, "server_info": server_info}


@pytest.mark.timeout(60)
def test_e2e_request_assign_progress_complete(tmp_path):
    """
    End-to-end test:
      - Launch server with a MissionStore
      - Populate demo missions
      - Launch a client process that requests a mission (with --exit-on-complete)
      - Verify the mission assigned to that client reaches COMPLETED in the MissionStore
    """
    host = "127.0.0.1"
    port = _get_free_port()

    # Create a MissionStore instance and start server bound to it
    ms = MissionStore()
    server = _start_server_in_thread(ms, host, port)
    try:
        # populate demo missions so the server can assign one immediately
        ms.create_demo_missions()

        # Prepare environment for client subprocess: ensure PYTHONPATH points to src
        env = os.environ.copy()
        env["PYTHONPATH"] = env.get("PYTHONPATH", "")
        # Prepend project src to PYTHONPATH so imports resolve
        if "src" not in env["PYTHONPATH"].split(os.pathsep):
            env["PYTHONPATH"] = os.pathsep.join(filter(None, ["src", env["PYTHONPATH"]]))

        client_proc = subprocess.Popen(
            [sys.executable, "-u", os.path.join("src", "rover", "ml_client.py"),
             "--rover-id", "R-E2E",
             "--server", host,
             "--port", str(port),
             "--exit-on-complete"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
        )

        # Optional: if E2E_VERBOSE env var is set, stream client stdout/stderr live to the test runner.
        # This is opt-in so CI logs aren't noisy. Set E2E_VERBOSE=1 before running pytest to see live logs.
        if os.environ.get("E2E_VERBOSE"):
            def _forward_stream(src, dst):
                try:
                    for line in iter(src.readline, ""):
                        if not line:
                            break
                        dst.write(line)
                        dst.flush()
                finally:
                    try:
                        src.close()
                    except Exception:
                        pass

            t_out = threading.Thread(target=_forward_stream, args=(client_proc.stdout, sys.stdout), daemon=True)
            t_err = threading.Thread(target=_forward_stream, args=(client_proc.stderr, sys.stderr), daemon=True)
            t_out.start()
            t_err.start()

        # Wait for mission completion in the mission store (polled)
        timeout_s = 30
        deadline = time.time() + timeout_s
        completed = False
        found_mid = None
        while time.time() < deadline:
            missions = ms.list_missions()
            # Look for a mission assigned to R-E2E with state COMPLETED
            for mid, m in missions.items():
                if m.get("assigned_rover") == "R-E2E" and m.get("state") == "COMPLETED":
                    completed = True
                    found_mid = mid
                    break
            if completed:
                break
            time.sleep(0.5)

        # Collect client output for debugging if needed
        try:
            stdout, stderr = client_proc.communicate(timeout=1.0)
        except subprocess.TimeoutExpired:
            # client may still be running; terminate it
            client_proc.terminate()
            try:
                stdout, stderr = client_proc.communicate(timeout=2.0)
            except Exception:
                stdout, stderr = "", ""

        # Assert mission completed
        assert completed, f"No mission completed for R-E2E within {timeout_s}s. Client stdout:\n{stdout}\nClient stderr:\n{stderr}"

    finally:
        # Shutdown client if still running
        try:
            if client_proc and client_proc.poll() is None:
                client_proc.terminate()
                client_proc.wait(timeout=2.0)
        except Exception:
            pass
        # Stop server loop
        try:
            loop = server["server_info"]["loop"]
            loop.call_soon_threadsafe(loop.stop)
            server["thread"].join(timeout=2.0)
        except Exception:
            pass