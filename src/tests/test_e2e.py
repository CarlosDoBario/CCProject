#!/usr/bin/env python3
"""
tests/test_e2e.py

End-to-end integration test for the binary ML implementation.

This variant runs the client in-process (background thread + event loop)
instead of as a subprocess to make the test more deterministic and robust.
"""
import os
import sys
import time
import socket
import threading
import asyncio
from typing import Dict, Any, Tuple

import pytest

from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
from rover.ml_client import SimpleMLClient


def _get_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _start_server_in_thread(mission_store: MissionStore, host: str, port: int):
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
    if not ready.wait(timeout=5.0):
        raise RuntimeError("Server thread did not become ready in time")
    return {"thread": thr, "server_info": server_info}


def _start_client_in_thread(rover_id: str, server_addr: Tuple[str, int]):
    """
    Start SimpleMLClient in a background thread with its own event loop.
    Returns a dict with thread, loop and protocol objects so caller can stop it.
    """
    ready = threading.Event()
    info: Dict[str, Any] = {}

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = SimpleMLClient(rover_id=rover_id, server=server_addr, exit_on_complete=True)
        info["loop"] = loop
        info["client"] = client
        # create UDP endpoint for the client on an ephemeral local port
        coro = loop.create_datagram_endpoint(lambda: client, local_addr=("127.0.0.1", 0))
        transport, _ = loop.run_until_complete(coro)
        info["transport"] = transport

        # IMPORTANT: schedule client.request_mission so the client actually initiates REQUEST_MISSION
        try:
            loop.create_task(client.request_mission())
            # also schedule retransmit loop so pending retransmits are handled in-client
            loop.create_task(client.retransmit_loop())
        except Exception:
            # if anything fails here, log but continue -- tests will detect lack of completion
            import logging
            logging.getLogger("test_e2e").exception("Failed to schedule client tasks")

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
    if not ready.wait(timeout=5.0):
        raise RuntimeError("Client thread did not become ready in time")
    return {"thread": thr, "info": info}


@pytest.mark.timeout(90)
def test_e2e_request_assign_progress_complete(tmp_path):
    host = "127.0.0.1"
    port = _get_free_port()

    # Create a MissionStore instance and start server bound to it
    ms = MissionStore()
    server = _start_server_in_thread(ms, host, port)
    client = None
    try:
        # populate demo missions so the server can assign one immediately
        if hasattr(ms, "create_demo_missions") and callable(ms.create_demo_missions):
            ms.create_demo_missions()
        else:
            ms.create_mission({
                "task": "capture_images",
                "params": {"interval_s": 0.01, "frames": 2},
                "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1},
                "priority": 1,
            })

        # Start the client in its own thread/event loop; this now schedules request_mission()
        client = _start_client_in_thread("R-E2E", (host, port))

        # Wait for mission completion in the mission store (polled)
        timeout_s = 40
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
            time.sleep(0.25)

        assert completed, f"No mission completed for R-E2E within {timeout_s}s."

    finally:
        # Stop client if still running
        try:
            if client and "info" in client and "loop" in client["info"]:
                loop = client["info"]["loop"]
                loop.call_soon_threadsafe(loop.stop)
                client["thread"].join(timeout=2.0)
        except Exception:
            pass

        # Stop server loop
        try:
            loop = server["server_info"]["loop"]
            loop.call_soon_threadsafe(loop.stop)
            server["thread"].join(timeout=2.0)
        except Exception:
            pass