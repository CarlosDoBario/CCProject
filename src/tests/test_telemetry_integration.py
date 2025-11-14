"""
Integration test for TelemetryServer + TelemetryClient + TelemetryStore.

This pytest-asyncio test:
 - creates a MissionStore and TelemetryStore
 - registers telemetry hooks to sync telemetry -> mission store
 - starts TelemetryServer on an ephemeral port
 - creates 3 TelemetryClient instances that each connect and send a single telemetry payload
 - verifies the TelemetryStore has received telemetry for the 3 rover ids and MissionStore sees the rovers

Place at: src/tests/test_telemetry_integration.py
"""
import asyncio
import pytest
import time

from nave_mae.telemetry_store import TelemetryStore
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_hooks import register_telemetry_hooks
from rover.telemetry_client import TelemetryClient


@pytest.mark.asyncio
async def test_telemetry_server_receives_multiple_clients():
    # Create mission store and telemetry store and register hooks
    ms = MissionStore()
    ts = TelemetryStore(history_size=10)
    register_telemetry_hooks(ms, ts)

    # Start telemetry server on an ephemeral port (port=0)
    server = TelemetryServer(mission_store=ms, telemetry_store=ts, host="127.0.0.1", port=0)
    await server.start()
    # Determine bound port
    sock = server._server.sockets[0]
    host, port = sock.getsockname()[:2]

    # Create three telemetry clients that will send a single message each and exit
    rover_ids = ["R-T1", "R-T2", "R-T3"]
    clients = [
        TelemetryClient(rover_id=rid, host=host, port=port, interval_s=0.01, hello_first=True, use_sim=False)
        for rid in rover_ids
    ]

    # Run client run_once coroutines concurrently
    tasks = [asyncio.create_task(c.run_once()) for c in clients]

    # Wait for clients to finish (with a timeout)
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

    # Allow slight time for server handlers and hooks to process
    await asyncio.sleep(0.2)

    # Verify telemetry store received entries for all rover_ids
    snapshot = await ts.snapshot()
    for rid in rover_ids:
        assert rid in snapshot, f"Telemetry not received for {rid}: snapshot keys={list(snapshot.keys())}"

    # Verify mission store has registered rovers (via register_rover in hook)
    rovers = ms.list_rovers()
    for rid in rover_ids:
        assert rid in rovers, f"MissionStore did not register rover {rid}: rovers={rovers}"

    # Clean up
    await server.stop()