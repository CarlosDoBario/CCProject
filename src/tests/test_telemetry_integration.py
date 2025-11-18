"""
Integration test for TelemetryServer + TelemetryClient + TelemetryStore (binary TLV).

This pytest-asyncio test:
 - creates a MissionStore and TelemetryStore
 - registers telemetry hooks to sync telemetry -> mission store
 - starts TelemetryServer on an ephemeral port
 - creates 3 telemetry senders that each connect and send a single telemetry payload
 - verifies the TelemetryStore has received telemetry for the 3 rover ids and MissionStore sees the rovers

Note: updated to use the new binary telemetry client helper (send_once) and the
synchronous TelemetryStore.snapshot() API.
"""
import asyncio
import pytest
import random

from nave_mae.telemetry_store import TelemetryStore
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_hooks import register_telemetry_hooks
from rover.telemetry_client import send_once as telemetry_send_once
from common import binary_proto


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

    # Create three telemetry send operations that will each send a single message
    rover_ids = ["R-T1", "R-T2", "R-T3"]

    async def send_for_rover(rid: str):
        telemetry = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": random.uniform(-5, 5), "y": random.uniform(-5, 5), "z": 0.0},
            "battery_level_pct": random.randint(30, 100),
            "status": "IN_MISSION",
            "progress_pct": random.uniform(0, 50),
        }
        # send_once opens a TCP connection, sends one framed binary TLV message and closes
        await telemetry_send_once(host, port, rid, telemetry, ack_requested=False, include_crc=False)

    # Run client coroutines concurrently
    tasks = [asyncio.create_task(send_for_rover(rid)) for rid in rover_ids]

    # Wait for clients to finish (with a timeout)
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

    # Allow slight time for server handlers and hooks to process
    await asyncio.sleep(0.2)

    # Verify telemetry store received entries for all rover_ids (TelemetryStore.snapshot is synchronous)
    snapshot = ts.snapshot()
    for rid in rover_ids:
        assert rid in snapshot, f"Telemetry not received for {rid}: snapshot keys={list(snapshot.keys())}"

    # Verify mission store has registered rovers (via register_rover in hook)
    rovers = ms.list_rovers()
    for rid in rover_ids:
        assert rid in rovers, f"MissionStore did not register rover {rid}: rovers={rovers}"

    # Clean up
    await server.stop()