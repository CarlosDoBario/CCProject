import asyncio
import json
import pytest
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_hooks import register_telemetry_hooks
from rover.telemetry_client import TelemetryClient

@pytest.mark.asyncio
async def test_telemetry_command_roundtrip():
    ms = MissionStore()
    ts = TelemetryStore(history_size=10)
    # register hooks so mission store would see telemetry (not strictly required here)
    register_telemetry_hooks(ms, ts)

    # start server on ephemeral port
    server = TelemetryServer(mission_store=ms, telemetry_store=ts, host="127.0.0.1", port=0)
    await server.start()
    sock = server._server.sockets[0]
    host, port = sock.getsockname()[:2]

    rover_id = "R-CMD-1"
    # event to signal client handled the command
    cmd_received_evt = asyncio.Event()
    cmd_payload_holder = {}

    async def on_command(payload):
        # store payload and set event
        cmd_payload_holder["payload"] = payload
        cmd_received_evt.set()

    # start a persistent client that will send telemetry periodically so server registers it
    client = TelemetryClient(rover_id=rover_id, host=host, port=port, interval_s=0.05, reconnect=True)
    client.on_command(on_command)
    await client.start()

    try:
        # wait until server has registered the rover (it registers upon receiving telemetry)
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            if rover_id in server.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Rover {rover_id} did not connect and register within timeout. Connected: {server.get_connected_rovers()}")

        # send a command and wait for ACK (server.send_command will wait for TS_ACK from client)
        cmd = {"cmd": "do_work", "params": {"value": 42}}
        # expect_ack=True so server waits for client's TS_ACK
        msgid = await server.send_command(rover_id, cmd, expect_ack=True, timeout=5.0)
        assert isinstance(msgid, int)

        # ensure client's on_command callback ran
        await asyncio.wait_for(cmd_received_evt.wait(), timeout=2.0)
        assert "payload" in cmd_payload_holder
        # payload should contain the TLV_PAYLOAD_JSON decoded to canonical dict (server put JSON in TLV)
        assert isinstance(cmd_payload_holder["payload"], dict)
        # the original JSON should be inside payload's PAYLOAD fields or directly present because we decode TLV_PAYLOAD_JSON to canonical
        # Accept the command content presence in payload: check for 'cmd' key
        assert "cmd" in cmd_payload_holder["payload"] or "cmd" in cmd_payload_holder["payload"].get("payload_json", {})
    finally:
        # cleanup
        await client.stop()
        await server.stop()