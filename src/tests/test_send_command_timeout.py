#!/usr/bin/env python3
"""
tests/test_send_command_timeout.py

Verify that TelemetryServer.send_command(..., expect_ack=True) raises a TimeoutError
when the connected rover does not send an application-level TS_ACK, and that the
server cleans up pending ACK futures.
"""
import asyncio
import time
import pytest

from common import binary_proto
from nave_mae.telemetry_server import TelemetryServer


@pytest.mark.asyncio
async def test_send_command_times_out_and_cleans_pending():
    # Start server on ephemeral port
    srv = TelemetryServer(host="127.0.0.1", port=0, mission_store=None, telemetry_store=None)
    await srv.start()
    sock = srv._server.sockets[0]
    host, port = sock.getsockname()[:2]

    # Open a raw asyncio connection that will send one telemetry frame and then do nothing (no ACK)
    reader, writer = await asyncio.open_connection(host, port)

    try:
        # Send a telemetry frame so the server registers the rover and we obtain a writer mapping.
        rover_id = "R-TIMEOUT"
        tlvs = [(binary_proto.TLV_PAYLOAD_JSON, b'{"hello": true}')]
        frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, msgid=0)
        writer.write(frame)
        await writer.drain()

        # Wait until server registers the rover
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if rover_id in srv.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Rover did not register with server in time")

        # Attempt to send a command and expect a timeout because the client will not ACK
        with pytest.raises(asyncio.TimeoutError):
            await srv.send_command(rover_id, {"cmd": "noack"}, expect_ack=True, timeout=0.2)

        # After timeout, pending ack map should not retain the previous future (no leaks)
        assert len(srv._pending_acks) == 0, f"Expected pending_acks to be empty after timeout, found: {srv._pending_acks}"

    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        await srv.stop()