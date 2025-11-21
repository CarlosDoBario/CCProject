#!/usr/bin/env python3
"""
tests/test_send_command_keyerror.py

Unit test that verifies TelemetryServer.send_command raises KeyError
when attempting to send a command to a rover that is not currently connected.
"""
import pytest
import asyncio

from nave_mae.telemetry_server import TelemetryServer


@pytest.mark.asyncio
async def test_send_command_raises_keyerror_when_rover_not_connected():
    srv = TelemetryServer(host="127.0.0.1", port=0, mission_store=None, telemetry_store=None)

    # Ensure no clients are registered
    assert "NONEXISTENT" not in srv.get_connected_rovers()

    # send_command should raise KeyError for unknown rover regardless of expect_ack flag
    with pytest.raises(KeyError):
        await srv.send_command("NONEXISTENT", {"cmd": "noop"}, expect_ack=False)

    with pytest.raises(KeyError):
        await srv.send_command("NONEXISTENT", {"cmd": "noop"}, expect_ack=True, timeout=0.1)