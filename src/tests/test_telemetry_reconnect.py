import asyncio
import socket
import time
import pytest

from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_hooks import register_telemetry_hooks
from rover.telemetry_client import TelemetryClient


def _get_free_port():
    """Reserve an ephemeral port and return it (close socket so server can bind)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_telemetry_client_reconnect_on_server_restart():
    """
    Integration test: start server A, start client, verify registration;
    stop server A, start server B on same port, verify client reconnects and re-registers.
    """
    host = "127.0.0.1"
    port = _get_free_port()
    rover_id = "R-RECON"

    # create stores for server A
    ms_a = MissionStore()
    ts_a = TelemetryStore(history_size=10)
    register_telemetry_hooks(ms_a, ts_a)

    # Start server A
    srv_a = TelemetryServer(mission_store=ms_a, telemetry_store=ts_a, host=host, port=port)
    await srv_a.start()

    # Start persistent client
    client = TelemetryClient(rover_id=rover_id, host=host, port=port, interval_s=0.05, reconnect=True, backoff_base=0.5, backoff_factor=1.5)
    await client.start()

    try:
        # Wait until server A has the rover connected
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            if rover_id in srv_a.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Client did not register with server A on port {port} within timeout")

        # Now stop server A (simulate server crash/restart)
        await srv_a.stop()

        # Wait briefly to allow client to detect disconnect and attempt reconnect
        await asyncio.sleep(0.2)

        # Start server B on same port
        ms_b = MissionStore()
        ts_b = TelemetryStore(history_size=10)
        register_telemetry_hooks(ms_b, ts_b)
        srv_b = TelemetryServer(mission_store=ms_b, telemetry_store=ts_b, host=host, port=port)
        await srv_b.start()

        # Wait for client to reconnect and register with server B (allow longer timeout)
        deadline = asyncio.get_event_loop().time() + 20.0
        while asyncio.get_event_loop().time() < deadline:
            if rover_id in srv_b.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Client did not reconnect to server B on port {port} within timeout")

        # Optionally, ensure TelemetryStore on server B received telemetry samples
        snapshot = ts_b.snapshot()
        assert rover_id in snapshot, "TelemetryStore on restarted server did not receive telemetry from client after reconnect"

    finally:
        # cleanup
        try:
            await client.stop()
        except Exception:
            pass
        try:
            await srv_a.stop()
        except Exception:
            pass
        try:
            await srv_b.stop()
        except Exception:
            pass