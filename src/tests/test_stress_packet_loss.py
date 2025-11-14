"""
tests/test_stress_packet_loss.py

Stress/integration test: simulate packet loss between ML client and server using
in-memory transports that randomly drop packets (deterministic seed).

- Starts a MLServerProtocol and MLClientProtocol connected via FakeTransports.
- Creates a small mission on the MissionStore (short duration).
- Simulates packet loss (drop_rate) and small network jitter.
- Asserts that the client still completes the assigned mission within timeout,
  demonstrating the retransmit/ACK/persistence behavior under loss.
"""
import asyncio
import random
import time
import pytest

from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
from rover.ml_client import MLClientProtocol
from common import config, ml_schema


class LossyTransport:
    """
    Fake transport that forwards packets to a destination protocol.datagram_received(...)
    with a configurable drop rate and jitter. Deterministic if random.seed is set.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, dest_proto, drop_rate: float = 0.3, max_delay_s: float = 0.02):
        self.loop = loop
        self.dest = dest_proto
        self.drop_rate = drop_rate
        self.max_delay = max_delay_s
        self.sent = []

    def sendto(self, packet: bytes, addr):
        # record attempt
        self.sent.append((packet, addr))
        # decide drop
        if random.random() < self.drop_rate:
            # drop packet
            return
        # schedule delivery with small jitter
        delay = random.uniform(0, self.max_delay)
        # dest.datagram_received expects (data, addr)
        self.loop.call_later(delay, self._deliver, packet, addr)

    def _deliver(self, packet: bytes, addr):
        try:
            self.dest.datagram_received(packet, addr)
        except Exception:
            # swallow exceptions to avoid failing scheduled call abruptly
            pass

    def get_extra_info(self, name, default=None):
        return None


@pytest.mark.asyncio
async def test_stress_packet_loss_retransmit_and_recovery(tmp_path):
    """
    Stress test with packet loss. Creates one short mission and runs a client that requests it.
    The fake network drops packets at configured rate; test asserts mission completes.
    """
    # Make deterministic
    random.seed(12345)

    # Tune config for faster test (small timeouts, higher retries)
    # Keep original values to restore after test
    orig_timeout = config.TIMEOUT_TX_INITIAL
    orig_nretx = config.N_RETX
    orig_backoff = config.BACKOFF_FACTOR
    orig_update_interval = config.DEFAULT_UPDATE_INTERVAL_S

    config.TIMEOUT_TX_INITIAL = 0.08  # small base timeout
    config.N_RETX = 6                 # allow several retries to survive loss
    config.BACKOFF_FACTOR = 1.7
    config.DEFAULT_UPDATE_INTERVAL_S = 0.05  # rover progress interval small to finish mission quickly

    loop = asyncio.get_event_loop()

    try:
        # Mission store with one small mission (short duration)
        ms = MissionStore()
        mission_spec = {
            "task": "capture_images",
            "params": {"interval_s": 0.01, "frames": 3},  # short mission
            "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1},
            "priority": 1,
        }
        ms.create_mission(mission_spec)

        # Create server protocol and attach a dummy transport (will be replaced)
        server = MLServerProtocol(ms)

        # Create client protocol (exit_on_complete True so it signals when mission done)
        done_event = asyncio.Event()
        client = MLClientProtocol(rover_id="R-STRESS", exit_on_complete=True, done_event=done_event)

        # Create lossy transports linking client <-> server
        # client_transport.sendto -> deliver to server.datagram_received
        client_transport = LossyTransport(loop, server, drop_rate=0.35, max_delay_s=0.03)
        # server_transport.sendto -> deliver to client.datagram_received
        server_transport = LossyTransport(loop, client, drop_rate=0.35, max_delay_s=0.03)

        # Call connection_made to start internal retransmit/cleanup tasks
        server.connection_made(server_transport)
        client.connection_made(client_transport)

        # Start the client's mission request (async)
        await client.request_mission(("127.0.0.1", config.ML_UDP_PORT))

        # Wait for mission completion signalled by client (or timeout)
        try:
            await asyncio.wait_for(done_event.wait(), timeout=25.0)
        except asyncio.TimeoutError:
            # collect some diagnostics
            missions = ms.list_missions()
            # raise with helpful info
            raise AssertionError(
                f"Stress test timed out waiting for client to finish. Missions snapshot: {missions}"
            )

        # Give server a short moment to process the final MISSION_COMPLETE (race avoidance).
        # Poll mission store for up to 2s for the server to mark the mission COMPLETED.
        completed = False
        deadline = time.time() + 2.0
        while time.time() < deadline:
            for mid, m in ms.list_missions().items():
                if m.get("assigned_rover") == "R-STRESS" and m.get("state") == "COMPLETED":
                    completed = True
                    break
            if completed:
                break
            await asyncio.sleep(0.05)

        assert completed, "Expected mission to be completed by R-STRESS despite packet loss"

    finally:
        # restore config values
        config.TIMEOUT_TX_INITIAL = orig_timeout
        config.N_RETX = orig_nretx
        config.BACKOFF_FACTOR = orig_backoff
        config.DEFAULT_UPDATE_INTERVAL_S = orig_update_interval

        # cancel background tasks created by protocols (if any) to avoid warnings
        try:
            if server._task_retransmit:
                server._task_retransmit.cancel()
            if server._task_cleanup:
                server._task_cleanup.cancel()
        except Exception:
            pass
        try:
            if client._task_retransmit:
                client._task_retransmit.cancel()
            if client._task_cleanup:
                client._task_cleanup.cancel()
        except Exception:
            pass