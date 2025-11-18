# tests/test_stress_packet_loss.py
# Replacement stress test that simulates packet loss via an in-memory lossy transport.
# This version is adapted to the binary TLV protocol and the updated SimpleMLClient.
# It verifies mission completion by checking the mission state OR the mission history
# for a COMPLETE event from the rover.
import asyncio
import random
import time
import pytest

from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
from rover.ml_client import SimpleMLClient
from common import config


class LossyTransport:
    """
    Fake transport that forwards packets to a destination protocol.datagram_received(...)
    with a configurable drop rate and jitter. Deterministic if random.seed is set.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, dest_proto, drop_rate: float = 0.25, max_delay_s: float = 0.02):
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
        # minimal compat for protocols that query sockname
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

    # more aggressive retransmit allowances for lossy in-memory transport
    config.TIMEOUT_TX_INITIAL = 0.05  # smaller base timeout to allow many attempts
    config.N_RETX = 20                # allow many retries to survive loss
    config.BACKOFF_FACTOR = 1.5
    config.DEFAULT_UPDATE_INTERVAL_S = 0.05  # rover progress interval small to finish mission quickly

    loop = asyncio.get_event_loop()

    # keep references to created background tasks so we can cancel them in cleanup
    client_retx_task = None

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

        # Create client protocol (SimpleMLClient). Provide server address in constructor.
        client = SimpleMLClient(rover_id="R-STRESS", server=("127.0.0.1", config.ML_UDP_PORT), exit_on_complete=True)

        # Create lossy transports linking client <-> server
        # client_transport.sendto -> deliver to server.datagram_received (higher loss)
        client_transport = LossyTransport(loop, server, drop_rate=0.30, max_delay_s=0.03)
        # server_transport.sendto -> deliver to client.datagram_received (lower loss so ASSIGNs eventually arrive)
        server_transport = LossyTransport(loop, client, drop_rate=0.10, max_delay_s=0.02)

        # Call connection_made to start internal retransmit/cleanup tasks on server/client
        server.connection_made(server_transport)
        client.connection_made(client_transport)

        # Start client's retransmit loop (SimpleMLClient.retransmit_loop must be run manually in this in-memory setup)
        client_retx_task = asyncio.create_task(client.retransmit_loop())

        # Start the client's mission request (async)
        await client.request_mission()

        # Wait for mission completion by polling MissionStore or timeout
        try:
            # Wait until either the mission appears completed or timeout
            deadline = time.time() + 40.0  # larger overall timeout to give retransmits time
            completed = False
            while time.time() < deadline:
                missions = ms.list_missions()
                for mid, m in missions.items():
                    if m.get("state") == "COMPLETED" and m.get("assigned_rover") == "R-STRESS":
                        completed = True
                        break
                    # fallback: history search for COMPLETE event
                    hist = m.get("history", [])
                    for e in hist:
                        if e.get("type") in ("COMPLETE",) and e.get("rover") == "R-STRESS":
                            completed = True
                            break
                    if completed:
                        break
                if completed:
                    break
                await asyncio.sleep(0.05)
        except asyncio.TimeoutError:
            missions = ms.list_missions()
            raise AssertionError(f"Stress test timed out waiting for client to finish. Missions snapshot: {missions}")

        assert completed, f"Expected mission to be completed by R-STRESS despite packet loss; snapshot: {ms.list_missions()}"

    finally:
        # restore config values
        config.TIMEOUT_TX_INITIAL = orig_timeout
        config.N_RETX = orig_nretx
        config.BACKOFF_FACTOR = orig_backoff
        config.DEFAULT_UPDATE_INTERVAL_S = orig_update_interval

        # cancel background tasks created by protocols (if any) to avoid warnings
        try:
            if hasattr(server, "_task_retransmit") and server._task_retransmit:
                server._task_retransmit.cancel()
            if hasattr(server, "_task_cleanup") and server._task_cleanup:
                server._task_cleanup.cancel()
        except Exception:
            pass
        try:
            if client_retx_task:
                client_retx_task.cancel()
        except Exception:
            pass