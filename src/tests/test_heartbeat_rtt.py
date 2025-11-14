from common import ml_schema
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import time
from datetime import datetime, timezone, timedelta

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None

def iso_now_minus(seconds: float) -> str:
    dt = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return dt.isoformat(timespec="seconds")

def build_heartbeat_with_past_timestamp(rover_id: str, msg_id: str, seconds_in_past: float):
    # build envelope and then override header timestamp to a past value
    env = ml_schema.build_envelope("HEARTBEAT", body={}, rover_id=rover_id, msg_id=msg_id)
    env["header"]["timestamp"] = iso_now_minus(seconds_in_past)
    # serialize after modifying header so bytes reflect the timestamp we set
    b = ml_schema.envelope_to_bytes(env)
    return b, env

def test_heartbeat_rtt_can_be_measured_via_header_timestamp():
    ms = MissionStore()
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    proto.transport = ft

    rover_id = "R-HB-RTT"
    msg_id = "hb-rtt-1"
    # simulate heartbeat sent 0.5s in the past
    past_seconds = 0.5
    hb_bytes, hb_env = build_heartbeat_with_past_timestamp(rover_id, msg_id, past_seconds)

    # Record "client receive time" before handing to server to bound measurement
    send_time = time.time()
    proto.datagram_received(hb_bytes, ("127.0.0.1", 50000))
    recv_time = time.time()

    # server should have sent an ACK
    assert len(ft.sent) >= 1, "Expected server to send ACK for heartbeat"

    # compute RTT estimate as time now minus the heartbeat header timestamp we injected
    ts_iso = hb_env["header"]["timestamp"]
    # convert ts_iso to epoch seconds
    from common.utils import iso_to_epoch_ms
    ts_ms = iso_to_epoch_ms(ts_iso)
    ts_s = ts_ms / 1000.0

    # best-effort RTT: use recv_time - ts_s (recv_time is when server processed and responded)
    rtt = recv_time - ts_s

    # rtt should be non-negative and approximately >= past_seconds (allow some slack)
    assert rtt >= 0.0, f"RTT should be non-negative, got {rtt}"
    assert rtt >= past_seconds - 0.1, f"Measured RTT ({rtt:.3f}s) should be close to the injected past_seconds ({past_seconds}s)"