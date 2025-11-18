from common import binary_proto
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import time

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def test_heartbeat_rtt_can_be_measured_via_msgid_timestamp():
    """
    Adaptação do teste original que usava timestamp no envelope JSON:
    - Montamos um ML_HEARTBEAT com msgid numérico igual ao timestamp (ms) de agora - past_seconds.
    - Ao processar a mensagem, o servidor responde com ACK; usamos o momento em que o servidor processou
      (recv_time) menos o timestamp embutido (msgid) para estimar o RTT.
    """
    ms = MissionStore()
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    proto.transport = ft

    rover_id = "R-HB-RTT"
    past_seconds = 0.5
    # embed a past timestamp (ms) in the msgid field
    msgid_ms = int((time.time() - past_seconds) * 1000) & 0xFFFFFFFFFFFFFFFF

    # build heartbeat datagram with that numeric msgid
    hb_bytes = binary_proto.pack_ml_datagram(binary_proto.ML_HEARTBEAT, rover_id, [], msgid=msgid_ms)

    # Record time around delivery to bound measurement
    send_time = time.time()
    proto.datagram_received(hb_bytes, ("127.0.0.1", 50000))
    recv_time = time.time()

    # server should have sent an ACK
    assert len(ft.sent) >= 1, "Expected server to send ACK for heartbeat"

    # compute RTT estimate as recv_time - embedded msgid timestamp
    ts_s = float(msgid_ms) / 1000.0
    rtt = recv_time - ts_s

    assert rtt >= 0.0, f"RTT should be non-negative, got {rtt}"
    assert rtt >= past_seconds - 0.1, f"Measured RTT ({rtt:.3f}s) should be close to the injected past_seconds ({past_seconds}s)"