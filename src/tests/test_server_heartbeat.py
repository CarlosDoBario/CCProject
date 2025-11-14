from common import ml_schema
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import time

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        # record packet bytes and destination addr
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None

def build_heartbeat(rover_id: str, msg_id: str):
    # empty body heartbeat is fine
    env = ml_schema.build_envelope("HEARTBEAT", body={}, rover_id=rover_id, msg_id=msg_id)
    return ml_schema.envelope_to_bytes(env), env

def test_server_responds_with_ack_for_heartbeat():
    ms = MissionStore()
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    proto.transport = ft

    rover_id = "R-HB"
    msg_id = "hb-msg-1"
    hb_bytes, hb_env = build_heartbeat(rover_id, msg_id)

    # server receives heartbeat
    proto.datagram_received(hb_bytes, ("127.0.0.1", 50000))

    # transport should have been used to send an ACK
    assert len(ft.sent) >= 1, "Expected server to send at least one packet (ACK) in response to HEARTBEAT"

    # parse the last sent packet to confirm it's an ACK and acked_msg_id matches
    last_packet, last_addr = ft.sent[-1]
    ack_env = ml_schema.parse_envelope(last_packet)
    assert ack_env["header"]["message_type"] == "ACK", f"Expected ACK, got {ack_env['header']['message_type']}"
    assert ack_env["body"].get("acked_msg_id") == msg_id, "ACK should acknowledge the heartbeat msg_id"

    # server should also have recorded the last_acks entry keyed by original msg_id
    stored = proto.last_acks.get(msg_id)
    assert stored is not None, "Server.last_acks should contain the ack packet keyed by the original msg_id"
    assert stored == last_packet, "Stored last_acks packet should match the sent ACK bytes"