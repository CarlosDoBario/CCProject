from common import binary_proto
from rover.ml_client import SimpleMLClient
import struct

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def build_mission_assign(rover_id: str, mission_id: str, msg_id: int):
    tlvs = []
    tlvs.append((binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8")))
    # include params json to mimic mission payload
    import json
    tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps({"task": "capture_images", "params": {"interval_s": 0.1, "frames": 1}}).encode("utf-8")))
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_MISSION_ASSIGN, rover_id, tlvs, msgid=int(msg_id))
    return pkt


def test_client_dedup_resends_ack_on_duplicate():
    rover_id = "R-CLIENT-DEDUP"
    proto = SimpleMLClient(rover_id=rover_id)
    ft = FakeTransport()
    proto.transport = ft

    mission_id = "M-DEDUP-1"
    msg_id = 0xDEADBEAF0001

    data = build_mission_assign(rover_id, mission_id, msg_id)

    # First arrival: client should send ACK once
    proto.datagram_received(data, ("127.0.0.1", 50000))
    assert len(ft.sent) >= 1, "Expected at least one sendto (ACK) on first assign"

    # Capture the ACK bytes sent for comparison
    first_ack = ft.sent[-1][0]

    # Second arrival (duplicate): client should resend the same ACK
    proto.datagram_received(data, ("127.0.0.1", 50000))
    assert len(ft.sent) >= 2, "Expected a resend ACK on duplicate assign"

    second_ack = ft.sent[-1][0]
    assert first_ack == second_ack, "Expected duplicated ACK bytes to match original ACK"