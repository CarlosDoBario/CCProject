from common import ml_schema
from rover.ml_client import MLClientProtocol

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None

def build_mission_assign(rover_id: str, mission_id: str, msg_id: str):
    body = {"mission_id": mission_id, "task": "capture_images", "params": {"interval_s": 0.1, "frames": 1}}
    env = ml_schema.build_envelope("MISSION_ASSIGN", body=body, rover_id=rover_id, mission_id=mission_id, msg_id=msg_id)
    return ml_schema.envelope_to_bytes(env)

def test_client_dedup_resends_ack_on_duplicate():
    rover_id = "R-CLIENT-DEDUP"
    proto = MLClientProtocol(rover_id=rover_id)
    ft = FakeTransport()
    proto.transport = ft

    mission_id = "M-DEDUP-1"
    msg_id = "assign-msg-1"
    data = build_mission_assign(rover_id, mission_id, msg_id)

    # First arrival: client should send ACK once (and schedule handling)
    proto.datagram_received(data, ("127.0.0.1", 50000))
    assert len(ft.sent) >= 1, "Expected at least one sendto (ACK) on first assign"

    # Capture the ACK bytes sent for comparison
    first_ack = ft.sent[-1][0]

    # Second arrival (duplicate): client should resend the same ACK
    proto.datagram_received(data, ("127.0.0.1", 50000))
    assert len(ft.sent) >= 2, "Expected a resend ACK on duplicate assign"

    second_ack = ft.sent[-1][0]
    assert first_ack == second_ack, "Expected duplicated ACK bytes to match original ACK"