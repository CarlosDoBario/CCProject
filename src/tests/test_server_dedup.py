from common import ml_schema
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None

def build_request_mission(rover_id: str, msg_id: str):
    body = {"capabilities": ["sampling"]}
    env = ml_schema.build_envelope("REQUEST_MISSION", body=body, rover_id=rover_id, msg_id=msg_id)
    return ml_schema.envelope_to_bytes(env)

def test_server_dedup_prevents_double_processing(tmp_path):
    # prepare mission store with one demo mission
    ms = MissionStore()
    ms.create_demo_missions()
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    proto.transport = ft

    rover_id = "R-DEDUP"
    msg_id = "dedup-test-1"

    data = build_request_mission(rover_id, msg_id)

    # First arrival: should be processed -> assignment attempted
    proto.datagram_received(data, ("127.0.0.1", 50000))

    # Capture current assignments count for this rover
    assigned_before = [m for m in ms.list_missions().values() if m.get("assigned_rover") == rover_id]

    # Second arrival (duplicate same msg_id): should be skipped by dedupe
    proto.datagram_received(data, ("127.0.0.1", 50000))

    # Check: missions assigned to rover should not increase due to duplicate
    assigned_after = [m for m in ms.list_missions().values() if m.get("assigned_rover") == rover_id]

    assert len(assigned_after) <= max(1, len(assigned_before)), f"Duplicate processing increased assignments: before={len(assigned_before)} after={len(assigned_after)}"