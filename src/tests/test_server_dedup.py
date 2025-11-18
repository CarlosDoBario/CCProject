from common import binary_proto
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import struct
import time

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def build_request_mission(rover_id: str, msg_id: int):
    """
    Build a binary ML REQUEST_MISSION datagram using binary_proto.
    msg_id is numeric (uint64-ish).
    """
    # body TLVs: send capabilities as a simple TLV so server will accept and process
    caps = b"sampling"
    tlvs = [(binary_proto.TLV_CAPABILITIES, caps)]
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_REQUEST_MISSION, rover_id, tlvs, msgid=int(msg_id))
    return pkt


def test_server_dedup_prevents_double_processing(tmp_path):
    # prepare mission store with one demo mission (fallback to create_mission if demo helper not present)
    ms = MissionStore()
    if hasattr(ms, "create_demo_missions") and callable(ms.create_demo_missions):
        ms.create_demo_missions()
    else:
        # create a simple pending mission so server has something to assign
        mission_spec = {
            "task": "capture_images",
            "params": {"frames": 1, "interval_s": 0.01},
            "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1},
            "priority": 1,
        }
        ms.create_mission(mission_spec)

    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    # attach fake transport so server._send_with_reliability can call sendto
    proto.transport = ft

    rover_id = "R-DEDUP"
    # pick a deterministic numeric msgid
    msg_id = 0xDEADBEEFCAFEBABE & 0xFFFFFFFFFFFFFFFF

    data = build_request_mission(rover_id, msg_id)

    # First arrival: should be processed -> assignment attempted
    proto.datagram_received(data, ("127.0.0.1", 50000))

    # Capture assignments count for this rover after first arrival
    assigned_before = [m for m in ms.list_missions().values() if m.get("assigned_rover") == rover_id]

    # Second arrival (duplicate same msg_id): should be skipped by dedupe
    proto.datagram_received(data, ("127.0.0.1", 50000))

    # Check: missions assigned to rover should not increase due to duplicate
    assigned_after = [m for m in ms.list_missions().values() if m.get("assigned_rover") == rover_id]

    assert len(assigned_after) <= max(1, len(assigned_before)), f"Duplicate processing increased assignments: before={len(assigned_before)} after={len(assigned_after)}"