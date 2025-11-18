from common import binary_proto
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import time
import struct


class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        # record packet bytes and destination addr
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def build_heartbeat(rover_id: str, msg_id: int):
    """
    Build a binary ML HEARTBEAT datagram using binary_proto.
    msg_id should be a numeric identifier (uint64-ish).
    """
    # empty TLV list is fine for heartbeat
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_HEARTBEAT, rover_id, [], msgid=int(msg_id))
    return pkt


def test_server_responds_with_ack_for_heartbeat():
    ms = MissionStore()
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    # attach fake transport to protocol (connection_made normally sets this)
    proto.transport = ft

    rover_id = "R-HB"
    msg_id = int(time.time() * 1000) & 0xFFFFFFFFFFFFFFFF
    hb_bytes = build_heartbeat(rover_id, msg_id)

    # server receives heartbeat
    proto.datagram_received(hb_bytes, ("127.0.0.1", 50000))

    # transport should have been used to send an ACK
    assert len(ft.sent) >= 1, "Expected server to send at least one packet (ACK) in response to HEARTBEAT"

    # parse the last sent packet to confirm it's an ACK and acked_msg_id matches
    last_packet, last_addr = ft.sent[-1]
    parsed = binary_proto.parse_ml_datagram(last_packet)
    header = parsed.get("header", {})
    tlvs = parsed.get("tlvs", {})

    # Header msgtype should be ML_ACK
    assert header.get("msgtype") == binary_proto.ML_ACK, f"Expected ML_ACK message type, got {header.get('msgtype')}"

    # TLV_ACKED_MSG_ID should be present and match the original msg_id
    acked_bytes = tlvs.get(binary_proto.TLV_ACKED_MSG_ID, [])
    assert acked_bytes, "ACK packet missing TLV_ACKED_MSG_ID"
    try:
        acked_id = struct.unpack(">Q", acked_bytes[0])[0]
    except Exception:
        raise AssertionError("Failed to unpack TLV_ACKED_MSG_ID as >Q")

    assert int(acked_id) == int(msg_id), f"ACK should acknowledge msg_id {msg_id}, got {acked_id}"

    # server should also have recorded the last_acks entry keyed by original msg_id (numeric)
    stored = proto.last_acks.get(int(msg_id))
    assert stored is not None, "Server.last_acks should contain the ack packet keyed by the original msg_id"
    assert stored == last_packet, "Stored last_acks packet should match the sent ACK bytes"