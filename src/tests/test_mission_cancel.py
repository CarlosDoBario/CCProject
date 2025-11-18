#!/usr/bin/env python3
"""
tests/test_mission_cancel.py

Unit test that verifies server->client MISSION_CANCEL send path and handling of ACK:
- create mission, assign to rover, register rover address
- call server.send_mission_cancel(mission_id)
- ensure a pending_outgoing entry exists of type MISSION_CANCEL
- simulate ACK arriving and assert mission_store marks mission CANCELLED
"""
import time
import struct

from nave_mae.mission_store import MissionStore
from nave_mae.ml_server import MLServerProtocol
from common import binary_proto, config


def test_send_mission_cancel_and_ack_flow():
    ms = MissionStore()
    # create mission and assign
    mid = ms.create_mission({"task": "collect_samples", "params": {"sample_count": 1, "depth_mm": 10}})
    # register rover info and assign
    ms.register_rover("R-CANCEL", {"ip": "127.0.0.1", "port": 51000} if hasattr(ms, "register_rover") else ("127.0.0.1", 51000))
    ms.assign_mission_to_rover(ms.get_mission(mid), "R-CANCEL")

    server = MLServerProtocol(ms)

    # Make sure no pending cancels exist yet
    assert all(po.message_type != "MISSION_CANCEL" for po in server.pending_outgoing.values())

    # Send cancel; returns the server msgid (numeric)
    msg_id = server.send_mission_cancel(mid, reason="test_cancel")

    # There should be a pending_outgoing entry keyed by msg_id (or one with message_type MISSION_CANCEL)
    found = None
    for k, po in server.pending_outgoing.items():
        if po.message_type == "MISSION_CANCEL" and po.mission_id == mid:
            found = po
            break
    assert found is not None, "Expected a pending MISSION_CANCEL in pending_outgoing"

    # Simulate receiving ACK from the rover: create an ML_ACK datagram acknowledging the msg_id
    ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(msg_id)))]
    ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, "R-CANCEL", ack_tlvs, msgid=0)
    # deliver to server as if from the rover address
    server.datagram_received(ack_pkt, ("127.0.0.1", 51000))

    # After ACK, mission should be cancelled in mission_store
    m = ms.get_mission(mid)
    assert m is not None
    # Expect state CANCELLED or at least assigned_rover cleared and a CANCEL entry in history
    state = m.get("state")
    history = m.get("history", [])
    types = [h.get("type") for h in history]
    assert state == "CANCELLED" or any(t == "CANCEL" for t in types), f"Mission not cancelled; state={state} history={history}"