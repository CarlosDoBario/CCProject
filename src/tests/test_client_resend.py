#!/usr/bin/env python3
"""
tests/test_client_resend.py

Adaptação binária do teste que verifica:
 - o cliente reenvia ficheiros persistidos em ML_CLIENT_CACHE_DIR,
 - e remove o ficheiro quando recebe o ACK correspondente.
"""
import os
import struct
from pathlib import Path

import pytest

from common import binary_proto
from rover.ml_client import SimpleMLClient


def test_client_resend_and_file_removal(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setenv("ML_CLIENT_CACHE_DIR", str(cache_dir))

    rover_id = "R-TEST"
    mission_id = "M-0001"
    msg_id = 0xA1B2C3D4E5F60001

    tlvs = []
    tlvs.append((binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8")))
    tlvs.append(binary_proto.tlv_progress(12))

    data = binary_proto.pack_ml_datagram(binary_proto.ML_PROGRESS, rover_id, tlvs, msgid=int(msg_id))

    fname = cache_dir / f"{rover_id}-{int(msg_id)}.bin"
    fname.write_bytes(data)
    assert fname.exists()

    proto = SimpleMLClient(rover_id=rover_id)

    class FakeTransport:
        def __init__(self):
            self.sent = []

        def sendto(self, packet: bytes, addr):
            self.sent.append((packet, addr))

        def get_extra_info(self, name, default=None):
            return None

    ft = FakeTransport()
    proto.transport = ft

    # Resend persisted packet
    proto._resend_persisted_packets(("127.0.0.1", 50000))

    assert len(ft.sent) == 1, f"expected one sendto() call, got {len(ft.sent)}"
    sent_packet, sent_addr = ft.sent[0]
    assert sent_addr == ("127.0.0.1", 50000)
    assert sent_packet == data

    # Simulate server ACK for that msg_id
    ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(msg_id)))]
    ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, "SERVER", ack_tlvs, msgid=0)

    # Deliver ACK to client (this should remove the persisted file)
    proto.datagram_received(ack_pkt, ("127.0.0.1", 50000))

    assert not fname.exists(), "persisted packet file should have been removed after ACK"