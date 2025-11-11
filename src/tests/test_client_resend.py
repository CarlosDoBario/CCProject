#!/usr/bin/env python3
"""
tests/test_client_resend.py

Unit test that verifies the ML client will:
 - detect persisted packets in ML_CLIENT_CACHE_DIR,
 - resend them via the transport,
 - remove the persisted file when an ACK for that msg_id is received.

This test does not open network sockets: it uses a fake transport to capture sendto()
and invokes datagram_received(...) on the protocol to simulate receipt of an ACK.
"""

import os
from pathlib import Path

import pytest

from common import ml_schema
from rover.ml_client import MLClientProtocol


def test_client_resend_and_file_removal(tmp_path, monkeypatch):
    # Prepare a temporary cache dir and tell the client to use it
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setenv("ML_CLIENT_CACHE_DIR", str(cache_dir))

    rover_id = "R-TEST"

    # Create a persisted PROGRESS envelope and write it to the cache
    env = ml_schema.build_envelope("PROGRESS", body={"mission_id": "M-0001", "progress_pct": 12}, rover_id=rover_id)
    data = ml_schema.envelope_to_bytes(env)
    msg_id = env["header"]["msg_id"]

    fname = cache_dir / f"{rover_id}-{msg_id}.bin"
    fname.write_bytes(data)
    assert fname.exists()

    # Create protocol and a fake transport to capture sendto calls
    proto = MLClientProtocol(rover_id=rover_id)

    class FakeTransport:
        def __init__(self):
            self.sent = []

        def sendto(self, packet: bytes, addr):
            # record packet bytes and destination addr
            self.sent.append((packet, addr))

        def get_extra_info(self, name, default=None):
            return None

    ft = FakeTransport()
    proto.transport = ft

    # Call resend routine (should read file and call transport.sendto once)
    proto._resend_persisted_packets(("127.0.0.1", 50000))

    assert len(ft.sent) == 1, f"expected one sendto() call, got {len(ft.sent)}"
    sent_packet, sent_addr = ft.sent[0]
    assert sent_addr == ("127.0.0.1", 50000)
    assert sent_packet == data

    # Now simulate server sending back an ACK for that msg_id
    ack_env = ml_schema.make_ack(msg_id, rover_id="SERVER", mission_id=env["header"].get("mission_id"))
    ack_bytes = ml_schema.envelope_to_bytes(ack_env)

    # Simulate arrival of ACK datagram; this should trigger file removal in _handle_incoming_ack
    proto.datagram_received(ack_bytes, ("127.0.0.1", 50000))

    # File should be removed by the client upon ACK
    assert not fname.exists(), "persisted packet file should have been removed after ACK"