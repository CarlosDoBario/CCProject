#!/usr/bin/env python3
import json
import pytest

from common import binary_proto, mission_schema


def test_checksum_roundtrip():
    """
    Round-trip packing/unpacking of an ML REQUEST_MISSION datagram.
    We include the original body as a PAYLOAD_JSON TLV and assert the parsed header.msgid
    equals the msgid we provided when packing.
    """
    body = {"a": 1, "b": "x"}
    tlvs = [(binary_proto.TLV_PAYLOAD_JSON, json.dumps(body).encode("utf-8"))]
    msgid = 0x12345678ABCDEF  # deterministic numeric id for test
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_REQUEST_MISSION, "R-TEST", tlvs, msgid=msgid)
    parsed = binary_proto.parse_ml_datagram(pkt)
    assert int(parsed["header"].get("msgid", 0)) == int(msgid)


def test_validate_capture_images_ok():
    m = {"task": "capture_images", "params": {"interval_s": 5, "frames": 10}, "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1}}
    ok, errs = mission_schema.validate_mission_spec(m)
    assert ok, f"Expected valid mission spec, got errors: {errs}"


def test_validate_capture_images_missing():
    m = {"task": "capture_images", "params": {}, "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1}}
    ok, errs = mission_schema.validate_mission_spec(m)
    assert not ok
    # expect that validation errors mention interval_s or frames
    assert any("interval_s" in e or "frames" in e for e in errs)