#!/usr/bin/env python3
import pytest
from common import ml_schema, mission_schema

def test_checksum_roundtrip():
    body = {"a":1, "b":"x"}
    env = ml_schema.build_envelope("REQUEST_MISSION", body=body, rover_id="R-TEST")
    b = ml_schema.envelope_to_bytes(env)
    parsed = ml_schema.parse_envelope(b)
    assert parsed["header"]["msg_id"] == env["header"]["msg_id"]

def test_validate_capture_images_ok():
    m = {"task":"capture_images", "params":{"interval_s":5, "frames":10}, "area":{"x1":0,"y1":0,"x2":1,"y2":1}}
    ok, errs = mission_schema.validate_mission_spec(m)
    assert ok

def test_validate_capture_images_missing():
    m = {"task":"capture_images", "params":{}, "area":{"x1":0,"y1":0,"x2":1,"y2":1}}
    ok, errs = mission_schema.validate_mission_spec(m)
    assert not ok
    assert any("interval_s" in e or "frames" in e for e in errs)