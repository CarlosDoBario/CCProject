#!/usr/bin/env python3
import pytest
from nave_mae.mission_store import MissionStore

def test_create_valid_mission():
    ms = MissionStore()
    mid = ms.create_mission({
        "task": "collect_samples",
        "params": {"sample_count": 2, "depth_mm": 50},
        "area": {"x1": 0, "y1": 0, "x2": 10, "y2": 10}
    })
    assert isinstance(mid, str) and mid.startswith("M-")
    m = ms.get_mission(mid)
    assert m is not None
    assert m["task"] == "collect_samples"
    assert m["params"].get("sample_count") == 2
    # area should be normalized and include z1,z2
    assert "area" in m and isinstance(m["area"], dict)
    area = m["area"]
    for key in ("x1", "y1", "z1", "x2", "y2", "z2"):
        assert key in area

def test_create_invalid_mission_raises():
    ms = MissionStore()
    # missing required params for capture_images
    with pytest.raises(ValueError) as exc:
        ms.create_mission({
            "task": "capture_images",
            "params": {},  # missing interval_s and frames
            "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1}
        })
    msg = str(exc.value)
    # error message should include validation info
    assert "Invalid mission_spec" in msg or "interval_s" in msg or "frames" in msg

def test_create_demo_missions_populates_store():
    ms = MissionStore()
    ms.create_demo_missions()
    missions = ms.list_missions()
    # there should be at least the three demo missions
    assert len(missions) >= 3
    # ensure each mission has normalized area with z fields
    for mid, m in missions.items():
        if m.get("area") is None:
            continue
        area = m["area"]
        for key in ("x1", "y1", "z1", "x2", "y2", "z2"):
            assert key in area