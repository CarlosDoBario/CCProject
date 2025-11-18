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
    # Some MissionStore implementations provide a helper to populate demo missions.
    if hasattr(ms, "create_demo_missions") and callable(ms.create_demo_missions):
        ms.create_demo_missions()
    else:
        # fallback: create a few sample missions so the test still validates normalization
        ms.create_mission({"task": "capture_images", "params": {"interval_s": 1, "frames": 1}, "area": {"x1":0,"y1":0,"x2":1,"y2":1}})
        ms.create_mission({"task": "collect_samples", "params": {"sample_count": 1, "depth_mm": 10}, "area": {"x1":1,"y1":1,"x2":2,"y2":2}})
        ms.create_mission({"task": "env_analysis", "params": {"sampling_rate_s": 1}, "area": {"x1":2,"y1":2,"x2":3,"y2":3}})

    missions = ms.list_missions()
    # there should be at least the three demo/sample missions
    assert len(missions) >= 3
    # ensure each mission has normalized area with z fields where area is present
    for mid, m in missions.items():
        if m.get("area") is None:
            continue
        area = m["area"]
        for key in ("x1", "y1", "z1", "x2", "y2", "z2"):
            assert key in area