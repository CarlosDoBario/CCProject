"""
Test: MissionStore crash + restart recovery

- cria uma MissionStore com persist_file (tmp)
- cria e assigna uma missão a um rover e usa update_progress para colocar IN_PROGRESS
- grava o ficheiro de persistência
- instancia uma nova MissionStore apontando para o mesmo ficheiro
- verifica que a missão foi marcada como RECOVERED (state -> CREATED, history inclui RECOVERED)
  e que o rover ficou em IDLE
"""
from nave_mae.mission_store import MissionStore


def test_missionstore_recovery(tmp_path):
    persist_path = tmp_path / "mission_store.json"

    # 1) criar store e registar rover
    ms1 = MissionStore(str(persist_path))
    ms1.register_rover("rover-1", ("127.0.0.1", 12345))

    # create a valid mission spec (use an allowed task)
    mission_spec = {
        "task": "capture_images",
        "area": {"x1": 10, "y1": 10, "z1": 0, "x2": 20, "y2": 20, "z2": 0},
        "params": {"interval_s": 5, "frames": 10},
        "priority": 1,
    }

    mid = ms1.create_mission(mission_spec)
    mission = ms1.get_mission(mid)
    assert mission is not None

    # assign to rover via public API
    ms1.assign_mission_to_rover(mission, "rover-1")

    # use the public API to set IN_PROGRESS (update_progress sets state to IN_PROGRESS)
    ms1.update_progress(mid, "rover-1", {"progress_pct": 10})

    # persist to file (atomic write)
    ms1.save_to_file()

    # 2) create a new store that should load and recover on startup
    ms2 = MissionStore(str(persist_path))

    # verify mission was recovered: state -> CREATED and history contains RECOVERED
    recovered = ms2.get_mission(mid)
    assert recovered is not None, "Recovered mission should exist"
    assert recovered["state"] == "CREATED", f"Expected state CREATED after recovery, got {recovered['state']}"

    history = recovered.get("history", [])
    types = [h.get("type") for h in history]
    assert "RECOVERED" in types, f"'RECOVERED' entry not found in history: {types}"

    # verify rover state reset to IDLE
    rover = ms2.get_rover("rover-1")
    assert rover is not None
    assert rover.get("state") == "IDLE"