"""
Test: MissionStore crash + restart recovery

- cria uma MissionStore com persist_file (tmp)
- cria e assigna uma missão a um rover e usa update_progress para colocar IN_PROGRESS
- grava o ficheiro de persistência
- instancia uma nova MissionStore apontando para o mesmo ficheiro
- verifica que a missão foi marcada como RECOVERED (state -> CREATED, history inclui RECOVERED)
  e que o rover ficou em IDLE
"""
import os
import pytest
from nave_mae.mission_store import MissionStore


def test_missionstore_recovery(tmp_path):
    # O tmp_path fornece um diretório temporário seguro para o teste
    persist_path = tmp_path / "mission_store.json"

    # --- 1) SETUP: Criar store e simular estado IN_PROGRESS ---
    ms1 = MissionStore(str(persist_path))
    
    # 1.1) Criar e registar Rover. O endereço é opcional, mas garante que o registo existe.
    # O register_rover no seu código aceita tuplas (ip, port).
    ms1.register_rover("rover-1", ("127.0.0.1", 12345))

    # 1.2) Criar uma missão válida
    mission_spec = {
        "task": "capture_images",
        "area": {"x1": 10, "y1": 10, "z1": 0, "x2": 20, "y2": 20, "z2": 0},
        "params": {"interval_s": 5, "frames": 10},
        "priority": 1,
    }

    mid = ms1.create_mission(mission_spec)
    mission = ms1.get_mission(mid)
    assert mission is not None

    # 1.3) Atribuir a missão
    ms1.assign_mission_to_rover(mission, "rover-1")

    # 1.4) Simular progresso (coloca a missão em estado IN_PROGRESS)
    ms1.update_progress(mid, "rover-1", {"progress_pct": 10})

    # Verifica o estado antes do "crash"
    assert ms1.get_mission(mid)["state"] == "IN_PROGRESS"
    assert ms1.get_rover("rover-1")["state"] == "IN_MISSION"

    # 1.5) Persistir o estado (Simula o crash)
    ms1.save_to_file()

    # --- 2) RECOVERY: Criar uma nova store que carrega o ficheiro ---
    
    # Instancia uma nova MissionStore que deve carregar o ficheiro e iniciar a recuperação
    ms2 = MissionStore(str(persist_path))

    # --- 3) ASSERÇÕES: Verificar o estado após a recuperação ---
    
    # 3.1) Verificar a missão
    recovered = ms2.get_mission(mid)
    assert recovered is not None, "Recovered mission should exist"
    # Estado esperado: De IN_PROGRESS para CREATED
    assert recovered["state"] == "CREATED", f"Expected state CREATED after recovery, got {recovered['state']}"

    # A história deve conter o evento RECOVERED
    history = recovered.get("history", [])
    types = [h.get("type") for h in history]
    assert "RECOVERED" in types, f"'RECOVERED' entry not found in history: {types}"
    
    # CORREÇÃO: O seu MissionStore não adiciona "CREATED" à história. 
    # A história deve ter pelo menos 3 entradas: ASSIGNED, PROGRESS, e RECOVERED.
    assert len(types) >= 3, f"Expected at least ASSIGNED, PROGRESS, RECOVERED entries. Got: {types}"
    
    # 3.2) Verificar o Rover
    rover = ms2.get_rover("rover-1")
    assert rover is not None
    # Estado esperado: De IN_MISSION para IDLE
    assert rover.get("state") == "IDLE", f"Expected rover state IDLE after recovery, got {rover.get('state')}"
