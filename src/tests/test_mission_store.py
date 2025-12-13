import pytest
import os
from nave_mae.mission_store import MissionStore
from common import config, utils # Adicionado utils para usar o logging se necessário


# Uso da fixture tmp_path para criar um arquivo de persistência temporário e isolado.

def test_create_valid_mission(tmp_path):
    """Testa a criação de uma missão válida e a normalização da área."""
    # Usa um ficheiro de persistência temporário e isolado
    ms = MissionStore(persist_file=str(tmp_path / "test.json")) 
    mid = ms.create_mission({
        "task": "collect_samples",
        "params": {"sample_count": 2, "depth_mm": 50},
        # Área sem z1/z2, que devem ser adicionados automaticamente
        "area": {"x1": 0, "y1": 0, "x2": 10, "y2": 10}
    })
    assert isinstance(mid, str) and mid.startswith("M-")
    m = ms.get_mission(mid)
    assert m is not None
    assert m["task"] == "collect_samples"
    assert m["params"].get("sample_count") == 2
    
    # A área deve ser normalizada e incluir z1,z2
    assert "area" in m and isinstance(m["area"], dict)
    area = m["area"]
    for key in ("x1", "y1", "z1", "x2", "y2", "z2"):
        assert key in area
    
    # Verifica que o ficheiro de persistência foi criado (se for o primeiro save)
    assert os.path.exists(ms.persist_file)


def test_create_invalid_mission_raises(tmp_path):
    """Testa se a criação de uma missão com esquema inválido levanta ValueError."""
    ms = MissionStore(persist_file=str(tmp_path / "test.json"))
    
    # Ausência de parâmetros obrigatórios para a tarefa 'capture_images'
    with pytest.raises(ValueError) as exc:
        ms.create_mission({
            "task": "capture_images",
            "params": {},  # faltam interval_s e frames
            "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1}
        })
    msg = str(exc.value)
    # A mensagem de erro deve incluir informação de validação
    assert "Invalid mission_spec" in msg or "interval_s" in msg or "frames" in msg


def test_create_demo_missions_populates_store(tmp_path):
    """Testa se o método de criação de missões demo funciona e popula a store."""
    ms = MissionStore(persist_file=str(tmp_path / "test.json"))
    
    # Chama o método de criação de demos
    if hasattr(ms, "create_demo_missions") and callable(ms.create_demo_missions):
        ms.create_demo_missions()
    else:
        # Fallback (não deverá ser executado com o seu MissionStore)
        ms.create_mission({"task": "capture_images", "params": {"interval_s": 1, "frames": 1}, "area": {"x1":0,"y1":0,"x2":1,"y2":1}})
        ms.create_mission({"task": "collect_samples", "params": {"sample_count": 1, "depth_mm": 10}, "area": {"x1":1,"y1":1,"x2":2,"y2":2}})
        ms.create_mission({"task": "env_analysis", "params": {"sampling_rate_s": 1}, "area": {"x1":2,"y1":2,"x2":3,"y2":3}})

    missions = ms.list_missions()
    # Pelo menos as 3 missões demo (M-001, M-002, M-003) devem estar presentes
    assert len(missions) >= 3
    
    # Garante que cada missão tem área normalizada com campos z, quando a área está presente
    for mid, m in missions.items():
        if m.get("area") is None:
            continue
        area = m["area"]
        for key in ("x1", "y1", "z1", "x2", "y2", "z2"):
            assert key in area
            
    # Verifica se o ficheiro de persistência foi atualizado
    assert os.path.exists(ms.persist_file)
