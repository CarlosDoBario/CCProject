import math
import pytest
from rover.rover_sim import RoverSim 
from pytest import approx 
from typing import Dict, Any, Tuple, Optional

# Define uma função de fixture para criar um Rover base
@pytest.fixture
def rover():
    # Posição (0, 0, 0) para forçar o estado RUNNING
    return RoverSim(rover_id="TEST_TEMP_R1", position=(0.0, 0.0, 0.0))

# ----------------------------------------------------------------------
# TESTE UNITÁRIO: Sobreaquecimento (Overheating) e Retomada de Missão
# ----------------------------------------------------------------------
def test_overheating_pause_and_resume(rover: RoverSim):
    """
    Testa o cenário: T > 35ºC (MAX_TEMP) -> Pausa Missão (COOLING) -> T <= 25ºC (INITIAL_TEMP) -> Retoma Missão.
    """
    # 1. SETUP: Obter constantes e preparar o Rover
    MAX_TEMP = RoverSim.MAX_TEMP          # 35.0°C (Limite de pausa)
    COOL_DOWN_TEMP = RoverSim.INITIAL_TEMP # 25.0°C (Limite de fim de COOLING)
    HEAT_RATE = RoverSim.HEAT_RATE_PER_S
    COOL_RATE = RoverSim.COOL_RATE_PER_S
    
    # Iniciar Missão de 60 segundos 
    mission_id = "M-TEST-TEMP"
    mission_spec = {"mission_id": mission_id, "task": "env_analysis", "max_duration_s": 60.0}
    rover.start_mission(mission_spec)
    
    # Garantir que estamos a correr
    assert rover.state == "RUNNING"
    
    # Configurar temperatura perto do limite para acelerar o teste (e.g., 34.0ºC)
    initial_temp = MAX_TEMP - 1.0 
    rover.internal_temp_c = initial_temp
    
    # 2. AÇÃO: Desencadear Overheating
    # Queremos aumentar 1.1ºC para garantir que ultrapassa o MAX_TEMP (35.0ºC)
    delta_temp = MAX_TEMP - initial_temp + 0.1 # 1.1ºC
    time_to_overheat = delta_temp / HEAT_RATE 
    
    # Executa a missão pelo tempo necessário
    rover.step(elapsed_s=time_to_overheat) 
    telemetry = rover.get_telemetry()
    
    # 3. VERIFICAÇÃO 1: Pausa (COOLING)
    progress_before_pause = rover._paused_mission_spec.get('progress_pct', 0.0) # Obtido do spec pausado
    
    assert rover.state == "COOLING"
    assert telemetry["status"] == "cooling_down"
    # Deve estar ligeiramente acima de 35.0ºC
    assert rover.internal_temp_c > MAX_TEMP
    assert any(err["code"] == "TEMP-OVERHEAT" for err in telemetry["errors"])
    
    # O rover deve ter guardado a missão original para a retoma
    paused_mission: Optional[Dict[str, Any]] = rover._paused_mission_spec
    assert paused_mission is not None
    assert paused_mission["mission_id"] == mission_id
    
    # 4. AÇÃO: Simular Arrefecimento até ao COOL_DOWN_TEMP (25.0ºC)
    temp_to_cool = rover.internal_temp_c - COOL_DOWN_TEMP
    # Tempo necessário para arrefecer: delta T / Cool Rate
    # Note: O RoverSim COOL_RATE_PER_S é 1.0
    time_to_cool = temp_to_cool / COOL_RATE 
    
    # Simula o arrefecimento (tempo + 1 segundo)
    rover.step(elapsed_s=time_to_cool + 1.0)
    telemetry = rover.get_telemetry()
    
    # 5. VERIFICAÇÃO 2: Saída do COOLING para IDLE
    # RoverSim transiciona para IDLE e limpa current_mission após arrefecimento
    assert rover.state == "IDLE"
    assert telemetry["status"] == "idle"
    # A temperatura deve ser 25.0ºC
    assert telemetry["internal_temp_c"] == approx(COOL_DOWN_TEMP, abs=0.1) 
    
    # 6. AÇÃO: Retomada Explícita da Missão Pausada (Simular Agente ML)
    # O Agente ML vê a missão pausada e ordena a retoma
    rover.start_mission(paused_mission)
    
    # 7. VERIFICAÇÃO 3: Retomada (RUNNING)
    assert rover.state == "RUNNING"
    assert rover.current_mission["mission_id"] == mission_id
    
    # O progresso deve ser o mesmo que antes da pausa
    assert rover.progress_pct == approx(progress_before_pause)
    
    # 8. VERIFICAÇÃO 4: Continuação do Progresso
    # A missão deve ter retomado, mostrando que o progresso avança após a retoma
    rover.step(elapsed_s=1.0)
    assert rover.state == "RUNNING"
    assert rover.progress_pct > progress_before_pause
