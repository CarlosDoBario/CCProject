import math
import pytest
# Importa a classe RoverSim do caminho do seu projeto
from rover.rover_sim import RoverSim 
# Usado para garantir a precisão dos números flutuantes
from pytest import approx 

# Define uma função de fixture para criar um Rover base (necessário para o pytest)
@pytest.fixture
def rover():
    return RoverSim(rover_id="TEST_TEMP_R1", position=(0.0, 0.0, 0.0))

# ----------------------------------------------------------------------
# TESTE UNITÁRIO: Sobreaquecimento (Overheating) e Retomada de Missão
# ----------------------------------------------------------------------
def test_overheating_pause_and_resume(rover):
    """
    Testa o cenário: T > 40ºC -> Pausa Missão (COOLING) -> T <= 30ºC -> Retoma Missão.
    """
    # 1. SETUP: Obter constantes e preparar o Rover
    MAX_TEMP = RoverSim.MAX_TEMP
    COOL_DOWN_TEMP = RoverSim.COOL_DOWN_TEMP
    HEAT_RATE = RoverSim.HEAT_RATE_PER_S
    COOL_RATE = RoverSim.COOL_RATE_PER_S
    
    # Iniciar Missão de 60 segundos (tempo suficiente para aquecer)
    mission_spec = {"mission_id": "M-TEST-TEMP", "task": "env_analysis", "max_duration_s": 60.0}
    rover.start_mission(mission_spec)
    
    # Configurar temperatura perto do limite para acelerar o teste (e.g., 39ºC)
    initial_temp = MAX_TEMP - 1.0 
    rover.internal_temp_c = initial_temp
    
    # 2. AÇÃO: Desencadear Overheating
    # Tempo necessário para aumentar 1ºC (40 - 39): delta T / Heat Rate -> 1.0 / 0.05 = 20s
    time_to_overheat = 1.0 / HEAT_RATE 
    
    # Executa a missão por tempo_necessario + 1 segundo (garante que ultrapassa 40ºC)
    rover.step(elapsed_s=time_to_overheat + 1.0) 
    telemetry = rover.get_telemetry()
    
    # 3. VERIFICAÇÃO 1: Pausa (COOLING)
    progress_before_pause = telemetry["progress_pct"]
    
    assert rover.state == "COOLING"
    assert telemetry["status"] == "cooling_down"
    assert rover.internal_temp_c > MAX_TEMP
    assert any(err["code"] == "TEMP-OVERHEAT" for err in telemetry["errors"])
    
    # 4. AÇÃO: Simular Arrefecimento até 30ºC
    temp_to_cool = rover.internal_temp_c - COOL_DOWN_TEMP
    # Tempo necessário para arrefecer: delta T / Cool Rate
    time_to_cool = temp_to_cool / COOL_RATE 
    
    # Simula o arrefecimento (tempo + 1 segundo)
    rover.step(elapsed_s=time_to_cool + 1.0)
    telemetry = rover.get_telemetry()
    
    # 5. VERIFICAÇÃO 2: Retomada (RUNNING)
    # Deve voltar ao RUNNING e a temperatura deve estar em 30ºC
    assert rover.state == "RUNNING"
    assert telemetry["status"] == "running"
    assert telemetry["internal_temp_c"] == approx(COOL_DOWN_TEMP, abs=0.1) 
    
    # 6. VERIFICAÇÃO 3: Progresso (Continuação)
    # A missão deve ter retomado, mostrando que o progresso avançou após a pausa
    rover.step(elapsed_s=1.0)
    assert rover.state == "RUNNING"
    assert rover.progress_pct > progress_before_pause