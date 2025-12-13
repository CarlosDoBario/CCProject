import math
import pytest
from rover.rover_sim import RoverSim 
from typing import Dict, Any, Tuple

# Constantes Sincronizadas com RoverSim
LOW_BATTERY_IDLE = RoverSim.LOW_BATTERY_THRESHOLD_IDLE
LOW_BATTERY_RUNNING = RoverSim.LOW_BATTERY_THRESHOLD_RUNNING
CHARGE_RATE = RoverSim.CHARGE_RATE_PER_S
TRAVEL_SPEED = RoverSim.TRAVEL_SPEED
MISSION_DRAIN_RATE = 2.0 # % do total drenado por ciclo de missão (ver RoverSim.step)

# Define uma função de fixture para criar um Rover base
@pytest.fixture
def rover():
    # CORREÇÃO: Posição Inicial perto da área de missão (0, 0, 0) para forçar o estado RUNNING
    return RoverSim(rover_id="TEST_R1", position=(0.0, 0.0, 0.0))

# Função auxiliar para calcular a distância entre o Rover e a Estação de Carregamento
def get_distance_to_charge_station(rover: RoverSim) -> float:
    cx, cy = rover.position["x"], rover.position["y"]
    tx, ty = rover.charging_station_position["x"], rover.charging_station_position["y"]
    return math.sqrt((tx - cx)**2 + (ty - cy)**2)

# ----------------------------------------------------------------------
# TESTE 1: Ciclo Completo de Carregamento (Viagem e Recarga)
# ----------------------------------------------------------------------
def test_full_charging_cycle_from_idle(rover: RoverSim):
    """
    Testa o cenário: Bateria < 25% em IDLE -> Viaja -> Carrega -> Volta para IDLE a 100%.
    """
    # 1. Preparação: Simular bateria baixa (abaixo do limite IDLE 25.0%)
    initial_low_battery = LOW_BATTERY_IDLE - 0.1 # 24.9%
    rover.battery_level_pct = initial_low_battery
    
    # IMPORTANTE: Definir uma estação próxima para a viagem não consumir demasiada bateria
    rover.charging_station_position = rover.CHARGING_STATION_B # Posição 24.0, 24.0
    distance_to_station = get_distance_to_charge_station(rover)
    
    # 2. Transição para CHARGING_TRAVEL (Trigger no step em IDLE)
    rover.step(elapsed_s=0.1) 
    telemetry = rover.get_telemetry()
    
    assert rover.state == "CHARGING_TRAVEL"
    assert telemetry["status"] == "traveling_to_charge"
    assert telemetry["mission_id"].startswith("EMERG-CHARGE-") 
    
    # 3. Viagem para a Estação
    travel_time = distance_to_station / TRAVEL_SPEED
    
    # Simula o tempo de viagem total (drenagem de 0.1% por segundo durante a viagem)
    rover.step(elapsed_s=travel_time + 0.1)
    telemetry = rover.get_telemetry()
    
    # Verifica se chegou à estação
    assert get_distance_to_charge_station(rover) < 0.1 
    
    # Verifica se entrou em CHARGING
    assert rover.state == "CHARGING"
    assert telemetry["status"] == "charging"
    
    # A bateria deve ter drenado ligeiramente e estar abaixo do nível inicial
    assert rover.battery_level_pct < initial_low_battery 

    # 4. Carregamento
    current_battery = rover.battery_level_pct
    battery_needed = 100.0 - current_battery
    
    # Calcula o tempo necessário para carregar
    charge_time = battery_needed / CHARGE_RATE
    
    # Simula o carregamento (um pouco a mais do que o necessário)
    rover.step(elapsed_s=charge_time * 1.05) 
    telemetry = rover.get_telemetry()
    
    # 5. Fim do Ciclo
    assert rover.state == "IDLE"
    assert telemetry["status"] == "idle"
    assert rover.battery_level_pct == pytest.approx(100.0) 
    assert rover.current_mission is None
    

# ----------------------------------------------------------------------
# TESTE 2: Emergência Crítica (< 20%) e Abandono de Missão
# ----------------------------------------------------------------------
def test_emergency_abort_during_mission(rover: RoverSim):
    """
    Testa o cenário: Durante RUNNING, bateria < 20% -> Abandona Missão -> Viaja para Carregar.
    
    O rover está na posição inicial (0, 0, 0), o que força o estado RUNNING.
    """
    # 1. Preparação: Iniciar Missão com duração de 10s
    mission_total_time = 10.0
    # Missão sem área para garantir que o alvo é (0,0,0)
    mission_spec = {"mission_id": "M-TEST-ABORT", "task": "collect_samples", "max_duration_s": mission_total_time}
    rover.start_mission(mission_spec)
    
    # O estado inicial DEVE ser RUNNING
    assert rover.state == "RUNNING", f"Expected RUNNING state, got {rover.state}"
    
    # Bateria inicial: ligeiramente acima do limite de emergência (20.0%)
    initial_battery = LOW_BATTERY_RUNNING + 5.0 # 25.0%
    rover.battery_level_pct = initial_battery
    
    # A missão drena 2.0% em 10s (mission_total_time). Drain rate = 0.2% por segundo (drenagem missao/total_time * 2.0).
    drain_rate_per_s = MISSION_DRAIN_RATE / mission_total_time 
    
    # Queremos drenar 5.0% (de 25% para 20%)
    drain_needed = initial_battery - LOW_BATTERY_RUNNING # 5.0%
    
    # Tempo necessário: 5.0% / 0.2% por segundo = 25 segundos
    elapsed_s_to_trigger_emergency = drain_needed / drain_rate_per_s 
    
    # 2. Simulação de Drenagem (step)
    # Damos um passo maior (25.1s) para garantir que:
    # 1) Atingimos o limite (20%).
    # 2) O progresso avança significativamente (25.1s / 10s = 251% do tempo da missão!).
    # NOTA: O progress_pct irá ser limitado a 100% pelo código do Rover.
    rover.step(elapsed_s=elapsed_s_to_trigger_emergency + 0.1) 
    telemetry = rover.get_telemetry()
    
    # 3. Verificação de Abandono de Missão
    
    # A bateria deve ter descido abaixo de 20.0%
    assert rover.battery_level_pct <= LOW_BATTERY_RUNNING
    
    # O estado deve ser CHARGING_TRAVEL
    assert rover.state == "CHARGING_TRAVEL"
    assert telemetry["status"] == "traveling_to_charge"
    
    # Verifica o erro de emergência (código BAT-PAUSE-CHARGE)
    assert any(err["code"] == "BAT-PAUSE-CHARGE" for err in telemetry["errors"])
    
    # O progresso da missão interrompida deve ser > 0
    # O progress_pct deve ser 100% (pois 25.1s > mission_total_time=10s)
    assert rover._paused_mission_spec is not None
    
    # Como elapsed_s > mission_total_time, o progress_pct deve ser 100.0% (limitado pelo RoverSim)
    expected_progress = 100.0 
    
    assert rover._paused_mission_spec.get('progress_pct', 0.0) == expected_progress
