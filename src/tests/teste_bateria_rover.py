import math
import pytest
# Importa a classe RoverSim do caminho do seu projeto
from rover.rover_sim import RoverSim 

# Constantes para simulação e limites
LOW_BATTERY = RoverSim.LOW_BATTERY_THRESHOLD
EMERGENCY_BATTERY = RoverSim.EMERGENCY_BATTERY_THRESHOLD
CHARGE_RATE = RoverSim.CHARGE_RATE_PER_S
TRAVEL_SPEED = RoverSim.TRAVEL_SPEED

# Define uma função de fixture para criar um Rover base
@pytest.fixture
def rover():
    # Rover ID: "TEST_R1", Posição Inicial: (0, 0, 0)
    return RoverSim(rover_id="TEST_R1", position=(0.0, 0.0, 0.0))

# Função auxiliar para calcular a distância entre o Rover e a Estação de Carregamento
def get_distance_to_charge_station(rover: RoverSim) -> float:
    cx, cy = rover.position["x"], rover.position["y"]
    tx, ty = rover.charging_station_position["x"], rover.charging_station_position["y"]
    return math.sqrt((tx - cx)**2 + (ty - cy)**2)

# ----------------------------------------------------------------------
# TESTE 1: Ciclo Completo de Carregamento (Viagem e Recarga)
# ----------------------------------------------------------------------
def test_full_charging_cycle_from_idle(rover):
    """
    Testa o cenário: Bateria < 10% em IDLE -> Viaja -> Carrega -> Volta para IDLE a 100%.
    """
    # 1. Preparação: Simular bateria baixa (abaixo de 10%)
    initial_low_battery = LOW_BATTERY - 0.1 
    rover.battery_level_pct = initial_low_battery
    distance_to_station = get_distance_to_charge_station(rover)
    
    # 2. Transição para CHARGING_TRAVEL
    # O passo é chamado em IDLE, o que deve forçar a transição para CHARGING_TRAVEL.
    rover.step(elapsed_s=0.1) 
    telemetry = rover.get_telemetry()
    
    assert rover.state == "CHARGING_TRAVEL"
    assert telemetry["status"] == "traveling_to_charge"
    assert telemetry["mission_id"].startswith("CHARGE-")
    
    # 3. Viagem para a Estação
    # Calcula o tempo teórico de viagem (simples)
    travel_time = distance_to_station / TRAVEL_SPEED
    
    # Simula o tempo de viagem total
    rover.step(elapsed_s=travel_time + 0.1)
    telemetry = rover.get_telemetry()
    
    # Verifica se chegou à estação
    assert get_distance_to_charge_station(rover) < 0.01 # Posição muito próxima do alvo
    
    # Verifica se entrou em CHARGING
    assert rover.state == "CHARGING"
    assert telemetry["status"] == "charging"
    
    # Verifica se a bateria drenou ligeiramente (travel_time * 0.1) e depois parou de viajar
    assert rover.battery_level_pct < initial_low_battery 

    # 4. Carregamento
    current_battery = rover.battery_level_pct
    battery_needed = 100.0 - current_battery
    # Calcula o tempo necessário para carregar
    charge_time = battery_needed / CHARGE_RATE
    
    # Simula o carregamento (95% do tempo necessário)
    rover.step(elapsed_s=charge_time * 0.95)
    assert rover.state == "CHARGING"
    assert rover.battery_level_pct < 100.0
    
    # Simula o carregamento até 100%
    rover.step(elapsed_s=charge_time * 0.1) # Um pouco a mais do que o necessário
    telemetry = rover.get_telemetry()
    
    # 5. Fim do Ciclo
    assert rover.state == "IDLE"
    assert telemetry["status"] == "idle"
    assert rover.battery_level_pct == 100.0
    assert rover.current_mission is None
    

# ----------------------------------------------------------------------
# TESTE 2: Emergência Crítica (< 5%) e Abandono de Missão
# ----------------------------------------------------------------------
def test_emergency_abort_during_mission(rover):
    """
    Testa o cenário: Durante RUNNING, bateria < 5% -> Abandona Missão -> Viaja para Carregar.
    """
    # 1. Preparação: Iniciar Missão (Assumindo que dura 10s e drena 2%/s = 20% total)
    mission_spec = {"mission_id": "M-TEST-ABORT", "task": "collect_samples", "max_duration_s": 10.0}
    rover.start_mission(mission_spec)
    
    # Simula uma bateria quase cheia, mas perto do limite de emergência para o teste ser rápido.
    # Ex: Bateria a 10% (5% acima do limite de emergência)
    rover.battery_level_pct = EMERGENCY_BATTERY + 5.0 # 10.0%
    
    # 2. Simulação de Drenagem Rápida
    
    # A taxa de drenagem é de (elapsed_s / 10.0) * 2.0 %. Para descer de 10% para 4%, 
    # precisamos drenar 6%.
    # 6% = (elapsed_s / 10.0) * 2.0  => elapsed_s = 6 * 10.0 / 2.0 = 30s
    # O simulador tem um cap interno de 60s, mas vamos usar um passo de 30s.
    
    elapsed_s_to_trigger_emergency = 30.0 
    
    rover.step(elapsed_s=elapsed_s_to_trigger_emergency)
    telemetry = rover.get_telemetry()
    
    # 3. Verificação de Abandono de Missão
    
    # A bateria deve ter descido para 10.0% - 6.0% = 4.0%
    final_battery = 10.0 - 6.0
    
    assert rover.state == "CHARGING_TRAVEL"
    assert telemetry["status"] == "traveling_to_charge"
    
    # Verifica o nível da bateria após a drenagem
    # Usamos o round() ou assertamos um intervalo devido à aritmética de ponto flutuante
    assert telemetry["battery_level_pct"] == pytest.approx(final_battery, abs=0.1)
    assert rover.battery_level_pct < EMERGENCY_BATTERY
    
    # Verifica o erro de emergência
    assert any(err["code"] == "BAT-EMERGENCY-ABORT" for err in telemetry["errors"])
    
    # A missão original deve ter sido substituída pela missão de carregamento de emergência
    assert "EMERG-CHARGE-" in telemetry["mission_id"]
    assert rover.progress_pct == 0.0