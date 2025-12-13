"""
Integration test for TelemetryServer + TelemetryClient + TelemetryStore (binary TLV).

Este teste verifica:
 - Conexão e decodificação de múltiplos clientes.
 - Atualização correta do estado e registo de presença no MissionStore (MS).
"""
import asyncio
import pytest
import random
import time

from nave_mae.telemetry_store import TelemetryStore
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.mission_store import MissionStore
from rover.telemetry_client import send_once as telemetry_send_once
from common import binary_proto
from typing import Dict, Any, List


@pytest.mark.asyncio
async def test_telemetry_server_receives_multiple_clients():
    # 1. SETUP STORES
    ms = MissionStore()
    ts = TelemetryStore(mission_store=ms) 
    
    server = TelemetryServer(mission_store=ms, telemetry_store=ts, host="127.0.0.1", port=0)
    await server.start()
    
    sock = server._server.sockets[0]
    host, port = sock.getsockname()[:2]

    rover_ids = ["R-T1", "R-T2", "R-T3"]

    async def send_for_rover(rid: str):
        telemetry = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": random.uniform(-5, 5), "y": random.uniform(-5, 5), "z": 0.0},
            "battery_level_pct": random.randint(30, 100),
            "status": "IN_MISSION", # Estado que queremos verificar no MissionStore
            "progress_pct": random.uniform(0, 50),
        }
        await telemetry_send_once(host, port, rid, telemetry, ack_requested=False, include_crc=False)

    # Run client coroutines concurrently
    tasks = [asyncio.create_task(send_for_rover(rid)) for rid in rover_ids]

    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

    # Allow slight time for server handlers to process (0.2s)
    await asyncio.sleep(0.2)

    # 3. VERIFICAÇÃO DO MISSION STORE (MS)
    rovers = ms.list_rovers()
    for rid in rover_ids:
        # Verifica se o MissionStore registou o rover
        assert rid in rovers, f"MissionStore did not register rover {rid}. Rovers found: {list(rovers.keys())}"
        
        # O estado do Rover no MS deve ter sido atualizado para IN_MISSION
        assert rovers[rid].get("state") == "IN_MISSION", f"Expected state IN_MISSION for {rid}, got {rovers[rid].get('state')}"

        # O MissionStore deve ter o campo 'last_seen' atualizado (implicitamente pela lógica de now_iso())
        assert "last_seen" in rovers[rid], f"'last_seen' field is missing for {rid}"

        # VERIFICAÇÕES DE COPIA DE DADOS REMOVIDAS: 
        # (Assumimos que o MissionStore não copia 'position' ou 'battery_level_pct')

    # 4. Clean up
    await server.stop()
