#!/usr/bin/env python3
"""
tests/test_telemetry_reconnect.py

Integration test: start server A, start client, verify registration;
stop server A, start server B on same port, verify client reconnects and re-registers.
"""
import asyncio
import socket
import time
import pytest
from typing import Optional, Dict, Any

from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.mission_store import MissionStore
# REMOVIDO: from nave_mae.telemetry_hooks import register_telemetry_hooks 
from rover.telemetry_client import TelemetryClient


def _get_free_port() -> int:
    """Reserve an ephemeral port and return it (close socket so server can bind)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_telemetry_client_reconnect_on_server_restart():
    host = "127.0.0.1"
    port = _get_free_port()
    rover_id = "R-RECON"

    # 1. SETUP SERVER A
    ms_a = MissionStore()
    # CORRIGIDO: Inicializa TelemetryStore corretamente
    ts_a = TelemetryStore(mission_store=ms_a)
    # REMOVIDO: register_telemetry_hooks(ms_a, ts_a) 

    srv_a = TelemetryServer(mission_store=ms_a, telemetry_store=ts_a, host=host, port=port)
    await srv_a.start()

    # 2. START CLIENT
    # reconnect=True é a chave para o teste
    client = TelemetryClient(
        rover_id=rover_id, 
        host=host, 
        port=port, 
        interval_s=0.05, 
        reconnect=True, 
        backoff_base=0.5, 
        backoff_factor=1.5
    )
    # O start() inicia o loop de envio e reconexão
    await client.start()

    try:
        # 3. VERIFICAR CONEXÃO COM SERVER A
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if rover_id in srv_a.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Client did not register with server A on port {port} within timeout")

        # 4. PARAR SERVER A (simular falha/reinício)
        await srv_a.stop()

        # Wait briefly to allow client to detect disconnect and attempt reconnect
        await asyncio.sleep(0.5) # Aumentei ligeiramente o sleep para detetar o disconnect

        # 5. START SERVER B (na mesma porta)
        ms_b = MissionStore()
        # CORRIGIDO: Inicializa TelemetryStore corretamente
        ts_b = TelemetryStore(mission_store=ms_b)
        # REMOVIDO: register_telemetry_hooks(ms_b, ts_b) 
        
        srv_b = TelemetryServer(mission_store=ms_b, telemetry_store=ts_b, host=host, port=port)
        await srv_b.start()

        # 6. VERIFICAR RECONEXÃO COM SERVER B
        deadline = time.time() + 20.0 # Timeout maior devido ao backoff e ao restart
        while time.time() < deadline:
            if rover_id in srv_b.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Client did not reconnect to server B on port {port} within timeout")

        # 7. VERIFICAR SE O ESTADO FOI PERSISTIDO EM SERVER B (via TelemetryStore)
        # Usamos o list_rovers do MissionStore, que deve ter sido atualizado pelo TelemetryServer B
        rovers_b = ms_b.list_rovers()
        assert rover_id in rovers_b, "MissionStore on restarted server did not register rover after reconnect"
        
        # O MissionStore deve ter recebido pelo menos uma telemetria, atualizando 'last_seen'
        assert "last_seen" in rovers_b[rover_id]
        
    finally:
        # 8. CLEANUP (garantir que todos os serviços são parados)
        try:
            await client.stop()
        except Exception:
            pass
        # Garantir que o srv_a está realmente parado (já deve ter sido parado, mas por segurança)
        try:
            await srv_a.stop()
        except Exception:
            pass
        # Parar srv_b
        try:
            await srv_b.stop()
        except Exception:
            pass
