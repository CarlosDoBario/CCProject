#!/usr/bin/env python3
"""
Unit test that verifies TelemetryServer.send_command raises KeyError
when attempting to send a command to a rover that is not currently connected.
"""
import pytest
import asyncio

# Assumimos que TelemetryServer pode ser importado
from nave_mae.telemetry_server import TelemetryServer
# Importar MissionStore e TelemetryStore para inicialização correta
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore


@pytest.mark.asyncio
async def test_send_command_raises_keyerror_when_rover_not_connected():
    # Inicializa as Stores necessárias para o TelemetryServer (mesmo que vazias)
    ms = MissionStore()
    ts = TelemetryStore(mission_store=ms)

    # Nota: Não é necessário chamar srv.start() ou ligar a porta, pois estamos a testar
    # apenas a lógica interna de `send_command` antes de tentar I/O.
    srv = TelemetryServer(host="127.0.0.1", port=0, mission_store=ms, telemetry_store=ts)
    
    # 1. Pré-condição: Garantir que o rover não está listado
    assert "NONEXISTENT" not in srv.get_connected_rovers()
    assert "NONEXISTENT" not in srv._clients
    
    # Comando de teste
    test_command = {"cmd": "noop"}

    # 2. Assert (expect_ack=False)
    # O comando deve falhar imediatamente, antes de qualquer operação de rede.
    with pytest.raises(KeyError, match="NONEXISTENT"):
        await srv.send_command("NONEXISTENT", test_command, expect_ack=False)

    # 3. Assert (expect_ack=True com timeout)
    with pytest.raises(KeyError, match="NONEXISTENT"):
        await srv.send_command("NONEXISTENT", test_command, expect_ack=True, timeout=0.1)
