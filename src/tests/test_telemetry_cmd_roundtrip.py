import asyncio
import json
import pytest
import time # Para usar time.time() no deadline

from common import binary_proto # Necessário para a decodificação da asserção
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.mission_store import MissionStore
# REMOVIDO: from nave_mae.telemetry_hooks import register_telemetry_hooks 
from rover.telemetry_client import TelemetryClient


@pytest.mark.asyncio
async def test_telemetry_command_roundtrip():
    # 1. SETUP STORES E SERVIDOR
    ms = MissionStore()
    # CORRIGIDO: Inicializar TelemetryStore sem argumentos inválidos
    ts = TelemetryStore(mission_store=ms) 

    # REMOVIDO: register_telemetry_hooks(ms, ts)

    # Start server on ephemeral port
    server = TelemetryServer(mission_store=ms, telemetry_store=ts, host="127.0.0.1", port=0)
    await server.start()
    
    # Obter o endereço real atribuído pelo OS
    sock = server._server.sockets[0]
    host, port = sock.getsockname()[:2]

    rover_id = "R-CMD-1"
    
    # Eventos e variáveis para capturar o resultado no cliente
    cmd_received_evt = asyncio.Event()
    cmd_payload_holder = {}

    async def on_command(payload):
        # Callback executado no cliente quando o comando é recebido/decodificado
        cmd_payload_holder["payload"] = payload
        cmd_received_evt.set()

    # 2. INICIAR CLIENTE
    # O cliente envia telemetria periodicamente (interval_s=0.05)
    client = TelemetryClient(rover_id=rover_id, host=host, port=port, interval_s=0.05, reconnect=True)
    client.on_command(on_command)
    await client.start() # Inicia o loop de telemetria/reconexão

    try:
        # 3. ESPERAR PELO REGISTO DO ROVER NO SERVIDOR
        deadline = time.time() + 5.0
        while time.time() < deadline:
            # O rover é registado assim que a primeira mensagem de telemetria chega
            if rover_id in server.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Rover {rover_id} did not connect and register within timeout. Connected: {server.get_connected_rovers()}")

        # 4. ENVIAR COMANDO DO SERVIDOR PARA O CLIENTE
        cmd = {"cmd": "do_work", "params": {"value": 42}}
        # expect_ack=True faz com que server.send_command() bloqueie até receber o TS_ACK do cliente.
        msgid = await server.send_command(rover_id, cmd, expect_ack=True, timeout=5.0)
        
        # Se a chamada acima retornar, o ACK foi recebido.
        assert isinstance(msgid, int)

        # 5. VERIFICAR RECEÇÃO E CONTEÚDO NO CLIENTE
        
        # Esperar que o callback on_command do cliente seja executado
        await asyncio.wait_for(cmd_received_evt.wait(), timeout=2.0)
        
        assert "payload" in cmd_payload_holder
        
        received_payload = cmd_payload_holder["payload"]
        
        # O TelemetryServer empacota o comando em TLV_PAYLOAD_JSON
        # O cliente decodifica o TLV_PAYLOAD_JSON para o dicionário canónico.
        
        # O payload final deve conter a chave 'cmd' (vindo do payload JSON)
        # Assumimos que o cliente TelemetryClient processa o PAYLOAD_JSON e o passa ao callback
        assert isinstance(received_payload, dict)
        assert received_payload.get("cmd") == "do_work", f"Payload do comando incorreto: {received_payload}"
        assert received_payload.get("params", {}).get("value") == 42
        
    finally:
        # 6. CLEANUP
        await client.stop()
        # O stop() do TelemetryServer já trata do wait_closed e do server.close()
        await server.stop()
