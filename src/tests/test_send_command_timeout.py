#!/usr/bin/env python3
"""
tests/test_send_command_timeout.py

Verify that TelemetryServer.send_command(..., expect_ack=True) raises a TimeoutError
when the connected rover does not send an application-level TS_ACK, and that the
server cleans up pending ACK futures.
"""
import asyncio
import time
import pytest

from common import binary_proto
from nave_mae.telemetry_server import TelemetryServer
# ADICIONADO: Importar Stores para inicialização limpa
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore


@pytest.mark.asyncio
async def test_send_command_times_out_and_cleans_pending():
    # Inicializa Stores (mesmo que não utilizadas ativamente neste teste)
    ms = MissionStore()
    ts = TelemetryStore(mission_store=ms)

    # Start server on ephemeral port
    # CORRIGIDO: Passar instâncias de stores
    srv = TelemetryServer(host="127.0.0.1", port=0, mission_store=ms, telemetry_store=ts)
    await srv.start()
    
    # Obter endereço do servidor
    sock = srv._server.sockets[0]
    host, port = sock.getsockname()[:2]

    # Abrir uma conexão TCP bruta para simular o cliente
    reader, writer = await asyncio.open_connection(host, port)

    try:
        # 1. Simular o envio de Telemetria para o servidor registar o rover
        rover_id = "R-TIMEOUT"
        # O payload JSON aciona o registo do rover no TelemetryServer
        tlvs = [(binary_proto.TLV_PAYLOAD_JSON, b'{"hello": true}')] 
        frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, msgid=0)
        
        # O cliente escreve o frame (que inclui o length prefix)
        writer.write(frame)
        await writer.drain()

        # 2. Esperar que o servidor registe o rover
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if rover_id in srv.get_connected_rovers():
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Rover {rover_id} did not register with server in time. Connected: {srv.get_connected_rovers()}")

        # 3. Tentar enviar comando e esperar Timeout
        # O cliente simulado não enviará o ACK, forçando o timeout.
        # Tempo de timeout baixo (0.2s) para acelerar o teste.
        with pytest.raises(asyncio.TimeoutError):
            await srv.send_command(rover_id, {"cmd": "noack"}, expect_ack=True, timeout=0.2)

        # 4. Verificar Limpeza
        # Após o timeout, o pending ack Future associado deve ser limpo
        assert len(srv._pending_acks) == 0, f"Expected pending_acks to be empty after timeout, found: {srv._pending_acks}"

    finally:
        # Limpeza da conexão e do servidor
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        await srv.stop()
