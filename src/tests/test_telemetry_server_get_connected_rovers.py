#!/usr/bin/env python3
"""
tests/test_telemetry_server_get_connected_rovers.py

Verify TelemetryServer.get_connected_rovers() returns currently connected rover IDs
and that mappings are removed when clients disconnect.
"""
import asyncio
import time
import pytest

from common import binary_proto
from nave_mae.telemetry_server import TelemetryServer
# ADICIONADO: Importar Stores
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore


@pytest.mark.asyncio
async def test_get_connected_rovers_adds_and_removes_on_disconnect():
    # Inicializar Stores vazias para evitar que o TelemetryServer falhe
    ms = MissionStore()
    ts = TelemetryStore(mission_store=ms)

    srv = TelemetryServer(host="127.0.0.1", port=0, mission_store=ms, telemetry_store=ts)
    await srv.start()
    try:
        sock = srv._server.sockets[0]
        host, port = sock.getsockname()[:2]

        # helper to open a raw connection and send one telemetry frame for a given rover_id
        async def open_and_send(rover_id: str):
            reader, writer = await asyncio.open_connection(host, port)
            # O TelemetryServer regista o rover assim que o primeiro payload é recebido.
            tlvs = [(binary_proto.TLV_PAYLOAD_JSON, b'{"hello": true}')]
            frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, msgid=0)
            writer.write(frame)
            await writer.drain()
            return reader, writer

        # open two connections for two rovers
        r1 = "R-ONE"
        r2 = "R-TWO"
        reader1, writer1 = await open_and_send(r1)
        reader2, writer2 = await open_and_send(r2)

        # 1. VERIFICAR REGISTO
        deadline = time.time() + 5.0
        while time.time() < deadline:
            conns = srv.get_connected_rovers()
            if r1 in conns and r2 in conns:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Rovers did not register within timeout. Connected: {srv.get_connected_rovers()}")

        # Assert both present
        conns = srv.get_connected_rovers()
        assert r1 in conns and r2 in conns

        # 2. FECHAR 1ª CONEXÃO E VERIFICAR REMOÇÃO
        writer1.close()
        try:
            await writer1.wait_closed()
        except Exception:
            pass

        deadline = time.time() + 5.0
        while time.time() < deadline:
            conns = srv.get_connected_rovers()
            # Esperar até que R-ONE seja removido
            if r1 not in conns:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Rover {r1} mapping not removed after disconnect. Connected: {srv.get_connected_rovers()}")

        # Garantir que o segundo continua presente
        conns = srv.get_connected_rovers()
        assert r2 in conns

        # 3. FECHAR 2ª CONEXÃO E VERIFICAR REMOÇÃO
        writer2.close()
        try:
            await writer2.wait_closed()
        except Exception:
            pass

        deadline = time.time() + 5.0
        while time.time() < deadline:
            conns = srv.get_connected_rovers()
            # Esperar até que R-TWO seja removido
            if r2 not in conns:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"Rover {r2} mapping not removed after disconnect. Connected: {srv.get_connected_rovers()}")

        # Finalmente, a lista de conectados deve estar vazia
        assert srv.get_connected_rovers() == []

    finally:
        await srv.stop()
