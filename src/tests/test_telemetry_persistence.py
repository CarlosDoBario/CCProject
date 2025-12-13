#!/usr/bin/env python3
"""
tests/test_telemetry_persistence.py

Integration test that demonstrates NDJSON persistence of telemetry via a simple hook.
- This test directly instantiates TelemetryServer and TelemetryStore.
- It asserts that the NDJSON file was created and contains the expected records.
"""
import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Any, List

import pytest

from common import binary_proto
# ADICIONADO: Importar classes diretamente
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.mission_store import MissionStore # Necessário para TelemetryStore
from rover.telemetry_client import send_once


@pytest.mark.asyncio
async def test_telemetry_persistence_ndjson(tmp_path):
    outdir = tmp_path / "telemetry"
    outdir.mkdir(parents=True, exist_ok=True)

    # 1. SETUP: Iniciar TelemetryServer e obter TelemetryStore
    ms = MissionStore()
    ts = TelemetryStore(mission_store=ms) 
    
    # Inicia o servidor em processo, obtendo a porta efêmera (port=0)
    server = TelemetryServer(mission_store=ms, telemetry_store=ts, host="127.0.0.1", port=0)
    await server.start()
    
    sock = server._server.sockets[0]
    host, port = sock.getsockname()[:2]
    
    assert server is not None, "TelemetryServer failed to start"
    assert ts is not None, "TelemetryStore failed to initialize"


    # 2. Criar e registar o hook de persistência NDJSON
    def simple_persist_hook(event_type: str, payload: dict):
        # Apenas processa eventos de atualização de telemetria
        if event_type != "telemetry_update":
             return

        try:
            # O payload esperado é {"rover_id": rid, "telemetry": {canonical_payload}}
            # Se o TelemetryStore emitir "telemetry_update", o payload é a telemetria canónica
            # (Verifique o TelemetryStore._emit na linha 141)
            # O TelemetryStore._emit envia: self._emit("telemetry_update", {"rover_id": rover_id, "telemetry": telemetry})
            
            tel: Dict[str, Any]
            if "telemetry" in payload and isinstance(payload["telemetry"], dict):
                rid = payload.get("rover_id")
                tel = payload["telemetry"]
            else:
                 # Fallback/Erro de formato de hook
                 rid = payload.get("rover_id", "<unknown>")
                 tel = payload


            ts_ms = int(tel.get("ts_ms") or tel.get("timestamp_ms") or int(time.time() * 1000))
            rid = rid or tel.get("rover_id") or "<unknown>" # Garante que o ID está presente

            rec = {
                "ts_ms": ts_ms,
                "rover_id": rid,
                # Campos chave
                "position": tel.get("position"),
                "progress_pct": tel.get("progress_pct"),
                "battery_level_pct": tel.get("battery_level_pct"),
                "status": tel.get("status"),
                # Incluir o payload completo para debugging
                "payload_json": tel, 
            }
            ymd = time.strftime("%Y%m%d", time.gmtime(ts_ms / 1000.0))
            fname = outdir / f"telemetry-{ymd}.ndjson"
            
            # append JSON line (sync, small writes acceptable for test)
            with open(fname, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                
        except Exception as e:
            # Em testes, queremos que os erros sejam lançados
            raise AssertionError(f"Error in simple_persist_hook: {e}") from e

    # Register hook on TelemetryStore 
    # O TelemetryStore tem o método register_hook.
    ts.register_hook(simple_persist_hook)


    # 3. Enviar Telemetria e Esperar Processamento
    rover_ids = ["R-P-1", "R-P-2", "R-P-3"]
    for rid in rover_ids:
        telemetry = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": 1.0, "y": 2.0, "z": 0.0},
            "battery_level_pct": 75,
            "status": "IN_MISSION",
            "progress_pct": 10.0,
        }
        await send_once(host, port, rid, telemetry, ack_requested=False, include_crc=False)
        # Pausa para permitir que o servidor processe e o hook escreva (síncrono)
        await asyncio.sleep(0.05) 

    # 4. Finalizar e Assert
    # Allow some time for hook writes to complete (embora sejam síncronas)
    await asyncio.sleep(0.2)

    # Determinar o ficheiro NDJSON esperado para hoje
    ymd_now = time.strftime("%Y%m%d", time.gmtime(time.time()))
    fname = outdir / f"telemetry-{ymd_now}.ndjson"
    
    assert fname.exists(), f"Expected NDJSON file {fname} to exist"

    # Ler e validar o conteúdo
    text = fname.read_text(encoding="utf-8").strip()
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) >= 3, f"Expected at least 3 persisted lines, found {len(lines)}"

    # Parse das últimas três linhas e garantir que todos os rovers estão presentes
    found_ids = set()
    # Ler todas as linhas para garantir que não perdemos nenhum registro por causa da ordem
    for ln in lines:
        try:
            obj = json.loads(ln)
            found_ids.add(obj.get("rover_id"))
        except json.JSONDecodeError as e:
             pytest.fail(f"Failed to decode JSON line in persisted file: {ln}. Error: {e}")
             
    for rid in rover_ids:
        assert rid in found_ids, f"Persisted file missing telemetry for rover {rid}"

    # Cleanup: stop server
    await server.stop()
