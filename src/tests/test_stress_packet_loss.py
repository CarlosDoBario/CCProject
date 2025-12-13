# tests/test_stress_packet_loss.py
# Replacement stress test that simulates packet loss via an in-memory lossy transport.
import asyncio
import random
import time
import pytest
from typing import Dict, Any, Tuple, List
import struct

from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
from rover.ml_client import SimpleMLClient
from common import config
from common import binary_proto 


class LossyTransport:
    """
    Fake transport that forwards packets to a destination protocol.datagram_received(...)
    with a configurable drop rate and jitter. Deterministic if random.seed is set.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, dest_proto, dest_addr, drop_rate: float = 0.25, max_delay_s: float = 0.02):
        self.loop = loop
        self.dest = dest_proto
        self.dest_addr = dest_addr 
        self.drop_rate = drop_rate
        self.max_delay = max_delay_s
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))
        if random.random() < self.drop_rate:
            return
        delay = random.uniform(0, self.max_delay)
        self.loop.call_later(delay, self._deliver, packet, self.dest_addr) 

    def _deliver(self, packet: bytes, addr):
        try:
            self.dest.datagram_received(packet, addr)
        except Exception:
            pass

    def get_extra_info(self, name, default=None):
        if name == "sockname":
            return self.dest_addr
        return None


@pytest.mark.asyncio
async def test_stress_packet_loss_retransmit_and_recovery():
    """
    Stress test com perda de pacotes. Testa se o cliente/servidor ML completam a missão 
    usando retransmissão, apesar da perda de pacotes.
    """
    random.seed(12345)

    # 1. Configuração de parâmetros mais agressivos para o teste
    orig_timeout = config.TIMEOUT_TX_INITIAL
    orig_nretx = config.N_RETX
    orig_backoff = config.BACKOFF_FACTOR
    orig_update_interval = config.DEFAULT_UPDATE_INTERVAL_S
    
    config.TIMEOUT_TX_INITIAL = 0.05
    config.N_RETX = 20
    config.BACKOFF_FACTOR = 1.5
    config.DEFAULT_UPDATE_INTERVAL_S = 0.05 

    loop = asyncio.get_event_loop()
    
    # --- SETUP DOS ENDEREÇOS ---
    SERVER_ADDR = ("127.0.0.1", 64070) 
    CLIENT_ADDR = ("127.0.0.1", 50000) 

    # Variáveis para cleanup
    server_retx_task = None
    client_retx_task = None

    try:
        # 2. Inicializar Mission Store e Missão Curta
        ms = MissionStore()
        mission_spec = {
            "task": "capture_images",
            "params": {"interval_s": 0.01, "frames": 3},
            "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1},
            "priority": 1,
        }
        ms.create_mission(mission_spec)

        # 3. Inicializar Protocolos
        server = MLServerProtocol(ms)
        client = SimpleMLClient(rover_id="R-STRESS", server=SERVER_ADDR, exit_on_complete=True)

        # 4. Criar Lossy Transports
        client_transport = LossyTransport(loop, server, CLIENT_ADDR, drop_rate=0.30, max_delay_s=0.03)
        server_transport = LossyTransport(loop, client, SERVER_ADDR, drop_rate=0.05, max_delay_s=0.02) 

        # 5. Ligar Protocolos e Transports
        server.connection_made(server_transport)
        client.connection_made(client_transport)

        # 6. Iniciar Tasks de Loop (Manualmente)
        # O _retransmit_loop é necessário para o servidor enviar a missão
        server_retx_task = asyncio.create_task(server._retransmit_loop())
        
        # O retransmit_loop do cliente é essencial para o progresso
        client_retx_task = asyncio.create_task(client.retransmit_loop())
        
        # O request_mission() inicia a comunicação
        await client.request_mission()


        # 7. Esperar pela Missão Completa
        deadline = time.time() + 40.0
        completed = False
        while time.time() < deadline:
            missions = ms.list_missions()
            for mid, m in missions.items():
                if m.get("state") == "COMPLETED" and m.get("assigned_rover") == "R-STRESS":
                    completed = True
                    break
            if completed:
                break
            await asyncio.sleep(0.1)

        # 8. Asserções
        assert completed, f"Expected mission to be completed by R-STRESS despite packet loss; snapshot: {ms.list_missions()}"
        
        # CORREÇÃO CRÍTICA: Aguardar um tempo extra para permitir que o servidor termine o processamento 
        # do MISSION_COMPLETE (e envie o ACK final) antes de o loop ser parado.
        await asyncio.sleep(1.0)


    finally:
        # 9. Cleanup
        
        # Cancelar tasks em background
        if server_retx_task and not server_retx_task.done():
            server_retx_task.cancel()
        if client_retx_task and not client_retx_task.done():
            client_retx_task.cancel()
        
        # Aguardar para resolver os cancelamentos (evitar warnings)
        await asyncio.sleep(0.1) 
        
        # Restaurar a configuração
        config.TIMEOUT_TX_INITIAL = orig_timeout
        config.N_RETX = orig_nretx
        config.BACKOFF_FACTOR = orig_backoff
        config.DEFAULT_UPDATE_INTERVAL_S = orig_update_interval
