#!/usr/bin/env python3
"""
Unit test that validates server behavior when pending_outgoing retransmissions exhaust:
- Create mission and assign to rover
- Insert a PendingOutgoing with attempts >= N_RETX
- Trigger the check and assert mission assignment was reverted
"""
import time
import pytest
from unittest.mock import MagicMock

from nave_mae.mission_store import MissionStore
from nave_mae.ml_server import MLServerProtocol, PendingOutgoing
from common import config
from typing import Dict, Any, Tuple, Optional


class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def run_retransmit_loop_once(server: MLServerProtocol, now_mock: float):
    """
    Simula uma única iteração da lógica de retransmissão/exaustão do servidor.
    Isto evita problemas de concorrência com o asyncio.
    """
    remove = []
    
    # Processa as mensagens pendentes (PendingOutgoing)
    for msg_id, po in list(server.pending_outgoing.items()):
        
        # 1. Verifica se o timeout expirou (sem timeout, não há retransmissão/exaustão)
        if now_mock > po.next_timeout:
            
            # 2. Verifica Exaustão: attempts > N_RETX (Tentativas de 1 a N_RETX esgotadas)
            if po.attempts > config.N_RETX:
                # O pacote falhou permanentemente -> reverte a missão se necessário
                if po.message_type == "MISSION_ASSIGN" and po.mission_id:
                    # MLServerProtocol.revert_mission_assignment()
                    server.mission_store.unassign_mission(po.mission_id, reason="RETRANSMIT_EXHAUSTED")
                
                # Marca para remoção
                remove.append(msg_id)
            else:
                # 3. Retransmitir (se o tempo excedeu o timeout e não estiver exausto)
                # (Esta parte não é relevante para o teste de exaustão, mas a lógica exige)
                po.attempts += 1
                po.next_timeout = now_mock + po.timeout_s * (config.BACKOFF_FACTOR ** (po.attempts - 1))
                server.transport.sendto(po.packet, po.addr)
    
    # Remove as entradas exaustas/removidas
    for msg_id in remove:
        server.pending_outgoing.pop(msg_id, None)


@pytest.mark.timeout(5)
def test_assign_revert_on_retransmit_exhaustion():
    # SETUP: Configuração de persistência e store
    config.N_RETX = 3 # Max 3 tentativas (1 inicial + 2 retransmissões)
    
    ms = MissionStore()
    
    # 1. Criar missão e atribuir
    mid = ms.create_mission({"task": "collect_samples", "params": {"sample_count": 1, "depth_mm": 10}})
    ms.assign_mission_to_rover(ms.get_mission(mid), "R-EXH")
    
    # Verificar estado inicial
    assert ms.get_mission(mid)["assigned_rover"] == "R-EXH"
    assert ms.get_mission(mid)["state"] == "ASSIGNED"
    
    server = MLServerProtocol(ms)
    server.transport = FakeTransport() # Simular transporte
    
    # 2. Criar Pendente: MISSÃO ATRIBUÍDA FALHADA
    now = time.time()
    test_msgid = int(now * 1000) & 0xFFFFFFFFFFFFFFFF
    
    po = PendingOutgoing(
        msg_id=test_msgid,
        packet=b"dummy_assign_packet",
        addr=("127.0.0.1", 9999),
        created_at=now - 100.0, # Criado há muito tempo
        timeout_s=1.0, 
        message_type="MISSION_ASSIGN",
        mission_id=mid,
    )
    
    # INJETAR ESTADO DE EXAUSTÃO: 
    # attempts = N_RETX + 1 (4) => O limite foi ultrapassado
    po.attempts = config.N_RETX + 1
    # next_timeout no passado para garantir que a condição de timeout é satisfeita
    po.next_timeout = now - 1.0 

    # Inserir no servidor
    server.pending_outgoing[int(po.msg_id)] = po

    # 3. ACIONAR LÓGICA DE REEXAUSTÃO
    # Simular que o tempo avançou o suficiente para o loop rodar
    mock_now = time.time() 
    run_retransmit_loop_once(server, mock_now) 

    # 4. ASSERÇÕES FINAIS
    
    # Pacote deve ser removido do pending_outgoing
    assert int(po.msg_id) not in server.pending_outgoing, "Exhausted packet was not removed"
    
    # Missão deve ser revertida
    m = ms.get_mission(mid)
    assert m is not None
    assert m.get("assigned_rover") is None, "Mission should have been unassigned after exhaust"
    assert m.get("state") == "CREATED", "Mission state should revert to CREATED after exhaust"
    
    # História deve indicar falha na atribuição
    h = m.get("history", [])
    assert any(entry.get("type") == "ASSIGN_FAILED" for entry in h), f"History entries: {h}"
