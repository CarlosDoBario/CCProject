import time
import struct
from typing import List, Tuple, Dict, Any

from nave_mae.mission_store import MissionStore
from nave_mae.ml_server import MLServerProtocol
from common import binary_proto, config


class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def test_send_mission_cancel_and_ack_flow():
    # 1. SETUP: Mission Store e Server Protocol
    ms = MissionStore()
    
    # Cria a missão e atribui ID
    mid = ms.create_mission({"task": "collect_samples", "params": {"sample_count": 1, "depth_mm": 10}})
    
    # Endereço do rover
    rover_addr = ("127.0.0.1", 51000)
    
    # Regista o Rover e o seu endereço (IP, Port) para que o servidor saiba para onde enviar o datagrama
    # Simplificado: usa a tupla (IP, Port)
    ms.register_rover("R-CANCEL", rover_addr)
    ms.assign_mission_to_rover(ms.get_mission(mid), "R-CANCEL")

    server = MLServerProtocol(ms)

    # Cria e anexa o FakeTransport ao servidor
    ft = FakeTransport()
    server.transport = ft
    
    # 2. ENVIAR CANCELAMENTO
    
    # server.send_mission_cancel() retorna o msg_id do pacote enviado.
    msg_id = server.send_mission_cancel(mid, reason="test_cancel")

    # Verifica se o pacote foi enviado pelo FakeTransport
    assert len(ft.sent) == 1, f"Expected one cancel packet sent, got {len(ft.sent)}"

    # 3. VERIFICAR REGISTO PENDENTE
    
    # Verifica se existe um pending_outgoing entry
    found_po = server.pending_outgoing.get(int(msg_id))
    
    assert found_po is not None, "Expected a pending outgoing entry for the sent msg_id"
    # Assumindo que a estrutura interna de PendingOutgoing é mantida (message_type e mission_id)
    assert found_po.message_type == "MISSION_CANCEL"
    assert found_po.mission_id == mid

    # 4. SIMULAR ACK DO CLIENTE
    
    # Cria um ML_ACK datagrama que reconhece o msg_id do servidor
    ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(msg_id)))]
    # Usamos o msgid=0 pois o msgid do ACK não é relevante para a fiabilidade do ACK
    ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, "R-CANCEL", ack_tlvs, msgid=0)
    
    # Entrega ao servidor como se fosse do endereço registado
    server.datagram_received(ack_pkt, rover_addr)

    # 5. ASSERÇÕES FINAIS (LIMPEZA E ESTADO)
    
    # O pacote pendente deve ter sido removido
    assert int(msg_id) not in server.pending_outgoing, "Pending packet was not removed after ACK"

    # A missão deve estar CANCELLED no MissionStore
    m = ms.get_mission(mid)
    assert m is not None
    state = m.get("state")
    
    # Verifica o estado final e a história da missão
    history = m.get("history", [])
    types = [h.get("type") for h in history]
    
    assert state == "CANCELLED", f"Mission state incorrect. Expected CANCELLED, got {state}"
    assert "CANCEL" in types, "Mission history missing CANCEL entry"
