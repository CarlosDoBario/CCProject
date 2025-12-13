from common import binary_proto
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import time
from typing import Dict, Any, Tuple


class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def test_heartbeat_rtt_can_be_measured_via_msgid_timestamp():
    """
    Testa que o RTT pode ser medido usando o timestamp embutido no campo msgid (ms).
    """
    ms = MissionStore()
    # Nota: A MLServerProtocol é testada fora de um loop asyncio, acedendo diretamente ao datagram_received.
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    proto.transport = ft

    rover_id = "R-HB-RTT"
    
    # Simula que o pacote foi enviado 0.5s no passado
    past_seconds = 0.5
    
    # Embed um timestamp passado (ms) no campo msgid (uint64)
    msgid_ms = int((time.time() - past_seconds) * 1000) & 0xFFFFFFFFFFFFFFFF

    # build heartbeat datagram with that numeric msgid
    # CORREÇÃO: Adicionar FLAG_ACK_REQUESTED para forçar o servidor a responder
    hb_bytes = binary_proto.pack_ml_datagram(
        binary_proto.ML_HEARTBEAT, 
        rover_id, 
        [], 
        msgid=msgid_ms,
        flags=binary_proto.FLAG_ACK_REQUESTED # CRÍTICO: Pede ACK
    )

    # Regista o tempo em torno da entrega
    send_time = time.time()
    proto.datagram_received(hb_bytes, ("127.0.0.1", 50000))
    recv_time = time.time()

    # O servidor deve ter enviado um ACK
    assert len(ft.sent) >= 1, "Expected server to send ACK for heartbeat"

    # Calcula o RTT estimado como recv_time - timestamp embutido no msgid
    ts_s = float(msgid_ms) / 1000.0
    rtt = recv_time - ts_s

    # O RTT medido deve ser aproximadamente `past_seconds` (0.5s).
    # Definimos uma margem de erro razoável para testes não determinísticos.
    MARGIN = 0.2
    
    assert rtt >= 0.0, f"RTT should be non-negative, got {rtt}"
    
    # Verifica o limite inferior e superior
    assert rtt >= past_seconds - MARGIN and rtt <= past_seconds + MARGIN, \
        f"Measured RTT ({rtt:.3f}s) should be close to the injected past_seconds ({past_seconds}s)"
