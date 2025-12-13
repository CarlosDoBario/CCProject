from common import binary_proto
from rover.ml_client import SimpleMLClient
import struct
import json
from common import config # Necessário para o default port

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def build_mission_assign(rover_id: str, mission_id: str, msg_id: int):
    tlvs = []
    tlvs.append((binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8")))
    # include params json to mimic mission payload
    tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps({"task": "capture_images", "params": {"interval_s": 0.1, "frames": 1}}).encode("utf-8")))
    # O msgid é embalado como Q (uint64)
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_MISSION_ASSIGN, rover_id, tlvs, msgid=int(msg_id))
    return pkt


def test_client_dedup_resends_ack_on_duplicate():
    rover_id = "R-CLIENT-DEDUP"
    # CORRIGIDO: Usar SimpleMLClient
    proto = SimpleMLClient(rover_id=rover_id)
    ft = FakeTransport()
    proto.transport = ft

    mission_id = "M-DEDUP-1"
    msg_id = 0xDEADBEAF0001 # MsgID de entrada

    data = build_mission_assign(rover_id, mission_id, msg_id)
    addr = ("127.0.0.1", 50000)

    # 1. Primeira chegada: o cliente deve processar e enviar o ACK
    proto.datagram_received(data, addr)
    
    # Após a primeira chegada: 1x ACK (no índice 0 da lista sent)
    assert len(ft.sent) == 1, "Expected exactly one sendto (ACK) on first assign"
    
    # Capturar os bytes do ACK enviado para comparação
    first_ack = ft.sent[0][0]
    
    # 2. Segunda chegada (duplicada): o cliente deve reenviar o mesmo ACK
    proto.datagram_received(data, addr)
    
    # Após a segunda chegada: deve haver 2 ACK no total.
    assert len(ft.sent) == 2, "Expected a single resend ACK on duplicate assign"

    second_ack = ft.sent[-1][0]
    assert first_ack == second_ack, "Expected duplicated ACK bytes to match original ACK"
    
    # 3. Verifica se o msg_id foi marcado como visto para deduplicação
    assert int(msg_id) in proto.seen_msgs
