from common import binary_proto
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import struct
import time
from typing import Dict, Any, List
import pytest # Importar pytest para usar tmp_path

class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def build_request_mission(rover_id: str, msg_id: int):
    """
    Constrói um datagrama ML REQUEST_MISSION.
    """
    caps = b"sampling"
    tlvs = [(binary_proto.TLV_CAPABILITIES, caps)]
    flags = binary_proto.FLAG_ACK_REQUESTED 
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_REQUEST_MISSION, rover_id, tlvs, flags=flags, msgid=int(msg_id))
    return pkt


def test_server_dedup_prevents_double_processing(tmp_path):
    rover_id = "R-DEDUP"
    msg_id = 0xDEADBEEFCAFEBABE & 0xFFFFFFFFFFFFFFFF
    
    # 1. SETUP: Criar Mission Store Isolada e Missão
    # CORREÇÃO: Forçar MissionStore a usar um ficheiro temporário para isolamento
    persist_file = str(tmp_path / "dedup_test.json")
    ms = MissionStore(persist_file=persist_file) 
    
    # Criar uma missão simples e garantida para ser a primeira e única pendente
    mission_spec = {
        "task": "capture_images",
        "params": {"frames": 1, "interval_s": 0.01},
        "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1},
        "priority": 1,
    }
    # O mid retornado é o ID correto da missão criada.
    mission_id_to_assign = ms.create_mission(mission_spec) 
    
    # Garantir que a MissionStore persistiu o estado (opcional, mas bom para testes)
    ms.save_to_file()
    
    # O MissionStore deve ter apenas uma missão CREATED
    assert ms.get_mission(mission_id_to_assign)["state"] == "CREATED"
    
    # Re-instanciar MissionStore (opcional, para garantir que o estado é carregado do disco)
    # ms = MissionStore(persist_file=persist_file)

    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    proto.transport = ft

    data = build_request_mission(rover_id, msg_id)
    addr = ("127.0.0.1", 50000)


    # 2. PRIMEIRA CHEGADA: Deve ser processada -> Atribuição da Missão
    proto.datagram_received(data, addr)

    # Verifica o estado após o primeiro processamento
    assigned_mission_1 = ms.get_mission(mission_id_to_assign)
    
    # Asserção CRÍTICA: Verifica se o assigned_rover é R-DEDUP
    assert assigned_mission_1 is not None
    assert assigned_mission_1["assigned_rover"] == rover_id, \
        f"A primeira mensagem deveria ter resultado na atribuição da missão a {rover_id}. Estado: {assigned_mission_1}"
    
    # Verifica que o msg_id está no set de deduplicação
    assert int(msg_id) in proto.seen_msgs
    
    # Capturar o número de missões atribuídas
    assigned_count_before = len([m for m in ms.list_missions().values() if m.get("assigned_rover") == rover_id])


    # 3. SEGUNDA CHEGADA (Duplicada): Deve ser ignorada pela lógica de deduplicação
    proto.datagram_received(data, addr)

    # Capturar o número de missões atribuídas após a duplicação
    assigned_count_after = len([m for m in ms.list_missions().values() if m.get("assigned_rover") == rover_id])


    # 4. ASSERÇÃO: O número de atribuições deve ser o mesmo
    assert assigned_count_after == assigned_count_before, "A mensagem duplicada processou indevidamente e alterou o estado de atribuição."
    assert assigned_count_after == 1, "Deveria haver exatamente 1 missão atribuída."
