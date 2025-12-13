from common import binary_proto
from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore
import time
import struct
from typing import Dict, Any, List, Tuple


class FakeTransport:
    def __init__(self):
        self.sent = []

    def sendto(self, packet: bytes, addr):
        # record packet bytes and destination addr
        self.sent.append((packet, addr))

    def get_extra_info(self, name, default=None):
        return None


def build_heartbeat(rover_id: str, msg_id: int):
    """
    Build a binary ML HEARTBEAT datagram using binary_proto.
    CRITICAL: Adds FLAG_ACK_REQUESTED to force server response.
    """
    # empty TLV list is fine for heartbeat
    # CORREÇÃO: Adicionar flags=binary_proto.FLAG_ACK_REQUESTED
    pkt = binary_proto.pack_ml_datagram(
        binary_proto.ML_HEARTBEAT, 
        rover_id, 
        [], 
        msgid=int(msg_id),
        flags=binary_proto.FLAG_ACK_REQUESTED 
    )
    return pkt


def test_server_responds_with_ack_for_heartbeat():
    ms = MissionStore()
    proto = MLServerProtocol(ms)
    ft = FakeTransport()
    # attach fake transport to protocol
    proto.transport = ft

    rover_id = "R-HB"
    # Usar um msg_id previsível
    msg_id = int(time.time() * 1000) & 0xFFFFFFFFFFFFFFFF
    hb_bytes = build_heartbeat(rover_id, msg_id)

    # server receives heartbeat
    # NOTE: O server também irá registar o rover R-HB
    proto.datagram_received(hb_bytes, ("127.0.0.1", 50000))

    # transport should have been used to send an ACK
    assert len(ft.sent) >= 1, "Expected server to send at least one packet (ACK) in response to HEARTBEAT"

    # parse the last sent packet to confirm it's an ACK and acked_msg_id matches
    last_packet, last_addr = ft.sent[-1]
    
    # 1. VERIFICAÇÃO DO PACOTE ENVIADO
    parsed = binary_proto.parse_ml_datagram(last_packet)
    header = parsed.get("header", {})
    tlvs = parsed.get("tlvs", {})

    # Header msgtype should be ML_ACK
    assert header.get("msgtype") == binary_proto.ML_ACK, f"Expected ML_ACK message type, got {header.get('msgtype')}"

    # 2. VERIFICAÇÃO DO TLV_ACKED_MSG_ID
    acked_bytes = tlvs.get(binary_proto.TLV_ACKED_MSG_ID, [])
    assert acked_bytes, "ACK packet missing TLV_ACKED_MSG_ID"
    
    # Desempacotar TLV_ACKED_MSG_ID (uint64)
    try:
        # Usamos [0] porque TLVs são listas de bytes (List[bytes])
        acked_id_bytes = acked_bytes[0]
        # Se os bytes forem menos de 8 (uint64), pode falhar, assumimos 8 bytes aqui
        if len(acked_id_bytes) < 8:
            # Garante que desempacotamos Q (8 bytes) usando right-justification
            acked_id = int.from_bytes(acked_id_bytes.rjust(8, b'\x00'), "big")
        else:
            # Caso ideal (8 bytes)
            acked_id = struct.unpack(">Q", acked_id_bytes)[0]
    except Exception as e:
        raise AssertionError(f"Failed to unpack TLV_ACKED_MSG_ID as >Q: {e}")

    assert int(acked_id) == int(msg_id), f"ACK should acknowledge msg_id {msg_id}, got {acked_id}"

    # 3. VERIFICAÇÃO DO CACHE last_acks (essencial para deduplicação)
    # O MLServer deve armazenar o pacote ACK que enviou, indexado pelo msg_id original.
    stored = proto.last_acks.get(int(msg_id))
    assert stored is not None, "Server.last_acks should contain the ack packet keyed by the original msg_id"
    assert stored == last_packet, "Stored last_acks packet should match the sent ACK bytes"
