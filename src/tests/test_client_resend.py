from common import binary_proto
from rover.ml_client import SimpleMLClient
import struct
import json
from pathlib import Path

# NOTE: O código original já usa SimpleMLClient, o que está correto.

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
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_MISSION_ASSIGN, rover_id, tlvs, msgid=int(msg_id))
    return pkt


def test_client_resend_and_file_removal(tmp_path, monkeypatch):
    rover_id = "R-TEST"
    mission_id = "M-0001"
    msg_id = 0xA1B2C3D4E5F60001
    
    # --- SETUP: Configurar Cache e Criar Ficheiro Persistido ---
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setenv("ML_CLIENT_CACHE_DIR", str(cache_dir))

    tlvs = []
    tlvs.append((binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8")))
    tlvs.append(binary_proto.tlv_progress(12))

    data = binary_proto.pack_ml_datagram(binary_proto.ML_PROGRESS, rover_id, tlvs, msgid=int(msg_id))

    fname = cache_dir / f"{rover_id}-{int(msg_id)}.bin"
    fname.write_bytes(data)
    assert fname.exists()

    # --- INICIALIZAÇÃO DO CLIENTE ---
    # O __init__ do SimpleMLClient lê a variável de ambiente ML_CLIENT_CACHE_DIR
    proto = SimpleMLClient(rover_id=rover_id)

    ft = FakeTransport()
    proto.transport = ft

    # --- PASSO 1: Resend persisted packet ---
    proto._resend_persisted_packets(("127.0.0.1", 50000))

    # Asserções de envio
    assert len(ft.sent) == 1, f"expected one sendto() call, got {len(ft.sent)}"
    sent_packet, sent_addr = ft.sent[0]
    assert sent_addr == ("127.0.0.1", 50000)
    assert sent_packet == data
    
    # NOVO: Confirma que o ficheiro foi registado em self._persisted_files
    assert int(msg_id) in proto._persisted_files, "Persisted file was not registered internally for cleanup"
    assert proto._persisted_files[int(msg_id)] == fname

    # --- PASSO 2: Simular server ACK for that msg_id ---
    ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(msg_id)))]
    # Usamos o rover ID "SERVER" para garantir que o cliente não tenta processar como seu
    ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, "SERVER", ack_tlvs, msgid=0)

    # Deliver ACK to client (this should remove the persisted file)
    proto.datagram_received(ack_pkt, ("127.0.0.1", 50000))

    # --- Asserções de Limpeza ---
    assert not fname.exists(), "persisted packet file should have been removed after ACK"
    assert int(msg_id) not in proto._persisted_files, "Internal persistence map should be cleared"
