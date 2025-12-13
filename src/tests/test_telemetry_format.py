#!/usr/bin/env python3
"""
tests/test_telemetry_format.py

Unit tests for TS framing and payload parsing (TCP stream).
"""
import random
import pytest
import struct
import json
from typing import Dict, Any, List

from common import binary_proto


def build_ts_frame(rover_id: str, tlvs, msgid: int = None, include_crc: bool = False) -> bytes:
    """
    Constrói um datagrama TS (com prefixo de comprimento de 4 bytes).
    """
    msgid = msgid if msgid is not None else random.getrandbits(48)
    return binary_proto.pack_ts_message(
        binary_proto.TS_TELEMETRY, 
        rover_id, 
        tlvs, 
        msgid=msgid, 
        include_crc=include_crc
    )


def _process_bytes_like_server(stream_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Helper que emula o tratamento de framing do lado do servidor (loop de leitura TCP).
    Consome um buffer de bytes e extrai frames completos (4-byte length + payload).
    Retorna lista de payloads parsed.
    """
    buf = bytearray(stream_bytes)
    parsed: List[Dict[str, Any]] = []
    
    while True:
        # Se não houver bytes suficientes para o prefixo de comprimento (4 bytes), pára.
        if len(buf) < 4:
            break
            
        # Lê o comprimento (L)
        L = int.from_bytes(buf[:4], "big")
        
        # Se o payload completo (L bytes) não estiver disponível, pára.
        if len(buf) < 4 + L:
            break
            
        # Extrai o payload
        payload = bytes(buf[4 : 4 + L])
        
        # Faz o parse e guarda
        parsed.append(binary_proto.parse_ts_payload(payload))
        
        # Remove os bytes consumidos
        del buf[: 4 + L]
        
    # Retorna os payloads parsed. O buffer restante não é retornado (simula o que sobra no buffer TCP)
    return parsed


def test_multiple_frames_single_recv():
    """Testa a decodificação de duas frames concatenadas numa única chamada recv (buffering TCP)."""
    # build two frames
    f1 = build_ts_frame("R-FMT-1", [(binary_proto.TLV_PAYLOAD_JSON, b'{"a":1}')], msgid=1)
    f2 = build_ts_frame("R-FMT-2", [(binary_proto.TLV_PAYLOAD_JSON, b'{"b":2}')], msgid=2)
    combined = f1 + f2

    parsed = _process_bytes_like_server(combined)
    
    assert len(parsed) == 2
    assert parsed[0]["rover_id"] == "R-FMT-1"
    assert parsed[1]["rover_id"] == "R-FMT-2"
    
    # Verifica que o conteúdo JSON foi decodificado corretamente pelo tlv_to_canonical
    canon0 = binary_proto.tlv_to_canonical(parsed[0]["tlvs"])
    canon1 = binary_proto.tlv_to_canonical(parsed[1]["tlvs"])
    
    assert canon0.get("payload_json", {}).get("a") == 1
    assert canon1.get("payload_json", {}).get("b") == 2


def test_fragmented_header_and_payload_parsing():
    """Testa a lógica de buffering da função auxiliar em cenários de fragmentação."""
    # Cria uma frame com payload JSON
    frame = build_ts_frame("R-FRAG", [(binary_proto.TLV_PAYLOAD_JSON, b'{"x": 123, "y": "z"}')], msgid=42)
    
    # Fragmentação arbitrária em 4 chunks para simular TCP
    chunks = [frame[:2], frame[2:10], frame[10:20], frame[20:]]
    
    buf = b""
    parsed = []
    
    # Processa cada chunk, simulando chamadas sucessivas a handler de TCP
    for i, c in enumerate(chunks):
        buf += c
        # Apenas processamos quando for a última chunk (onde a mensagem completa deve estar)
        if i == len(chunks) - 1:
            parsed.extend(_process_bytes_like_server(buf))
            # O processo de parsing deve consumir a frame completa.
            break

    # Se o parsing funcionou, devemos ter exatamente uma mensagem
    assert len(parsed) == 1
    p = parsed[0]
    assert p["rover_id"] == "R-FRAG"
    
    canon = binary_proto.tlv_to_canonical(p["tlvs"])
    assert canon.get("payload_json", {}).get("x") == 123
    assert canon.get("payload_json", {}).get("y") == "z"


def test_crc_trailer_and_mismatch_detection():
    """Testa a inclusão e verificação do CRC32 no payload."""
    # 1. OK: Build a frame with CRC included
    frame_ok = build_ts_frame("R-CRC", [(binary_proto.TLV_PAYLOAD_JSON, b'{"ok":true}')], include_crc=True)
    payload_ok = frame_ok[4:]
    
    # O parse deve ter sucesso
    parsed = binary_proto.parse_ts_payload(payload_ok)
    assert parsed["rover_id"] == "R-CRC"

    # 2. FAIL: Corromper o CRC trailer (últimos 4 bytes)
    corrupted_payload_ba = bytearray(payload_ok)
    corrupted_payload_ba[-1] ^= 0xFF  # Inverte o último byte do trailer CRC
    
    with pytest.raises(ValueError, match="CRC mismatch"):
        binary_proto.parse_ts_payload(bytes(corrupted_payload_ba))


def test_partial_frame_not_yet_ready():
    """Testa se o handler não processa uma frame incompleta."""
    # Build one frame
    frame = build_ts_frame("R-PART", [(binary_proto.TLV_PAYLOAD_JSON, b'{"p":1}')], msgid=99)
    
    # 1. Simular server buffer recebendo apenas prefixo de comprimento + parcial payload
    buf = frame[:len(frame) // 2] 
    
    # helper não deve retornar parsed messages
    parsed = _process_bytes_like_server(buf)
    assert parsed == [], "Server should not process incomplete frame"

    # 2. Após fornecer os bytes restantes, o parsing deve suceder
    buf2 = frame[len(frame) // 2:]
    parsed_full = _process_bytes_like_server(buf + buf2)
    
    assert len(parsed_full) == 1
    assert parsed_full[0]["rover_id"] == "R-PART"
