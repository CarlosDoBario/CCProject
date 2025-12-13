#!/usr/bin/env python3
import json
import pytest
from typing import Dict, Any

from common import binary_proto, mission_schema


def test_checksum_roundtrip():
    """
    Testa o empacotamento/desempacotamento ML (UDP), verificando o msgid e
    a integridade do payload (o que implicitamente valida o CRC32).
    """
    # Payload de teste que será serializado em JSON TLV
    original_body = {"a": 1, "b": "x", "timestamp_ms": binary_proto.now_ms()}
    
    tlvs = [(binary_proto.TLV_PAYLOAD_JSON, json.dumps(original_body).encode("utf-8"))]
    msgid = 0x12345678ABCDEF 
    
    # 1. PACK: Empacotar o datagrama ML (inclui CRC32 obrigatório)
    pkt = binary_proto.pack_ml_datagram(binary_proto.ML_REQUEST_MISSION, "R-TEST", tlvs, msgid=msgid)
    
    # 2. PARSE: Desempacotar e verificar CRC
    parsed: Dict[str, Any] = binary_proto.parse_ml_datagram(pkt)
    
    # 3. VERIFICAÇÃO 1: MsgID do Header
    parsed_msgid = int(parsed["header"].get("msgid", 0))
    assert parsed_msgid == int(msgid), f"Expected MsgID {msgid}, got {parsed_msgid}"
    
    # 4. VERIFICAÇÃO 2: Conteúdo do Payload (TLV)
    # Converter TLVs para formato canónico. O tlv_to_canonical deve extrair o PAYLOAD_JSON
    canonical = binary_proto.tlv_to_canonical(parsed["tlvs"])
    
    # O PAYLOAD_JSON é devolvido dentro da chave 'payload_json'
    assert 'payload_json' in canonical
    
    # Como enviamos um único PAYLOAD_JSON que é um dict, esperamos o dict de volta
    assert canonical['payload_json'] == original_body


def test_validate_capture_images_ok():
    """Verifica se uma especificação de missão 'capture_images' válida passa na validação."""
    m = {
        "task": "capture_images", 
        "params": {"interval_s": 5, "frames": 10}, 
        "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1}
    }
    ok, errs = mission_schema.validate_mission_spec(m)
    
    assert ok, f"Expected valid mission spec, got errors: {errs}"
    assert len(errs) == 0


def test_validate_capture_images_missing():
    """Verifica se uma especificação de missão 'capture_images' incompleta falha."""
    m = {"task": "capture_images", "params": {}, "area": {"x1": 0, "y1": 0, "x2": 1, "y2": 1}}
    
    ok, errs = mission_schema.validate_mission_spec(m)
    
    assert not ok
    assert len(errs) > 0
    
    # Espera que a validação mencione explicitamente os campos em falta.
    # Pode haver mais de um erro por validação, usamos `any`.
    error_messages = [str(e) for e in errs]
    assert any("interval_s" in e for e in error_messages), "Validation failed to report missing 'interval_s'."
    assert any("frames" in e for e in error_messages), "Validation failed to report missing 'frames'."
