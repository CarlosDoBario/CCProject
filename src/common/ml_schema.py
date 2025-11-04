"""
ml_schema.py
Helpers para construir, serializar, desserializar e validar envelopes ML (MissionLink).
- Serialização determinística do body para cálculo de checksum (SHA1).
- Construção de header + body.
- Conversão envelope <-> bytes (JSON compact).
- Validações básicas.
"""

import json
import hashlib
import uuid
from typing import Any, Dict, Optional

PROTOCOL = "ML/1.0"
MAX_DATAGRAM_SIZE = 1200  # para evitar fragmentação

# Tipos de mensagem permitidos
MESSAGE_TYPES = {
    "REQUEST_MISSION",
    "MISSION_ASSIGN",
    "ACK",
    "PROGRESS",
    "MISSION_COMPLETE",
    "MISSION_CANCEL",
    "ERROR",
    "HEARTBEAT",
}

"""
Serializar JSON de modo determinístico:
- sem espaços desnecessários (separators)
- chaves ordenadas (sort_keys=True)
Garante que o checksum seja consistente entre sender e receiver.
"""
def _stable_json_dumps(obj: Any) -> str:
  return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)

"""
Calcula SHA1 (hex) sobre a serialização determinística do body.
"""
def body_checksum(body: Dict[str, Any]) -> str:
    body_bytes = _stable_json_dumps(body).encode("utf-8")
    h = hashlib.sha1()
    h.update(body_bytes)
    return h.hexdigest()

def build_header(
    message_type: str,
    rover_id: Optional[str] = None,
    mission_id: Optional[str] = None,
    seq: int = 0,
    msg_id: Optional[str] = None,
    checksum: str = "",
) -> Dict[str, Any]:
    """
    Constrói o objecto header com os campos obrigatórios.
    - msg_id é gerado se None.
    - timestamp é gerado aqui em formato ISO8601 (UTC).
    """
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"Invalid message_type: {message_type}")

    if msg_id is None:
        msg_id = str(uuid.uuid4())

    from datetime import datetime, timezone

    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    header = {
        "protocol": PROTOCOL,
        "message_type": message_type,
        "msg_id": msg_id,
        "seq": seq,
        "timestamp": timestamp,
        "rover_id": rover_id,
        "mission_id": mission_id,
        "checksum": checksum,
    }
    return header


def build_envelope(
    message_type: str,
    body: Optional[Dict[str, Any]] = None,
    rover_id: Optional[str] = None,
    mission_id: Optional[str] = None,
    seq: int = 0,
    msg_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Constrói o envelope ML completo (header + body) e calcula checksum sobre o body.
    """
    if body is None:
        body = {}

    checksum = body_checksum(body) if body else ""
    header = build_header(
        message_type=message_type,
        rover_id=rover_id,
        mission_id=mission_id,
        seq=seq,
        msg_id=msg_id,
        checksum=checksum,
    )
    envelope = {"header": header, "body": body}
    return envelope


def envelope_to_bytes(envelope: Dict[str, Any]) -> bytes:
    """
    Serializa envelope para bytes (JSON compactado). Garante tamanho inferior ao limite.
    """
    s = _stable_json_dumps(envelope)
    b = s.encode("utf-8")
    if len(b) > MAX_DATAGRAM_SIZE:
        raise ValueError(
            f"Envelope size {len(b)} > MAX_DATAGRAM_SIZE ({MAX_DATAGRAM_SIZE}). Consider reducing payload."
        )
    return b


def parse_envelope(data: bytes) -> Dict[str, Any]:
    """
    Desserializa bytes para envelope dict e realiza validações básicas:
    - header presente e com campos obrigatórios
    - protocol correto
    - message_type válido
    - checksum válido (se body não vazio)
    Retorna o envelope dict (header + body). Lança ValueError em caso de erro.
    """
    try:
        text = data.decode("utf-8")
    except Exception as e:
        raise ValueError(f"Invalid UTF-8 data: {e}")

    try:
        envelope = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

    if "header" not in envelope or "body" not in envelope:
        raise ValueError("Envelope must contain 'header' and 'body'")

    header = envelope["header"]
    body = envelope["body"]

    # Basic header checks
    if header.get("protocol") != PROTOCOL:
        raise ValueError(f"Unsupported protocol: {header.get('protocol')}")
    if header.get("message_type") not in MESSAGE_TYPES:
        raise ValueError(f"Invalid message_type: {header.get('message_type')}")
    if not header.get("msg_id"):
        raise ValueError("Missing header.msg_id")
    if "seq" not in header:
        raise ValueError("Missing header.seq")
    # checksum validation
    expected = header.get("checksum", "")
    actual = body_checksum(body) if body else ""
    if expected != actual:
        raise ValueError("Checksum mismatch")

    return envelope


# Utility small helper to create an ACK envelope
def make_ack(acked_msg_id: str, rover_id: Optional[str] = None, mission_id: Optional[str] = None, seq: int = 0) -> Dict[str, Any]:
    body = {"acked_msg_id": acked_msg_id, "status": "ok", "reason": ""}
    env = build_envelope("ACK", body=body, rover_id=rover_id, mission_id=mission_id, seq=seq)
    return env