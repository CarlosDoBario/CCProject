#!/usr/bin/env python3
"""
ml_schema.py (binary ML helpers)

Compat layer for MissionLink (ML) adapted to the binary TLV protocol.

This module preserves a small compatibility surface (functions named similarly to
the original JSON envelope helpers) but implements packing/unpacking using the
binary_proto TLV format used across the project.

Provided helpers:
- PROTOCOL, MAX_DATAGRAM_SIZE
- MESSAGE_TYPES (string set retained for compatibility)
- build_envelope(...) -> returns an envelope-like dict (header/body)
- envelope_to_bytes(envelope) -> bytes (binary ML datagram)
- parse_envelope(data: bytes) -> envelope-like dict (header/body) parsed from binary datagram
- make_ack(acked_msg_id, rover_id, mission_id, seq) -> envelope dict
- make_ack_bytes(...) -> bytes datagram (convenience)

Notes:
- The ML wire format uses binary_proto.pack_ml_datagram / parse_ml_datagram.
- This module maps high-level fields in the legacy JSON envelope to TLVs.
- For mission_spec heavy payloads we prefer PARAMS_JSON TLV (JSON encoded string).
"""
from __future__ import annotations

import json
import struct
import time
from typing import Any, Dict, Optional, Tuple, List

from common import binary_proto, utils, config
from common import mission_schema

# Protocol identifier (kept for compatibility)
PROTOCOL = "ML/1.0"
MAX_DATAGRAM_SIZE = config.ML_MAX_DATAGRAM_SIZE if hasattr(config, "ML_MAX_DATAGRAM_SIZE") else 1200

# Legacy message type names (kept for compatibility)
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

# Map string names to numeric codes used on the wire
MSGTYPE_STR_TO_CODE = {
    "REQUEST_MISSION": binary_proto.ML_REQUEST_MISSION,
    "MISSION_ASSIGN": binary_proto.ML_MISSION_ASSIGN,
    "ACK": binary_proto.ML_ACK,
    "PROGRESS": binary_proto.ML_PROGRESS,
    "MISSION_COMPLETE": binary_proto.ML_MISSION_COMPLETE,
    "MISSION_CANCEL": binary_proto.ML_CANCEL,
    "ERROR": binary_proto.ML_ERROR,
    "HEARTBEAT": binary_proto.ML_HEARTBEAT,
}
MSGTYPE_CODE_TO_STR = {v: k for k, v in MSGTYPE_STR_TO_CODE.items()}


# -------------------------
# Helpers to build envelope-like dicts (compat)
# -------------------------
def _now_iso() -> str:
    return utils.now_iso()


def _now_ms() -> int:
    return binary_proto.now_ms()


def build_header(
    message_type: str,
    rover_id: Optional[str] = None,
    mission_id: Optional[str] = None,
    seq: int = 0,
    msg_id: Optional[str] = None,
    timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a compatibility header dict similar to the old JSON envelope.
    msg_id if None -> generated as an integer (epoch ms based) for simpler mapping to wire msgid.
    """
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"Invalid message_type: {message_type}")
    ts_iso = timestamp or _now_iso()
    if msg_id is None:
        # use epoch ms string as id for easier correlation; callers may pass 0 or explicit numeric msgid
        msg_id = str(int(_now_ms()))
    header = {
        "protocol": PROTOCOL,
        "message_type": message_type,
        "msg_id": msg_id,
        "seq": int(seq),
        "timestamp": ts_iso,
        "rover_id": rover_id,
        "mission_id": mission_id,
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
    Build an envelope-like dict (header + body). Body is a plain dict that will be
    mapped to TLVs by envelope_to_bytes when serializing.
    """
    if body is None:
        body = {}
    header = build_header(message_type=message_type, rover_id=rover_id, mission_id=mission_id, seq=seq, msg_id=msg_id)
    return {"header": header, "body": body}


# -------------------------
# Convert envelope -> binary datagram
# -------------------------
def envelope_to_bytes(envelope: Dict[str, Any]) -> bytes:
    """
    Convert an envelope (as returned by build_envelope) to a binary ML datagram (bytes).
    The conversion maps well-known body fields into TLVs:
      - capabilities -> TLV_CAPABILITIES (CSV string)
      - position -> TLV_POSITION (3*float32) if body['position'] is present
      - battery_level_pct -> TLV_BATTERY (uint8 or uint8+float voltage)
      - progress_pct -> TLV_PROGRESS (float32)
      - mission_id -> TLV_MISSION_ID (string)
      - params (in mission assign) -> TLV_PARAMS_JSON (JSON string)
    """
    header = envelope.get("header", {})
    body = envelope.get("body", {}) or {}
    mtype = header.get("message_type")
    if mtype not in MESSAGE_TYPES:
        raise ValueError(f"Unsupported message_type: {mtype}")

    # Choose numeric msgtype
    code = MSGTYPE_STR_TO_CODE.get(mtype)
    if code is None:
        raise ValueError(f"No wire mapping for message_type {mtype}")

    # msgid numeric: try to coerce header.msg_id into uint64; fallback to epoch ms
    try:
        msgid_num = int(str(header.get("msg_id") or int(_now_ms())))
    except Exception:
        msgid_num = int(_now_ms())

    seqnum = int(header.get("seq", 0))

    # Build TLVs according to message type
    tlvs: List[Tuple[int, bytes]] = []

    # common mission_id if present
    if body.get("mission_id"):
        tlvs.append((binary_proto.TLV_MISSION_ID, str(body.get("mission_id")).encode("utf-8")))

    # Specific handling per message type
    if mtype == "REQUEST_MISSION":
        caps = body.get("capabilities") or header.get("capabilities")
        if caps:
            # capabilities can be list or comma string
            if isinstance(caps, list):
                caps_s = ",".join(map(str, caps))
            else:
                caps_s = str(caps)
            tlvs.append((binary_proto.TLV_CAPABILITIES, caps_s.encode("utf-8")))
        # include optional status/position for richer request
        if body.get("position"):
            p = body["position"]
            tlvs.append(binary_proto.tlv_position(p.get("x", 0.0), p.get("y", 0.0), p.get("z", 0.0)))
        if body.get("battery_level_pct") is not None:
            tlvs.append(binary_proto.tlv_battery_level(int(body.get("battery_level_pct", 0))))
    elif mtype == "MISSION_ASSIGN":
        # Expect body to contain mission details (mission_id, task, params, area)
        mission_spec = body.get("mission") or body.get("mission_spec") or body
        # If mission_spec is a dict, use mission_schema helper to TLVs
        if isinstance(mission_spec, dict):
            spec_tlvs = mission_schema.mission_spec_to_tlvs(mission_spec)
            tlvs.extend(spec_tlvs)
        else:
            # fallback: put mission_spec as JSON string under TLV_PARAMS_JSON
            try:
                tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(mission_spec).encode("utf-8")))
            except Exception:
                tlvs.append((binary_proto.TLV_PARAMS_JSON, str(mission_spec).encode("utf-8")))
    elif mtype in ("PROGRESS", "MISSION_COMPLETE"):
        # progress/complete: include progress, position, battery, status, params if present
        if body.get("progress_pct") is not None:
            tlvs.append(binary_proto.tlv_progress(float(body.get("progress_pct"))))
        if body.get("position"):
            p = body["position"]
            tlvs.append(binary_proto.tlv_position(p.get("x", 0.0), p.get("y", 0.0), p.get("z", 0.0)))
        if body.get("battery_level_pct") is not None:
            tlvs.append(binary_proto.tlv_battery_level(int(body.get("battery_level_pct", 0))))
        if body.get("status"):
            tlvs.append(binary_proto.tlv_status_code(str(body.get("status"))))
        # include any params as JSON fallback if present
        if body.get("params") is not None:
            try:
                tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(body.get("params")).encode("utf-8")))
            except Exception:
                tlvs.append((binary_proto.TLV_PARAMS_JSON, str(body.get("params")).encode("utf-8")))
    elif mtype == "ACK":
        acked = body.get("acked_msg_id")
        if acked is None:
            # try header mission_id as fallback
            acked = header.get("msg_id")
        try:
            ack_num = int(str(acked))
        except Exception:
            ack_num = int(_now_ms())
        tlvs.append((binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(ack_num) & 0xFFFFFFFFFFFFFFFF)))
    elif mtype == "ERROR":
        desc = body.get("description") or body.get("error") or str(body)
        tlvs.append((binary_proto.TLV_ERRORS, str(desc).encode("utf-8")))
    elif mtype == "HEARTBEAT":
        # HEARTBEAT may carry an optional nonce/diagnostic; put it in PAYLOAD_JSON for clarity
        if body.get("nonce"):
            try:
                # If nonce is a JSON-serializable object, encode as JSON; else stringify
                tlvs.append((binary_proto.TLV_PAYLOAD_JSON, json.dumps(body.get("nonce")).encode("utf-8")))
            except Exception:
                tlvs.append((binary_proto.TLV_PAYLOAD_JSON, str(body.get("nonce")).encode("utf-8")))

    # Build datagram: use flags (ack requested?) from body['flags'] if present
    flags = 0
    if body.get("ack_requested"):
        flags |= binary_proto.FLAG_ACK_REQUESTED

    datagram = binary_proto.pack_ml_datagram(code, str(header.get("rover_id") or ""), tlvs, flags=flags, seqnum=seqnum, msgid=msgid_num)
    if len(datagram) > MAX_DATAGRAM_SIZE:
        raise ValueError(f"Datagram size {len(datagram)} > MAX_DATAGRAM_SIZE ({MAX_DATAGRAM_SIZE})")
    return datagram


# -------------------------
# Parse binary datagram -> envelope-like dict
# -------------------------
def parse_envelope(data: bytes) -> Dict[str, Any]:
    """
    Parses binary ML datagram and returns an envelope-like dict:
      {"header": {...}, "body": {...}}
    header contains: protocol, message_type (string), msg_id, seq, timestamp (ms), rover_id, mission_id (if present)
    body is a canonical dict produced by binary_proto.tlv_to_canonical(...)
    Raises ValueError on parse / CRC errors.
    """
    try:
        parsed = binary_proto.parse_ml_datagram(data)
    except Exception as e:
        raise ValueError(f"Failed to parse ML datagram: {e}")

    header_raw = parsed.get("header", {})
    code = header_raw.get("msgtype")
    mtype = MSGTYPE_CODE_TO_STR.get(code, f"UNKNOWN_{code}")
    header = {
        "protocol": PROTOCOL,
        "message_type": mtype,
        "msg_id": str(header_raw.get("msgid")),
        "seq": int(header_raw.get("seqnum", 0)),
        "timestamp": int(header_raw.get("timestamp_ms", int(_now_ms()))),
        "rover_id": parsed.get("rover_id"),
        "mission_id": None,
    }
    tlv_map = parsed.get("tlvs", {})
    body = binary_proto.tlv_to_canonical(tlv_map)

    # if mission_id present in TLVs, copy to header.mission_id
    if body.get("mission_id"):
        header["mission_id"] = body.get("mission_id")

    envelope = {"header": header, "body": body}
    return envelope


# Convenience ACK helpers
def make_ack(acked_msg_id: str, rover_id: Optional[str] = None, mission_id: Optional[str] = None, seq: int = 0) -> Dict[str, Any]:
    body = {"acked_msg_id": acked_msg_id, "status": "ok", "reason": ""}
    header = build_header("ACK", rover_id=rover_id, mission_id=mission_id, seq=seq, msg_id=str(int(_now_ms())))
    return {"header": header, "body": body}


def make_ack_bytes(acked_msg_id: str, rover_id: Optional[str] = None, mission_id: Optional[str] = None, seq: int = 0) -> bytes:
    env = make_ack(acked_msg_id=acked_msg_id, rover_id=rover_id, mission_id=mission_id, seq=seq)
    return envelope_to_bytes(env)


# Small convenience builders (used by server/client)
def build_request_mission_bytes(rover_id: str, capabilities: Optional[List[str]] = None, seq: int = 0, msgid: Optional[int] = None) -> bytes:
    body = {"capabilities": capabilities or []}
    env = build_envelope("REQUEST_MISSION", body=body, rover_id=rover_id, seq=seq, msg_id=str(int(msgid) if msgid is not None else int(_now_ms())))
    return envelope_to_bytes(env)


def build_mission_assign_bytes(rover_id: str, mission_spec: Dict[str, Any], seq: int = 0, msgid: Optional[int] = None) -> bytes:
    # mission_spec is dictionary: convert to body with 'mission' field for compatibility
    body = {"mission": mission_spec}
    env = build_envelope("MISSION_ASSIGN", body=body, rover_id=rover_id, seq=seq, msg_id=str(int(msgid) if msgid is not None else int(_now_ms())))
    return envelope_to_bytes(env)


# Exported symbols
__all__ = [
    "PROTOCOL",
    "MAX_DATAGRAM_SIZE",
    "MESSAGE_TYPES",
    "build_envelope",
    "envelope_to_bytes",
    "parse_envelope",
    "make_ack",
    "make_ack_bytes",
    "build_request_mission_bytes",
    "build_mission_assign_bytes",
]