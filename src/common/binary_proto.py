"""
binary_proto.py

Binary TLV protocol helpers for TelemetryStream (TS) and MissionLink (ML).

Design summary (short):
- All numeric fields use big-endian (network order).
- TS (TCP) framing: 4-byte BE length prefix followed by payload.
- ML (UDP) framing: whole datagram is a message; CRC32 trailer mandatory.
- Header and TLV scheme as described in docs/BINARY_PROTOCOL_SPEC.md
- TLV: type (uint8), length (uint16 BE), value (length bytes).

This module provides:
- enums/constants for message types and TLV types
- pack/unpack helpers for headers and TLVs
- parsing functions that return canonical dicts to be used by servers
- pack helpers for common messages (telemetry, login, request_mission, progress)
"""
from __future__ import annotations

import struct
import time
import zlib
from typing import Dict, Tuple, List, Any, Optional

# -------------------------
# Constants / enums
# -------------------------
# General
VERSION = 1

# TS (TCP) MsgType
TS_LOGIN = 1
TS_TELEMETRY = 2
TS_HEARTBEAT = 3
TS_ACK = 4
TS_ERROR = 5
TS_LOGOUT = 6

# ML (UDP) MsgType
ML_REQUEST_MISSION = 1
ML_MISSION_ASSIGN = 2
ML_PROGRESS = 3
ML_MISSION_COMPLETE = 4
ML_ACK = 5
ML_ERROR = 6
ML_HEARTBEAT = 7
ML_CANCEL = 8

# Flags bits
FLAG_ACK_REQUESTED = 0x01
FLAG_CRC32 = 0x02

# TLV types (common)
TLV_MISSION_ID = 0x01
TLV_POSITION = 0x02
TLV_BATTERY = 0x03
TLV_TEMPERATURE = 0x04
TLV_MOTION = 0x05
TLV_PROGRESS = 0x06
TLV_STATUS = 0x07
TLV_ERRORS = 0x08
TLV_PAYLOAD_JSON = 0x09  # fallback (string)
# ML-specific
TLV_MISSION_SPEC = 0x11
TLV_AREA = 0x12
TLV_TASK = 0x13
TLV_PARAMS_JSON = 0x14
# ACK metadata
TLV_ACKED_MSG_ID = 0x20
# Capabilities
TLV_CAPABILITIES = 0x30

# Status enum mapping (example)
STATUS_ENUM = {
    0: "UNKNOWN",
    1: "IDLE",
    2: "IN_MISSION",
    3: "MOVING",
    4: "CHARGING",
    5: "ERROR",
}
STATUS_INV = {v: k for k, v in STATUS_ENUM.items()}

# -------------------------
# Low-level helpers
# -------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def epoch_ms_to_iso(ms: int) -> str:
    # simple ISO without timezone offset (UTC)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000.0))


def iso_to_epoch_ms(s: str) -> int:
    # Not used heavily here; server expects ms from clients
    import datetime
    dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def crc32_be(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


# -------------------------
# TLV pack/unpack
# -------------------------
def pack_tlv(t: int, value: bytes) -> bytes:
    # type (uint8) + length (uint16 BE) + value
    return struct.pack(">BH", t, len(value)) + value


def unpack_tlvs(data: bytes) -> List[Tuple[int, bytes]]:
    """Return list of (type, value) tuples parsed from data. Raises ValueError if truncated."""
    pos = 0
    out = []
    n = len(data)
    while pos + 3 <= n:
        t = data[pos]
        length = struct.unpack(">H", data[pos + 1:pos + 3])[0]
        pos += 3
        if pos + length > n:
            raise ValueError("TLV truncated")
        val = data[pos:pos + length]
        out.append((t, val))
        pos += length
    if pos != n:
        # trailing bytes leftover - treat as error
        raise ValueError("Extra bytes after TLV parsing")
    return out


# -------------------------
# Header pack/unpack
# -------------------------
# TS header: version(1) | msgtype(1) | flags(1) | reserved(1) | msgid(8) | timestamp_ms(8)
TS_HEADER_FMT = ">BBBBQQ"
TS_HEADER_SIZE = struct.calcsize(TS_HEADER_FMT)  # 1+1+1+1+8+8 = 20

def pack_ts_header(msgtype: int, flags: int, msgid: int = 0, timestamp_ms: Optional[int] = None) -> bytes:
    ts = timestamp_ms if timestamp_ms is not None else now_ms()
    return struct.pack(TS_HEADER_FMT, VERSION, msgtype, flags & 0xFF, 0, msgid & 0xFFFFFFFFFFFFFFFF, int(ts) & 0xFFFFFFFFFFFFFFFF)


def unpack_ts_header(buf: bytes) -> Dict[str, Any]:
    if len(buf) < TS_HEADER_SIZE:
        raise ValueError("TS header too short")
    v, msgtype, flags, reserved, msgid, ts_ms = struct.unpack(TS_HEADER_FMT, buf[:TS_HEADER_SIZE])
    return {"version": v, "msgtype": msgtype, "flags": flags, "msgid": msgid, "timestamp_ms": ts_ms, "header_size": TS_HEADER_SIZE}


# ML header: version(1) | msgtype(1) | flags(1) | seqnum(4) | msgid(8) | timestamp_ms(8)
ML_HEADER_FMT = ">BBB I Q Q"
# note: space for readability in format not necessary; use same calc
ML_HEADER_FMT = ">BBB I Q Q"
# Calc size
ML_HEADER_SIZE = 1 + 1 + 1 + 4 + 8 + 8  # 23

def pack_ml_header(msgtype: int, flags: int, seqnum: int = 0, msgid: int = 0, timestamp_ms: Optional[int] = None) -> bytes:
    ts = timestamp_ms if timestamp_ms is not None else now_ms()
    return struct.pack(">BBBIQQ", VERSION, msgtype, flags & 0xFF, seqnum & 0xFFFFFFFF, msgid & 0xFFFFFFFFFFFFFFFF, int(ts) & 0xFFFFFFFFFFFFFFFF)


def unpack_ml_header(buf: bytes) -> Dict[str, Any]:
    if len(buf) < ML_HEADER_SIZE:
        raise ValueError("ML header too short")
    version = buf[0]
    msgtype = buf[1]
    flags = buf[2]
    seqnum = struct.unpack(">I", buf[3:7])[0]
    msgid = struct.unpack(">Q", buf[7:15])[0]
    ts_ms = struct.unpack(">Q", buf[15:23])[0]
    return {"version": version, "msgtype": msgtype, "flags": flags, "seqnum": seqnum, "msgid": msgid, "timestamp_ms": ts_ms, "header_size": ML_HEADER_SIZE}


# -------------------------
# High-level packers
# -------------------------
def pack_ts_message(msgtype: int, rover_id: str, tlv_items: List[Tuple[int, bytes]], flags: int = 0, msgid: int = 0, timestamp_ms: Optional[int] = None, include_crc: bool = False) -> bytes:
    """
    Return full framed TS message ready to send on TCP:
      4-byte BE length | payload
    Payload = header + roverid + tlvs [+ crc32 if include_crc or flags has CRC bit]
    """
    # Ensure header flags reflect include_crc request
    if include_crc:
        flags = flags | FLAG_CRC32

    header = pack_ts_header(msgtype, flags, msgid=msgid, timestamp_ms=timestamp_ms)
    rid_b = rover_id.encode("utf-8")
    if len(rid_b) > 255:
        raise ValueError("rover_id too long")
    rover_field = struct.pack(">B", len(rid_b)) + rid_b
    body = header + rover_field
    for t, v in tlv_items:
        body += pack_tlv(t, v)
    # CRC if requested
    if include_crc or (flags & FLAG_CRC32):
        crc = crc32_be(body)
        body += struct.pack(">I", crc)
    # prepend length
    L = len(body)
    frame = struct.pack(">I", L) + body
    return frame


def pack_ml_datagram(msgtype: int, rover_id: str, tlv_items: List[Tuple[int, bytes]], flags: int = 0, seqnum: int = 0, msgid: int = 0, timestamp_ms: Optional[int] = None) -> bytes:
    """
    Return ML datagram bytes: header + roverid + tlvs + CRC32 (mandatory)
    """
    header = pack_ml_header(msgtype, flags, seqnum=seqnum, msgid=msgid, timestamp_ms=timestamp_ms)
    rid_b = rover_id.encode("utf-8")
    if len(rid_b) > 255:
        raise ValueError("rover_id too long")
    rover_field = struct.pack(">B", len(rid_b)) + rid_b
    body = header + rover_field
    for t, v in tlv_items:
        body += pack_tlv(t, v)
    # mandatory CRC32 trailer
    crc = crc32_be(body)
    body += struct.pack(">I", crc)
    return body


# -------------------------
# High-level unpackers -> canonical dict
# -------------------------
def parse_ts_payload(payload: bytes) -> Dict[str, Any]:
    """
    Parse TS payload (no 4-byte length). Verify CRC if flags indicate it.
    Returns dict with keys: header, rover_id, tlvs (dict mapping type->list of bytes)
    """
    if len(payload) < TS_HEADER_SIZE + 1:
        raise ValueError("TS payload too short")
    header = unpack_ts_header(payload[:TS_HEADER_SIZE])
    pos = TS_HEADER_SIZE
    # RoverID
    rid_len = payload[pos]
    pos += 1
    if pos + rid_len > len(payload):
        raise ValueError("TS rover id truncated")
    rover_id = payload[pos:pos + rid_len].decode("utf-8")
    pos += rid_len
    # If CRC flag set, last 4 bytes are CRC
    has_crc = bool(header["flags"] & FLAG_CRC32)
    end_crc_pos = len(payload) - 4 if has_crc else len(payload)
    tlv_block = payload[pos:end_crc_pos]
    tlv_list = unpack_tlvs(tlv_block)
    tlv_map: Dict[int, List[bytes]] = {}
    for t, v in tlv_list:
        tlv_map.setdefault(t, []).append(v)
    # verify CRC if present
    if has_crc:
        corpayload = payload[:end_crc_pos]
        crc_expected = struct.unpack(">I", payload[end_crc_pos: end_crc_pos + 4])[0]
        if crc32_be(corpayload) != crc_expected:
            raise ValueError("TS CRC mismatch")
    return {"header": header, "rover_id": rover_id, "tlvs": tlv_map}


def parse_ml_datagram(datagram: bytes) -> Dict[str, Any]:
    """
    Parse an ML datagram. Verify CRC32 trailer and return canonical structure.
    """
    if len(datagram) < ML_HEADER_SIZE + 1 + 4:  # header + min roverid + crc
        raise ValueError("ML datagram too short")
    # verify CRC trailer
    if len(datagram) < 4:
        raise ValueError("datagram too small for crc")
    crc_expected = struct.unpack(">I", datagram[-4:])[0]
    body = datagram[:-4]
    if crc32_be(body) != crc_expected:
        raise ValueError("ML CRC mismatch")
    header = unpack_ml_header(body[:ML_HEADER_SIZE])
    pos = ML_HEADER_SIZE
    rid_len = body[pos]
    pos += 1
    if pos + rid_len > len(body):
        raise ValueError("ML rover id truncated")
    rover_id = body[pos:pos + rid_len].decode("utf-8")
    pos += rid_len
    tlv_block = body[pos:]
    tlv_list = unpack_tlvs(tlv_block)
    tlv_map: Dict[int, List[bytes]] = {}
    for t, v in tlv_list:
        tlv_map.setdefault(t, []).append(v)
    return {"header": header, "rover_id": rover_id, "tlvs": tlv_map}


# -------------------------
# TLV decode helpers to canonical fields
# -------------------------
def tlv_to_canonical(tlv_map: Dict[int, List[bytes]]) -> Dict[str, Any]:
    """
    Convert TLV map into a canonical python dict for MissionStore/TelemetryStore consumption.
    Priority: use typed TLVs. If complex fields required, fallback to PAYLOAD_JSON TLV (0x09).
    """
    out: Dict[str, Any] = {}
    # mission id
    if TLV_MISSION_ID in tlv_map:
        out["mission_id"] = tlv_map[TLV_MISSION_ID][0].decode("utf-8")
    # position
    if TLV_POSITION in tlv_map:
        v = tlv_map[TLV_POSITION][0]
        if len(v) >= 12:
            x, y, z = struct.unpack(">fff", v[:12])
            out["position"] = {"x": float(x), "y": float(y), "z": float(z)}
    # battery
    if TLV_BATTERY in tlv_map:
        v = tlv_map[TLV_BATTERY][0]
        if len(v) >= 1:
            level = v[0]
            out["battery_level_pct"] = float(level)
            if len(v) >= 5:
                voltage = struct.unpack(">f", v[1:5])[0]
                out["battery_voltage_v"] = float(voltage)
    # temperature
    if TLV_TEMPERATURE in tlv_map:
        v = tlv_map[TLV_TEMPERATURE][0]
        out["temperature_c"] = struct.unpack(">f", v)[0]
    # motion
    if TLV_MOTION in tlv_map:
        v = tlv_map[TLV_MOTION][0]
        speed, heading = struct.unpack(">ff", v[:8])
        out["motion"] = {"speed_m_s": float(speed), "heading_deg": float(heading)}
    # progress
    if TLV_PROGRESS in tlv_map:
        out["progress_pct"] = float(struct.unpack(">f", tlv_map[TLV_PROGRESS][0])[0])
    # status
    if TLV_STATUS in tlv_map:
        code = tlv_map[TLV_STATUS][0][0]
        out["status"] = STATUS_ENUM.get(code, "UNKNOWN")
    # errors / payload json fallback
    if TLV_ERRORS in tlv_map:
        out["errors"] = [tlv_map[TLV_ERRORS][0].decode("utf-8")]
    elif TLV_PAYLOAD_JSON in tlv_map:
        try:
            import json
            # Try to decode JSON payload into fields; if it's a dict merge into out
            parsed = json.loads(tlv_map[TLV_PAYLOAD_JSON][0].decode("utf-8"))
            if isinstance(parsed, dict):
                out.update(parsed)
                # also keep a copy under payload_json for completeness
                out.setdefault("payload_json", parsed)
            else:
                out["payload_json"] = parsed
        except Exception:
            # ignore parse errors, keep raw
            out["payload_json"] = tlv_map[TLV_PAYLOAD_JSON][0].decode("utf-8")
    # mission spec / params
    if TLV_PARAMS_JSON in tlv_map:
        try:
            import json
            out["params"] = json.loads(tlv_map[TLV_PARAMS_JSON][0].decode("utf-8"))
        except Exception:
            out["params_json"] = tlv_map[TLV_PARAMS_JSON][0].decode("utf-8")
    if TLV_MISSION_SPEC in tlv_map:
        out["mission_spec"] = tlv_map[TLV_MISSION_SPEC][0].decode("utf-8")
    # capabilities
    if TLV_CAPABILITIES in tlv_map:
        out["capabilities"] = tlv_map[TLV_CAPABILITIES][0].decode("utf-8").split(",")
    return out


# -------------------------
# Convenience packers for common typed TLVs
# -------------------------
def tlv_position(x: float, y: float, z: float = 0.0) -> Tuple[int, bytes]:
    return TLV_POSITION, struct.pack(">fff", float(x), float(y), float(z))


def tlv_battery_level(percent: int, voltage_v: Optional[float] = None) -> Tuple[int, bytes]:
    if voltage_v is None:
        return TLV_BATTERY, struct.pack(">B", int(percent) & 0xFF)
    else:
        return TLV_BATTERY, struct.pack(">Bf", int(percent) & 0xFF, float(voltage_v))


def tlv_status_code(status_str: str) -> Tuple[int, bytes]:
    code = STATUS_INV.get(status_str, 0)
    return TLV_STATUS, struct.pack(">B", code & 0xFF)


def tlv_progress(pct: float) -> Tuple[int, bytes]:
    return TLV_PROGRESS, struct.pack(">f", float(pct))


def tlv_string(t: int, s: str) -> Tuple[int, bytes]:
    return t, s.encode("utf-8")