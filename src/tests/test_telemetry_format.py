#!/usr/bin/env python3
"""
tests/test_telemetry_format.py

Unit tests for TS framing and payload parsing.

Covers:
 - multiple frames concatenated in one recv
 - fragmented header (partial 4-byte length)
 - fragmented payload (split after header)
 - CRC presence and CRC mismatch detection
"""
import random
import pytest

from common import binary_proto


def build_ts_frame(rover_id: str, tlvs, msgid: int = None, include_crc: bool = False):
    msgid = msgid if msgid is not None else random.getrandbits(48)
    # ensure we request CRC in header when include_crc True
    return binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, msgid=msgid, include_crc=include_crc)


def _process_bytes_like_server(stream_bytes: bytes):
    """
    Simple helper that emulates server-side framing handling:
    consume a bytes buffer, extract frames (4-byte length + payload) and parse payloads with parse_ts_payload.
    Returns list of parsed payload dicts.
    """
    buf = bytearray(stream_bytes)
    parsed = []
    while True:
        if len(buf) < 4:
            break
        L = int.from_bytes(buf[:4], "big")
        if len(buf) < 4 + L:
            break
        payload = bytes(buf[4 : 4 + L])
        parsed.append(binary_proto.parse_ts_payload(payload))
        # remove consumed bytes
        del buf[: 4 + L]
    return parsed


def test_multiple_frames_single_recv():
    # build two frames
    f1 = build_ts_frame("R-FMT-1", [(binary_proto.TLV_PAYLOAD_JSON, b'{"a":1}')], msgid=1)
    f2 = build_ts_frame("R-FMT-2", [(binary_proto.TLV_PAYLOAD_JSON, b'{"b":2}')], msgid=2)
    combined = f1 + f2

    parsed = _process_bytes_like_server(combined)
    assert len(parsed) == 2
    assert parsed[0]["rover_id"] == "R-FMT-1"
    assert parsed[1]["rover_id"] == "R-FMT-2"
    # check canonical conversion yields payload json included
    canon0 = binary_proto.tlv_to_canonical(parsed[0]["tlvs"])
    canon1 = binary_proto.tlv_to_canonical(parsed[1]["tlvs"])
    assert canon0.get("payload_json") is not None or "a" in str(canon0)
    assert canon1.get("payload_json") is not None or "b" in str(canon1)


def test_fragmented_header_and_payload_parsing():
    # create one frame and then split it across several recv chunks emulating TCP fragmentation
    frame = build_ts_frame("R-FRAG", [(binary_proto.TLV_PAYLOAD_JSON, b'{"x": 123, "y": "z"}')], msgid=42)
    # split positions: first 2 bytes of header, then remaining header + first part of payload, then rest
    chunks = [frame[:2], frame[2:10], frame[10:20], frame[20:]]  # arbitrary fragmentation
    # feed chunks into a rolling buffer like a server would
    buf = b""
    parsed = []
    for c in chunks:
        buf += c
        parsed.extend(_process_bytes_like_server(buf))
        if parsed:
            # compute bytes consumed by the first parsed frame and remove from buffer
            hdr = int.from_bytes(frame[:4], "big")
            consumed = 4 + hdr
            buf = buf[consumed:]
            break

    # If parsing worked, we should have one parsed message
    assert len(parsed) >= 1
    p = parsed[0]
    assert p["rover_id"] == "R-FRAG"
    canon = binary_proto.tlv_to_canonical(p["tlvs"])
    # canonical may either unpack payload_json into fields or leave payload in payload_json key
    assert canon.get("payload_json") is not None or ("x" in canon and "y" in canon)


def test_crc_trailer_and_mismatch_detection():
    # Build a frame with CRC included (include_crc True)
    frame_ok = build_ts_frame("R-CRC", [(binary_proto.TLV_PAYLOAD_JSON, b'{"ok":true}')], include_crc=True)
    # The parse should succeed for the payload portion (strip the 4-byte length first)
    payload_ok = frame_ok[4:]
    parsed = binary_proto.parse_ts_payload(payload_ok)
    assert parsed["rover_id"] == "R-CRC"

    # Now corrupt the CRC trailer (last 4 bytes)
    corrupted = bytearray(payload_ok)
    corrupted[-1] ^= 0xFF  # flip last byte to break CRC
    with pytest.raises(ValueError):
        binary_proto.parse_ts_payload(bytes(corrupted))


def test_partial_frame_not_yet_ready():
    # Build one frame and give only length prefix + partial payload -> parser not invoked by server until full payload available
    frame = build_ts_frame("R-PART", [(binary_proto.TLV_PAYLOAD_JSON, b'{"p":1}')], msgid=99)
    # simulate server buffer receiving only first 6 bytes (4 length + 2 bytes of payload)
    buf = frame[:6]
    # helper should return no parsed messages
    parsed = _process_bytes_like_server(buf)
    assert parsed == []  # no full frame yet

    # after providing remaining bytes, parsing should succeed
    buf2 = frame[6:]
    parsed_full = _process_bytes_like_server(buf + buf2)
    assert len(parsed_full) == 1
    assert parsed_full[0]["rover_id"] == "R-PART"