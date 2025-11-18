#!/usr/bin/env python3
"""
Simple binary TelemetryStream (TS) client for rovers.

Sends periodic telemetry messages over TCP using the binary TLV framing.
Improvements:
 - uses config defaults
 - optional --crc flag to include CRC32
 - waits for ACK with a timeout when ack_requested (to avoid hanging)
"""
import argparse
import asyncio
import logging
import random
import time
import struct

from common import binary_proto, utils, config

logger = utils.get_logger("rover.telemetry_client")


async def send_once(host: str, port: int, rover_id: str, telemetry: dict, ack_requested: bool = False, ack_timeout: float = 5.0, include_crc: bool = False):
    reader, writer = await asyncio.open_connection(host, port)
    try:
        # convert telemetry dict to TLVs
        tlvs = []
        pos = telemetry.get("position")
        if pos:
            tlvs.append(binary_proto.tlv_position(pos.get("x", 0.0), pos.get("y", 0.0), pos.get("z", 0.0)))
        if "battery_level_pct" in telemetry:
            tlvs.append(binary_proto.tlv_battery_level(int(telemetry.get("battery_level_pct", 0))))
        if "progress_pct" in telemetry:
            tlvs.append(binary_proto.tlv_progress(float(telemetry.get("progress_pct", 0.0))))
        if "status" in telemetry:
            tlvs.append(binary_proto.tlv_status_code(telemetry.get("status", "UNKNOWN")))
        # include other fields as PAYLOAD_JSON TLV so nothing is lost compared to old JSON
        import json
        tlvs.append((binary_proto.TLV_PAYLOAD_JSON, json.dumps(telemetry, ensure_ascii=False).encode("utf-8")))

        flags = binary_proto.FLAG_ACK_REQUESTED if ack_requested else 0
        if include_crc:
            flags |= binary_proto.FLAG_CRC32

        msgid = random.getrandbits(48)
        frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, flags=flags, msgid=msgid, include_crc=bool(flags & binary_proto.FLAG_CRC32))
        writer.write(frame)
        await writer.drain()

        # optional: wait for ACK if ack_requested
        if ack_requested:
            try:
                hdr = await asyncio.wait_for(reader.readexactly(4), timeout=ack_timeout)
                L = int.from_bytes(hdr, "big")
                payload = await asyncio.wait_for(reader.readexactly(L), timeout=ack_timeout)
                try:
                    parsed = binary_proto.parse_ts_payload(payload)
                    if parsed["header"]["msgtype"] == binary_proto.TS_ACK:
                        logger.info("Received ACK from server")
                except Exception:
                    logger.warning("Received non-ACK or malformed response")
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for ACK from server")
            except Exception:
                logger.exception("Error while waiting for ACK")

    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_periodic(host: str, port: int, rover_id: str, interval_s: float):
    while True:
        telemetry = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": random.uniform(-10, 10), "y": random.uniform(-10, 10), "z": 0.0},
            "battery_level_pct": random.randint(20, 100),
            "status": "IN_MISSION",
            "progress_pct": random.uniform(0, 100),
        }
        try:
            await send_once(host, port, rover_id, telemetry, ack_requested=False, include_crc=False)
            logger.info("Sent telemetry for %s", rover_id)
        except Exception:
            logger.exception("Failed sending telemetry")
        await asyncio.sleep(interval_s)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=config.TELEMETRY_HOST)
    parser.add_argument("--port", type=int, default=config.TELEMETRY_PORT)
    parser.add_argument("--rover-id", required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=float, default=config.DEFAULT_UPDATE_INTERVAL_S)
    parser.add_argument("--ack", action="store_true", help="request ACK from server (wait briefly)")
    parser.add_argument("--ack-timeout", type=float, default=5.0)
    parser.add_argument("--crc", action="store_true", help="include CRC32 trailer in TS frame")
    args = parser.parse_args()

    if args.once:
        asyncio.run(send_once(args.host, args.port, args.rover_id, {"position": {"x": 0.0, "y": 0.0, "z": 0.0}, "battery_level_pct": 100, "status": "IDLE", "progress_pct": 0.0}, ack_requested=args.ack, ack_timeout=args.ack_timeout, include_crc=args.crc))
    else:
        try:
            asyncio.run(run_periodic(args.host, args.port, args.rover_id, args.interval))
        except KeyboardInterrupt:
            logger.info("Telemetry client exiting")


if __name__ == "__main__":
    main()