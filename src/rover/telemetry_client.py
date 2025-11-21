#!/usr/bin/env python3
"""
Telemetry client for rovers (binary TLV over TCP).

Provides:
- send_once(...) compatibility function (keeps previous behaviour)
- TelemetryClient: persistent client with automatic reconnect/backoff,
  reader loop to receive server frames (commands/acks), and a simple
  on_command(callback) hook for handling server-issued commands.

This file implements a complete CLI entrypoint that accepts:
  --host, --port, --rover-id, --once, --interval, --ack, --ack-timeout, --crc
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import random
import time
import struct
import json
from typing import Optional, Callable, Any, Dict

from common import binary_proto, utils, config

logger = utils.get_logger("rover.telemetry_client")

# Keep the old simple send_once API for compatibility with tests/scripts
async def send_once(host: str, port: int, rover_id: str, telemetry: dict, ack_requested: bool = False, ack_timeout: float = 5.0, include_crc: bool = False):
    """
    Open a TCP connection, send a single framed telemetry message and optionally wait for an ACK.
    """
    reader, writer = await asyncio.open_connection(host, port)
    try:
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
        # include full payload as JSON fallback
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


class TelemetryClient:
    """
    Persistent telemetry client.

    - Connects to TS server and keeps connection alive (if reconnect=True).
    - Sends periodic telemetry when `interval_s` > 0 (uses telemetry_provider or simple generator).
    - Reads incoming frames and dispatches commands to registered on_command callback.
    - Automatically responds with TS_ACK when the incoming header flags request ACK.
    """

    def __init__(
        self,
        rover_id: str,
        host: str = config.TELEMETRY_HOST,
        port: int = config.TELEMETRY_PORT,
        interval_s: float = 0.0,
        reconnect: bool = True,
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
        include_crc: bool = False,
        telemetry_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        self.rover_id = rover_id
        self.host = host
        self.port = port
        self.interval_s = float(interval_s) if interval_s is not None else 0.0
        self.reconnect = bool(reconnect)
        self.backoff_base = float(backoff_base)
        self.backoff_factor = float(backoff_factor)
        self.include_crc = bool(include_crc)
        self.telemetry_provider = telemetry_provider
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._telemetry_task: Optional[asyncio.Task] = None
        self._on_command_cb: Optional[Callable[[Dict[str, Any]], Any]] = None
        self._loop = None

    def on_command(self, cb: Callable[[Dict[str, Any]], Any]) -> None:
        """
        Register a callback to handle incoming commands from the server.
        Callback can be sync function or coroutine function; it receives the canonical payload dict.
        """
        self._on_command_cb = cb

    async def _connect(self) -> None:
        """
        Establish a TCP connection to the telemetry server and set reader/writer.
        """
        logger.info("TelemetryClient %s connecting to %s:%d", self.rover_id, self.host, self.port)
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        logger.info("TelemetryClient %s connected", self.rover_id)

    async def _disconnect(self) -> None:
        """
        Close connection and cleanup tasks.
        """
        logger.info("TelemetryClient %s disconnecting", self.rover_id)
        try:
            if self._reader_task and not self._reader_task.done():
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except asyncio.CancelledError:
                    pass
        except Exception:
            logger.exception("Error cancelling reader task")
        try:
            if self._telemetry_task and not self._telemetry_task.done():
                self._telemetry_task.cancel()
                try:
                    await self._telemetry_task
                except asyncio.CancelledError:
                    pass
        except Exception:
            logger.exception("Error cancelling telemetry task")
        try:
            if self._writer:
                self._writer.close()
                # guard wait_closed so shutdown doesn't hang indefinitely
                try:
                    await asyncio.wait_for(self._writer.wait_closed(), timeout=1.0)
                except Exception:
                    pass
        except Exception:
            pass
        self._reader = None
        self._writer = None

    async def _reader_loop(self) -> None:
        """
        Read frames from server and dispatch commands / handle ACKs.

        Improved error handling: treat ConnectionResetError/OSError as a normal disconnect
        (log at INFO) rather than logging a full stacktrace. Other exceptions still log.
        """
        assert self._reader is not None
        reader = self._reader
        writer = self._writer
        try:
            while True:
                try:
                    hdr = await reader.readexactly(4)
                except asyncio.IncompleteReadError:
                    logger.info("TelemetryClient %s: connection closed by server", self.rover_id)
                    break
                except (ConnectionResetError, OSError) as e:
                    # common on Windows when the server is stopped abruptly
                    logger.info("TelemetryClient %s: connection reset or network error: %s", self.rover_id, e)
                    break
                except Exception:
                    # unexpected error reading; log full exception
                    logger.exception("TelemetryClient %s: unexpected error reading from socket", self.rover_id)
                    break

                if not hdr:
                    break
                L = int.from_bytes(hdr, "big")
                if L <= 0 or L > (1024 * 1024):
                    logger.warning("TelemetryClient %s: invalid frame length %s", self.rover_id, L)
                    break
                try:
                    payload = await reader.readexactly(L)
                except asyncio.IncompleteReadError:
                    logger.info("TelemetryClient %s: incomplete payload (connection closed)", self.rover_id)
                    break
                except (ConnectionResetError, OSError) as e:
                    logger.info("TelemetryClient %s: connection reset while reading payload: %s", self.rover_id, e)
                    break
                except Exception:
                    logger.exception("TelemetryClient %s: unexpected error reading payload", self.rover_id)
                    break

                try:
                    parsed = binary_proto.parse_ts_payload(payload)
                except Exception as e:
                    logger.warning("TelemetryClient %s: failed to parse TS payload: %s", self.rover_id, e)
                    continue

                header = parsed.get("header", {})
                tlvs = parsed.get("tlvs", {})
                canonical = binary_proto.tlv_to_canonical(tlvs)
                # include header metadata for callbacks
                canonical["_msgid"] = header.get("msgid")
                canonical["_ts_server_sent_ms"] = header.get("timestamp_ms", None)

                msgtype = header.get("msgtype")
                logger.debug("TelemetryClient %s received msgtype=%s keys=%s", self.rover_id, msgtype, list(canonical.keys()))

                # If server requested ACK, reply with TS_ACK referencing the server msgid
                try:
                    if header.get("flags") & binary_proto.FLAG_ACK_REQUESTED:
                        try:
                            srv_msgid = int(header.get("msgid") or 0)
                            ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(srv_msgid) & 0xFFFFFFFFFFFFFFFF))]
                            ack_frame = binary_proto.pack_ts_message(binary_proto.TS_ACK, self.rover_id, ack_tlvs, msgid=0)
                            if writer:
                                writer.write(ack_frame)
                                await writer.drain()
                                logger.debug("TelemetryClient %s sent TS_ACK for msgid=%s", self.rover_id, srv_msgid)
                        except Exception:
                            logger.exception("TelemetryClient: failed sending TS_ACK")

                except Exception:
                    logger.exception("TelemetryClient: error checking flags")

                # If message contains payload/json TLV, treat as potential command
                if binary_proto.TLV_PAYLOAD_JSON in tlvs or binary_proto.TLV_PARAMS_JSON in tlvs:
                    # Pass canonical payload to callback if provided
                    if self._on_command_cb:
                        try:
                            cb = self._on_command_cb
                            if inspect.iscoroutinefunction(cb):
                                asyncio.create_task(cb(canonical))
                            else:
                                # call sync cb in executor to avoid blocking reader loop
                                loop = asyncio.get_running_loop()
                                loop.run_in_executor(None, cb, canonical)
                        except Exception:
                            logger.exception("TelemetryClient: on_command callback raised")

        finally:
            logger.info("TelemetryClient %s: reader loop exiting", self.rover_id)

    async def _telemetry_loop(self) -> None:
        """
        Periodic telemetry sender (if interval_s > 0). Uses telemetry_provider if provided,
        otherwise generates a minimal telemetry sample.
        """
        assert self._writer is not None
        while True:
            try:
                telemetry = None
                if self.telemetry_provider:
                    maybe = self.telemetry_provider()
                    if asyncio.iscoroutine(maybe):
                        telemetry = await maybe
                    else:
                        telemetry = maybe
                else:
                    # generate minimal telemetry sample
                    telemetry = {
                        "timestamp_ms": binary_proto.now_ms(),
                        "position": {"x": random.uniform(-5, 5), "y": random.uniform(-5, 5), "z": 0.0},
                        "battery_level_pct": random.randint(20, 100),
                        "status": "IN_MISSION",
                        "progress_pct": random.uniform(0, 100),
                    }
                await self.send_telemetry(telemetry, ack_requested=False, include_crc=self.include_crc)
                await asyncio.sleep(self.interval_s)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("TelemetryClient: error in telemetry loop (continuing)")
                await asyncio.sleep(max(0.1, self.interval_s))

    async def send_telemetry(self, telemetry: dict, ack_requested: bool = False, include_crc: bool = False) -> int:
        """
        Send a telemetry dict as a framed TS_TELEMETRY message. Returns the msgid used.
        """
        if not self._writer:
            raise ConnectionError("Not connected")
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
        tlvs.append((binary_proto.TLV_PAYLOAD_JSON, json.dumps(telemetry, ensure_ascii=False).encode("utf-8")))

        flags = binary_proto.FLAG_ACK_REQUESTED if ack_requested else 0
        if include_crc:
            flags |= binary_proto.FLAG_CRC32

        msgid = random.getrandbits(48)
        frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, self.rover_id, tlvs, flags=flags, msgid=msgid, include_crc=bool(flags & binary_proto.FLAG_CRC32))

        try:
            self._writer.write(frame)
            await self._writer.drain()
            logger.debug("TelemetryClient %s sent telemetry msgid=%s", self.rover_id, msgid)
        except Exception:
            logger.exception("TelemetryClient %s failed to send telemetry", self.rover_id)
            raise

        return int(msgid)

    async def run_once(self, telemetry: Optional[dict] = None, ack_requested: bool = False, include_crc: bool = False):
        """
        Convenience: connect, send a single telemetry sample (or one provided), then disconnect.
        """
        try:
            await self._connect()
            if telemetry is None:
                telemetry = {
                    "timestamp_ms": binary_proto.now_ms(),
                    "position": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "battery_level_pct": 100,
                    "status": "IDLE",
                    "progress_pct": 0.0,
                }
            await self.send_telemetry(telemetry, ack_requested=ack_requested, include_crc=include_crc)
            # small delay to allow server to process and send possible ACKs
            await asyncio.sleep(0.05)
        finally:
            try:
                if self._writer:
                    self._writer.close()
                    await self._writer.wait_closed()
            except Exception:
                pass

    async def start(self):
        """
        Start the persistent client in background and return immediately.
        Use stop() to terminate.
        """
        if self._running:
            return
        self._running = True
        self._loop = asyncio.get_event_loop()
        self._task = self._loop.create_task(self._run_background())
        logger.info("TelemetryClient %s background task started", self.rover_id)

    async def stop(self):
        """
        Stop background client and wait for shutdown.
        """
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # ensure disconnect
        await self._disconnect()
        logger.info("TelemetryClient %s stopped", self.rover_id)

    async def _run_background(self):
        """
        Background runner that maintains connection, spawns reader and telemetry loops,
        and attempts reconnects with exponential backoff on failures.
        """
        backoff_gen = utils.exponential_backoff(base=self.backoff_base, factor=self.backoff_factor, max_delay=60.0)
        while self._running:
            try:
                await self._connect()
                # reset backoff generator by re-creating it on successful connect
                backoff_gen = utils.exponential_backoff(base=self.backoff_base, factor=self.backoff_factor, max_delay=60.0)
                # spawn reader and telemetry tasks
                self._reader_task = asyncio.create_task(self._reader_loop())
                if self.interval_s and self.interval_s > 0:
                    self._telemetry_task = asyncio.create_task(self._telemetry_loop())
                # wait until reader task completes (connection closes) or client asked to stop
                done, pending = await asyncio.wait([self._reader_task], return_when=asyncio.FIRST_COMPLETED)
                # if reader ended, we'll cleanup and possibly reconnect
                try:
                    await self._disconnect()
                except Exception:
                    logger.exception("Error during disconnect cleanup")
            except Exception:
                logger.exception("TelemetryClient connection error; will retry if configured")
                # ensure writer/reader cleaned up
                try:
                    await self._disconnect()
                except Exception:
                    pass

            if not self.reconnect:
                break
            # wait according to backoff
            delay = next(backoff_gen)
            # add jitter
            jitter = max(0.0, random.uniform(-0.5, 0.5))
            sleep_for = max(0.0, delay + jitter)
            logger.info("TelemetryClient %s reconnecting after %.2f s", self.rover_id, sleep_for)
            await asyncio.sleep(sleep_for)


# CLI helper to run persistent client until cancelled
async def _run_persistent(client: 'TelemetryClient'):
    """
    Helper coroutine that starts the persistent client and keeps the process alive
    until cancelled (Ctrl-C). Ensures client.stop() is called on shutdown.
    """
    await client.start()
    try:
        # Sleep loop; wake periodically to allow clean KeyboardInterrupt handling
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        try:
            await client.stop()
        except Exception:
            logger.exception("Error stopping TelemetryClient")


def main():
    parser = argparse.ArgumentParser(description="Telemetry client (rover) for TelemetryStream (TS)")
    parser.add_argument("--host", default=config.TELEMETRY_HOST, help="Telemetry server host")
    parser.add_argument("--port", type=int, default=config.TELEMETRY_PORT, help="Telemetry server port")
    parser.add_argument("--rover-id", required=True, help="Rover identifier")
    parser.add_argument("--once", action="store_true", help="Send a single telemetry sample and exit")
    parser.add_argument("--interval", type=float, default=config.DEFAULT_UPDATE_INTERVAL_S, help="Telemetry update interval in seconds for persistent mode")
    parser.add_argument("--ack", action="store_true", help="Request application-level ACK from server (used with --once)")
    parser.add_argument("--ack-timeout", type=float, default=5.0, help="Timeout to wait for ACK when --once and --ack are used")
    parser.add_argument("--crc", action="store_true", help="Include CRC32 trailer in TS frame")
    parser.add_argument("--reconnect", action="store_true", help="Enable automatic reconnect for persistent client (default enabled)")
    args = parser.parse_args()

    # Run appropriate mode
    if args.once:
        # Build a minimal telemetry sample and send once
        sample = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "battery_level_pct": 100,
            "status": "IDLE",
            "progress_pct": 0.0,
        }
        try:
            asyncio.run(send_once(args.host, args.port, args.rover_id, sample, ack_requested=args.ack, ack_timeout=args.ack_timeout, include_crc=args.crc))
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception:
            logger.exception("send_once failed")
    else:
        # Persistent mode
        client = TelemetryClient(
            rover_id=args.rover_id,
            host=args.host,
            port=args.port,
            interval_s=args.interval,
            reconnect=args.reconnect,
            include_crc=args.crc,
        )
        try:
            asyncio.run(_run_persistent(client))
        except KeyboardInterrupt:
            logger.info("Telemetry client exiting")
        except Exception:
            logger.exception("Unexpected error in telemetry client")


if __name__ == "__main__":
    main()