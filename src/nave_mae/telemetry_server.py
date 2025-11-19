#!/usr/bin/env python3
"""
telemetry_server.py

TelemetryStream (TS) TCP server using the binary TLV format.

Behaviour:
- Listens for TCP connections from rovers.
- Each message is framed with a 4-byte big-endian length prefix followed by a payload.
- Payload parsed via common.binary_proto.parse_ts_payload -> canonical fields
- Calls mission_store.register_rover(...) and telemetry_store.update(...) with canonical payload.
- Replies optional ACK when header flags ask for ACK (ACK TLV with acked msgid).

Enhancements:
- Maintains mapping of connected rovers -> StreamWriter (self._clients)
- Adds async send_command(rover_id, cmd_dict, expect_ack=False, timeout=5.0) to push commands
  down to connected rovers. Uses application-level ACK TLV (TLV_ACKED_MSG_ID) to wait.
"""
from __future__ import annotations

import asyncio
import logging
import json
import struct
from typing import Optional, Tuple, Any, Dict

from common import binary_proto, utils, config

logger = utils.get_logger("telemetry.server")


class TelemetryServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 65080, mission_store: Any = None, telemetry_store: Any = None):
        self.host = host
        self.port = port
        self._server: Optional[asyncio.base_events.Server] = None
        self._loop = asyncio.get_event_loop()
        self.mission_store = mission_store
        self.telemetry_store = telemetry_store

        # Map rover_id -> StreamWriter for connected clients (last writer wins)
        self._clients: Dict[str, asyncio.StreamWriter] = {}
        # Pending ACK futures keyed by msgid (uint64)
        self._pending_acks: Dict[int, asyncio.Future] = {}

    async def start(self):
        self._server = await asyncio.start_server(self._handle_client, host=self.host, port=self.port)
        logger.info("TelemetryServer listening on %s:%d", self.host, self.port)

    async def stop(self):
        if self._server:
            # Close all active client writers first
            for rid, w in list(self._clients.items()):
                try:
                    w.close()
                    await w.wait_closed()
                except Exception:
                    pass
            self._clients.clear()
            self._server.close()
            await self._server.wait_closed()
            logger.info("TelemetryServer stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        logger.info("TS connection from %s", peer)
        rover_id = None
        try:
            while True:
                # Read 4-byte length prefix
                hdr = await reader.readexactly(4)
                if not hdr:
                    break
                L = int.from_bytes(hdr, "big")
                if L <= 0 or L > (1024 * 1024):  # sanity limit 1MB
                    logger.warning("Invalid frame length %s from %s", L, peer)
                    break
                payload = await reader.readexactly(L)
                try:
                    parsed = binary_proto.parse_ts_payload(payload)
                except Exception as e:
                    logger.warning("Failed to parse TS message from %s: %s", peer, e)
                    # send ERROR frame back
                    try:
                        err_frame = binary_proto.pack_ts_message(binary_proto.TS_ERROR, "", [(binary_proto.TLV_ERRORS, str(e).encode("utf-8"))], flags=0)
                        writer.write(err_frame)
                        await writer.drain()
                    except Exception:
                        pass
                    continue

                header = parsed["header"]
                incoming_rover = parsed["rover_id"]
                tlvs = parsed["tlvs"]
                canonical = binary_proto.tlv_to_canonical(tlvs)
                # attach header metadata
                canonical["_msgid"] = header.get("msgid")
                canonical["_ts_server_received_ms"] = binary_proto.now_ms()

                # Once we know rover_id, register writer mapping so server can push commands
                if incoming_rover:
                    rover_id = incoming_rover
                    # record writer (last connection for a rover wins)
                    self._clients[rover_id] = writer

                # register rover in mission_store if available
                try:
                    if self.mission_store and hasattr(self.mission_store, "register_rover"):
                        addr = peer if peer else None
                        try:
                            # prefer kwarg address if signature supports
                            self.mission_store.register_rover(rover_id, address={"ip": addr[0], "port": int(addr[1])} if addr else None)
                        except TypeError:
                            try:
                                self.mission_store.register_rover(rover_id, addr)
                            except Exception:
                                self.mission_store.register_rover(rover_id)
                except Exception:
                    logger.exception("mission_store.register_rover failed for %s", rover_id)

                # update telemetry_store
                try:
                    if self.telemetry_store and hasattr(self.telemetry_store, "update"):
                        # try update(rover_id, payload, addr=peer)
                        try:
                            self.telemetry_store.update(rover_id, canonical, addr=peer)
                        except TypeError:
                            # fallback without addr
                            self.telemetry_store.update(rover_id, canonical)
                except Exception:
                    logger.exception("telemetry_store.update failed for %s", rover_id)

                logger.info("Received TS %s from %s rover=%s fields=%s", header.get("msgtype"), peer, rover_id, list(canonical.keys()))

                # If this is an ACK frame from the client, resolve any pending future
                try:
                    if header.get("msgtype") == binary_proto.TS_ACK:
                        acked_bytes = tlvs.get(binary_proto.TLV_ACKED_MSG_ID, [])
                        if acked_bytes:
                            try:
                                acked_id = struct.unpack(">Q", acked_bytes[0])[0]
                                fut = self._pending_acks.pop(int(acked_id), None)
                                if fut and not fut.done():
                                    fut.set_result(True)
                                    logger.debug("Received TS_ACK for msgid=%s from %s", acked_id, rover_id)
                            except Exception:
                                logger.exception("Failed to unpack TS_ACK TLV_ACKED_MSG_ID")
                except Exception:
                    logger.exception("Error processing ACK frame")

                # send ACK if requested by client
                if header.get("flags") & binary_proto.FLAG_ACK_REQUESTED:
                    try:
                        ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(canonical.get("_msgid") or 0)))]
                        ack_frame = binary_proto.pack_ts_message(binary_proto.TS_ACK, rover_id or "", ack_tlvs)
                        writer.write(ack_frame)
                        await writer.drain()
                    except Exception:
                        logger.exception("Failed to send ACK to %s", peer)

        except asyncio.IncompleteReadError:
            logger.info("Connection closed by %s", peer)
        except Exception:
            logger.exception("Unexpected error in TS handler for %s", peer)
        finally:
            # Cleanup writer mapping for this rover if it matches current writer
            try:
                if rover_id and self._clients.get(rover_id) is writer:
                    del self._clients[rover_id]
                    logger.info("Removed client mapping for rover %s", rover_id)
            except Exception:
                pass
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


    ############################################################################
    # Public helper methods for server->rover command push and introspection  #
    ############################################################################
    async def send_command(self, rover_id: str, cmd: dict, expect_ack: bool = False, timeout: float = 5.0) -> Optional[int]:
        """
        Send a command dict to a connected rover identified by rover_id.

        - Serializes `cmd` as JSON into a TLV_PAYLOAD_JSON TLV and sends it as a TS_TELEMETRY message.
        - If expect_ack=True, waits up to `timeout` seconds for the rover to respond with a TS_ACK
          containing a TLV_ACKED_MSG_ID referencing the msgid used for this command.
        - Returns the msgid used for the command on success. If expect_ack is True and timeout
          occurs, raises asyncio.TimeoutError.

        Raises:
            KeyError: if rover_id not connected.
            Exception: for IO errors while sending.
        """
        writer = self._clients.get(rover_id)
        if not writer:
            raise KeyError(f"Rover {rover_id} not connected")

        # Use current epoch ms as msgid
        msgid = int(binary_proto.now_ms()) & 0xFFFFFFFFFFFFFFFF
        # Build TLVs: command payload as JSON
        payload_b = json.dumps(cmd, ensure_ascii=False).encode("utf-8")
        tlvs = [(binary_proto.TLV_PAYLOAD_JSON, payload_b)]

        flags = binary_proto.FLAG_ACK_REQUESTED if expect_ack else 0
        frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, flags=flags, msgid=msgid, include_crc=False)

        # If we need to wait for an ACK, create a Future and store it
        fut: Optional[asyncio.Future] = None
        if expect_ack:
            fut = self._loop.create_future()
            self._pending_acks[int(msgid)] = fut

        # Write frame
        try:
            writer.write(frame)
            await writer.drain()
            logger.info("Sent command msgid=%s to rover=%s", msgid, rover_id)
        except Exception:
            # clean up pending ack on write failure
            if fut:
                self._pending_acks.pop(int(msgid), None)
            logger.exception("Failed to send command to %s", rover_id)
            raise

        # Wait for ack if requested
        if expect_ack and fut:
            try:
                await asyncio.wait_for(fut, timeout=timeout)
                return msgid
            except asyncio.TimeoutError:
                # remove pending ack future to avoid leak
                self._pending_acks.pop(int(msgid), None)
                logger.warning("Timeout waiting for ACK for command msgid=%s to rover=%s", msgid, rover_id)
                raise

        return msgid

    def get_connected_rovers(self) -> list:
        """Return a list of currently connected rover IDs (last seen connection)."""
        return list(self._clients.keys())


# helper for packing uint64 in big-endian (used above)
def struct_pack_q(v: int) -> bytes:
    return struct.pack(">Q", v & 0xFFFFFFFFFFFFFFFF)


# Entrypoint convenience
async def start_server_async(host: str = "0.0.0.0", port: int = 65080, mission_store: Any = None, telemetry_store: Any = None):
    srv = TelemetryServer(host=host, port=port, mission_store=mission_store, telemetry_store=telemetry_store)
    await srv.start()
    return srv


def run_server(host: str = "0.0.0.0", port: int = 65080, mission_store: Any = None, telemetry_store: Any = None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    srv = TelemetryServer(host=host, port=port, mission_store=mission_store, telemetry_store=telemetry_store)
    try:
        loop.run_until_complete(srv.start())
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("TS server shutting down")
    finally:
        loop.run_until_complete(srv.stop())
        loop.close()