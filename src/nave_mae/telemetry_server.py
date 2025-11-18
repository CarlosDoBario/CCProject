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
"""

import asyncio
import logging
from typing import Optional, Tuple, Any

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

    async def start(self):
        self._server = await asyncio.start_server(self._handle_client, host=self.host, port=self.port)
        logger.info("TelemetryServer listening on %s:%d", self.host, self.port)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("TelemetryServer stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        logger.info("TS connection from %s", peer)
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
                        err_tlv = binary_proto.tlv_string(binary_proto.TLV_ERRORS, str(e))
                        err_frame = binary_proto.pack_ts_message(binary_proto.TS_ERROR, "", [(binary_proto.TLV_ERRORS, str(e).encode("utf-8"))], flags=0)
                        # write directly (empty rover id in error)
                        writer.write(err_frame)
                        await writer.drain()
                    except Exception:
                        pass
                    continue

                header = parsed["header"]
                rover_id = parsed["rover_id"]
                tlvs = parsed["tlvs"]
                canonical = binary_proto.tlv_to_canonical(tlvs)
                # attach header metadata
                canonical["_msgid"] = header.get("msgid")
                canonical["_ts_server_received_ms"] = binary_proto.now_ms()

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

                # send ACK if requested
                if header.get("flags") & binary_proto.FLAG_ACK_REQUESTED:
                    try:
                        ack_tlvs = [(binary_proto.TLV_ACKED_MSG_ID, struct_pack_q(canonical["_msgid"] if canonical.get("_msgid") else 0))]
                        ack_frame = binary_proto.pack_ts_message(binary_proto.TS_ACK, rover_id, ack_tlvs)
                        writer.write(ack_frame)
                        await writer.drain()
                    except Exception:
                        logger.exception("Failed to send ACK to %s", peer)

        except asyncio.IncompleteReadError:
            logger.info("Connection closed by %s", peer)
        except Exception:
            logger.exception("Unexpected error in TS handler for %s", peer)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


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