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

Enhancements in this version:
- Treats ConnectionResetError / OSError as expected disconnects (no noisy tracebacks).
- Tracks client handler tasks so stop() can cancel and await them for a clean shutdown.
- Ensures pending ACK futures are cancelled on shutdown to avoid resource leaks.
- stop() initiates shutdown quickly by force-closing transports and scheduling a background cleanup.
"""
from __future__ import annotations

import asyncio
import logging
import json
import struct
from typing import Optional, Tuple, Any, Dict, Set

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

        # Track active client handler tasks so we can cancel/await them on stop
        self._client_tasks: Set[asyncio.Task] = set()
        self._stopping = False

    async def start(self):
        self._server = await asyncio.start_server(self._handle_client, host=self.host, port=self.port)
        logger.info("TelemetryServer listening on %s:%d", self.host, self.port)

    # --- Stop is designed to return quickly and schedule background cleanup to avoid blocking tests ---
    async def stop(self) -> None:
        """
        Initiate server shutdown quickly and return.

        This function performs an immediate close/abort of client connections to
        ensure clients observe the disconnect promptly, then schedules a background
        cleanup task that will await wait_closed() and cancel pending futures with
        short timeouts. This avoids blocking the caller (tests) while ensuring
        resources are reclaimed eventually.
        """
        if self._stopping:
            logger.debug("TelemetryServer.stop() called but already stopping")
            return
        self._stopping = True
        logger.info("TelemetryServer: initiating stop()")

        # Stop accepting new connections
        if self._server:
            try:
                self._server.close()
            except Exception:
                logger.exception("Error closing server socket (close call)")
            # schedule server.wait_closed in background to complete close
            try:
                loop = asyncio.get_running_loop()
                async def _await_server_closed(srv):
                    try:
                        await srv.wait_closed()
                    except Exception:
                        pass
                loop.create_task(_await_server_closed(self._server))
            except Exception:
                pass
            self._server = None

        # Snapshot current writers/tasks
        writers = list(self._clients.values())
        tasks = list(self._client_tasks)

        # Force-close underlying transports and call writer.close() to prompt immediate disconnect
        if writers:
            logger.info("TelemetryServer: force-closing %d client connection(s) to prompt client disconnect", len(writers))
        for w in writers:
            try:
                # try to abort transport if available (fast)
                transport = getattr(w, "transport", None) or getattr(w, "_transport", None)
                if transport and hasattr(transport, "abort"):
                    try:
                        transport.abort()
                    except Exception:
                        pass
                # fallback: try to close underlying socket if accessible
                try:
                    sock = w.get_extra_info("socket")
                    if sock:
                        try:
                            sock.shutdown(2)
                        except Exception:
                            pass
                        try:
                            sock.close()
                        except Exception:
                            pass
                except Exception:
                    pass
                # polite close (won't block here)
                try:
                    w.close()
                except Exception:
                    pass
            except Exception:
                pass

        # Cancel client handler tasks (they should wake up after transport abort); do not block long here
        if tasks:
            logger.debug("TelemetryServer: cancelling %d client handler task(s)", len(tasks))
            for t in tasks:
                try:
                    t.cancel()
                except Exception:
                    pass

        # Schedule background cleanup to await wait_closed() and clear pending futures
        async def _background_cleanup(writers_snapshot, tasks_snapshot):
            try:
                # Give tasks a short window to finish after cancellation
                if tasks_snapshot:
                    try:
                        await asyncio.wait_for(asyncio.gather(*tasks_snapshot, return_exceptions=True), timeout=1.0)
                    except Exception:
                        # ignore timeouts/exceptions; proceed
                        pass
                # Wait briefly for writer.wait_closed to finish (short timeout)
                for w2 in writers_snapshot:
                    try:
                        await asyncio.wait_for(w2.wait_closed(), timeout=0.5)
                    except Exception:
                        pass
                # Cancel pending ack futures
                if self._pending_acks:
                    logger.debug("TelemetryServer: cancelling %d pending ack future(s) in background cleanup", len(self._pending_acks))
                for mid, fut in list(self._pending_acks.items()):
                    try:
                        if not fut.done():
                            fut.cancel()
                    except Exception:
                        pass
                    self._pending_acks.pop(mid, None)
                # Clear tracking sets/maps
                try:
                    self._client_tasks.difference_update(tasks_snapshot)
                except Exception:
                    pass
                try:
                    self._clients.clear()
                except Exception:
                    pass
            except Exception:
                logger.exception("TelemetryServer: background cleanup encountered error")
            finally:
                logger.info("TelemetryServer: background cleanup finished")

        try:
            asyncio.get_running_loop().create_task(_background_cleanup(writers, tasks))
        except Exception:
            # as last resort run cleanup in a safe fire-and-forget manner
            try:
                asyncio.create_task(_background_cleanup(writers, tasks))
            except Exception:
                logger.exception("TelemetryServer: failed to schedule background cleanup")

        logger.info("TelemetryServer: stop() initiated (returning to caller)")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Handler for a single client connection.
        This reads framed messages (4-byte length prefix) and dispatches them.
        It treats socket-level ConnectionResetError/OSError as expected
        disconnects and logs them at INFO level (no noisy stacktraces).
        """
        peer = writer.get_extra_info("peername")
        logger.debug("TS handler connected from %s", peer)
        rover_id: Optional[str] = None

        # register task so stop() can cancel/await it
        task = asyncio.current_task()
        if task is not None:
            self._client_tasks.add(task)

        try:
            while True:
                try:
                    # Read 4-byte length prefix
                    hdr = await reader.readexactly(4)
                except asyncio.IncompleteReadError:
                    # Remote closed connection cleanly
                    logger.info("Connection closed by %s", peer)
                    break
                except (ConnectionResetError, OSError) as e:
                    # Common on Windows when the peer disappears; treat as normal disconnect
                    logger.info("Connection reset / network error from %s: %s", peer, e)
                    break
                except Exception:
                    # Unexpected error: log full exception for debugging
                    logger.exception("Unexpected error in TS handler for %s", peer)
                    break

                if not hdr:
                    # no data
                    break
                L = int.from_bytes(hdr, "big")
                if L <= 0 or L > (10 * 1024 * 1024):
                    logger.warning("Invalid TS frame length %s from %s", L, peer)
                    break
                # read payload
                try:
                    payload = await reader.readexactly(L)
                except asyncio.IncompleteReadError:
                    logger.info("Incomplete TS payload from %s", peer)
                    break
                except (ConnectionResetError, OSError) as e:
                    logger.info("Connection reset while reading payload from %s: %s", peer, e)
                    break
                except Exception:
                    logger.exception("Unexpected error reading TS payload from %s", peer)
                    break

                # parse payload and handle messages
                try:
                    parsed = binary_proto.parse_ts_payload(payload)
                except Exception:
                    logger.exception("Failed to parse TS payload from %s", peer)
                    continue

                header = parsed.get("header", {})
                tlvs = parsed.get("tlvs", {})
                rid = parsed.get("rover_id")
                # update rover mapping if not yet present
                if rid and rid not in self._clients:
                    try:
                        self._clients[rid] = writer
                        logger.info("TS connection from %s", peer)
                        # if telemetry/mission hooks exist, propagate registration
                        if self.telemetry_store and hasattr(self.telemetry_store, "register_rover"):
                            # some stores expose helpers; best effort
                            try:
                                self.telemetry_store.register_rover(rid, peer)
                            except Exception:
                                pass
                    except Exception:
                        logger.exception("Error registering rover %s from %s", rid, peer)

                # Dispatch on msgtype
                msgtype = header.get("msgtype")
                if msgtype == binary_proto.TS_TELEMETRY:
                    # convert TLVs to canonical and notify telemetry store/mission_store hooks
                    try:
                        canonical = binary_proto.tlv_to_canonical(tlvs)
                        canonical["_msgid"] = header.get("msgid")
                        canonical["_ts_server_received_ms"] = binary_proto.now_ms()
                        # TelemetryStore expected hook signature may differ; try common ones
                        if self.telemetry_store and hasattr(self.telemetry_store, "update"):
                            try:
                                self.telemetry_store.update(rid, canonical)
                            except Exception:
                                # fallback: if telemetry_store has register_hook style, call hooks externally
                                pass
                        # MissionStore update: register last seen / state
                        if self.mission_store and hasattr(self.mission_store, "update_rover"):
                            try:
                                self.mission_store.update_rover(rid, canonical)
                            except Exception:
                                pass
                    except Exception:
                        logger.exception("Error handling telemetry from %s rover=%s", peer, rid)
                elif msgtype == binary_proto.TS_ACK:
                    # extract acked msg id tlv and set future if present
                    try:
                        if binary_proto.TLV_ACKED_MSG_ID in tlvs:
                            data = tlvs[binary_proto.TLV_ACKED_MSG_ID][0]
                            # unpack as big-endian Q (8 bytes) or smaller
                            acked_id = int.from_bytes(data.rjust(8, b'\x00'), "big")
                            fut = self._pending_acks.pop(acked_id, None)
                            if fut and not fut.done():
                                fut.set_result(True)
                    except Exception:
                        logger.exception("Error processing TS_ACK from %s", peer)
                else:
                    # other message types ignored or logged
                    logger.debug("TS message type %s from %s", msgtype, peer)

        finally:
            # cleanup on disconnect
            try:
                if task is not None:
                    self._client_tasks.discard(task)
                if writer in self._clients.values():
                    # remove any mapping that points to this writer
                    to_remove = [k for k, v in list(self._clients.items()) if v is writer]
                    for k in to_remove:
                        try:
                            self._clients.pop(k, None)
                            logger.info("Removed client mapping for rover %s", k)
                        except Exception:
                            pass
            except Exception:
                logger.exception("Error during cleanup for peer %s", peer)
            try:
                writer.close()
                # await wait_closed but don't block forever
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
                except Exception:
                    pass
            except Exception:
                pass

    async def send_command(self, rover_id: str, command: dict, expect_ack: bool = False, timeout: float = 5.0) -> Optional[bool]:
        """
        Send a command (as payload JSON TLV) to a connected rover.
        If expect_ack is True, wait for a TS_ACK referencing the generated msgid and return True on ack.
        Raises KeyError if the rover is not connected.
        Raises asyncio.TimeoutError on ack timeout.
        """
        if rover_id not in self._clients:
            raise KeyError(rover_id)
        peer_writer = self._clients[rover_id]
        # build TLVs: include payload_json
        import json
        tlvs = [(binary_proto.TLV_PAYLOAD_JSON, json.dumps(command, ensure_ascii=False).encode("utf-8"))]
        msgid = int(binary_proto.now_ms()) & 0xFFFFFFFFFFFFFFFF
        flags = binary_proto.FLAG_ACK_REQUESTED if expect_ack else 0
        frame = binary_proto.pack_ts_message(binary_proto.TS_TELEMETRY, rover_id, tlvs, flags=flags, msgid=msgid)
        fut = None
        if expect_ack:
            fut = asyncio.get_event_loop().create_future()
            self._pending_acks[int(msgid)] = fut
        try:
            peer_writer.write(frame)
            await peer_writer.drain()
            if expect_ack:
                await asyncio.wait_for(fut, timeout=timeout)
                return True
            return None
        finally:
            if expect_ack:
                self._pending_acks.pop(int(msgid), None)
                if fut and not fut.done():
                    fut.cancel()

    def get_connected_rovers(self) -> list:
        return list(self._clients.keys())