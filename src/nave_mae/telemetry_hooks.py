"""
telemetry_hooks.py

Register hooks that map incoming telemetry events into MissionStore updates.

Adaptado para o formato binário canonical (dicts produzidos por binary_proto.tlv_to_canonical).
O hook tenta:
 - extrair rover_id e addr do payload (várias formas possíveis),
 - chamar mission_store.register_rover(rover_id, address=...) quando possível,
 - invocar mission_store.update_from_telemetry(rover_id, telemetry) se disponível,
   caso contrário tentar encaminhar para update_progress/complete conforme o payload.
"""

from typing import Any, Callable, Optional, Tuple, Dict
import logging
import inspect

from common import utils

logger = utils.get_logger("nave_mae.telemetry_hooks")


def _extract_rover_and_addr(payload: Any, maybe_addr: Any = None) -> Tuple[Optional[str], Optional[Tuple[str, int]]]:
    """
    Try to find a rover id and address from the payload / args emitted by TelemetryServer.
    Returns (rover_id: Optional[str], addr: Optional[(ip, port)]).
    Accepts canonical telemetry dicts (with keys like 'rover_id', '_addr', 'position', etc.)
    and older shapes that telemetry_store implementations may emit.
    """
    rover_id: Optional[str] = None
    addr: Optional[Tuple[str, int]] = None

    # If payload is a dict, try common keys (canonical)
    if isinstance(payload, dict):
        # canonical patterns
        rover_id = payload.get("rover_id") or payload.get("rover") or payload.get("id")
        # some telemetry_store wrap telemetry inside {"telemetry": {...}}
        if not rover_id and "telemetry" in payload and isinstance(payload["telemetry"], dict):
            rover_id = payload["telemetry"].get("rover_id") or payload["telemetry"].get("rover")
            inner = payload["telemetry"]
            # prefer inner addr if present
            addr_info = inner.get("_addr") or inner.get("addr") or inner.get("address")
        else:
            addr_info = payload.get("_addr") or payload.get("addr") or payload.get("address")

        # If addr_info is dict with ip/port
        if isinstance(addr_info, dict):
            ip = addr_info.get("ip")
            port = addr_info.get("port")
            try:
                if ip is not None and port is not None:
                    addr = (str(ip), int(port))
            except Exception:
                addr = None
        # If addr_info is tuple/list
        elif isinstance(addr_info, (tuple, list)) and len(addr_info) >= 2:
            try:
                addr = (str(addr_info[0]), int(addr_info[1]))
            except Exception:
                addr = None

    # If not found in payload, maybe emitter passed (payload, addr) style
    if not rover_id and maybe_addr and isinstance(maybe_addr, (tuple, list)) and len(maybe_addr) >= 2:
        # sometimes payload is rover_id string and maybe_addr is (ip,port)
        if isinstance(payload, str):
            rover_id = payload
        # if payload is dict but no rover_id, try to glean from payload keys
        elif isinstance(payload, dict):
            rover_id = payload.get("rover_id") or payload.get("rover")

    # If we still don't have addr but maybe_addr looks like an address, use it
    if addr is None and isinstance(maybe_addr, (tuple, list)) and len(maybe_addr) >= 2:
        try:
            addr = (str(maybe_addr[0]), int(maybe_addr[1]))
        except Exception:
            addr = None

    # Normalise rover_id if nested dict
    if isinstance(rover_id, dict):
        rover_id = rover_id.get("rover_id") or rover_id.get("id") or None

    return rover_id, addr


def register_telemetry_hooks(mission_store: Any, telemetry_store: Any) -> None:
    """
    Install hooks so telemetry events update the supplied MissionStore.

    The function tries several strategies to attach the hook depending on the API
    of telemetry_store. It logs an INFO when the hook is registered and also logs
    an INFO each time a rover is registered by the hook.
    """

    def _core_register(rover_id: Optional[str], addr: Optional[Tuple[str, int]]):
        """
        Idempotent registration: only call mission_store.register_rover when the rover
        is not known or its stored address differs from the provided one.
        This avoids noisy repeated INFO logs when telem samples arrive frequently.
        """
        if not rover_id:
            return
        try:
            # Normalize incoming addr to tuple or None
            norm_addr: Optional[Tuple[str, int]] = None
            if isinstance(addr, dict):
                try:
                    ip = addr.get("ip")
                    port = addr.get("port")
                    if ip is not None and port is not None:
                        norm_addr = (str(ip), int(port))
                except Exception:
                    norm_addr = None
            elif isinstance(addr, (tuple, list)) and len(addr) >= 2:
                try:
                    norm_addr = (str(addr[0]), int(addr[1]))
                except Exception:
                    norm_addr = None

            # Try to examine existing registration to decide if we must call register_rover
            existing = None
            try:
                if hasattr(mission_store, "get_rover"):
                    existing = mission_store.get_rover(rover_id)
            except Exception:
                existing = None

            should_register = False
            # If no existing record, we should register
            if existing is None:
                should_register = True
            else:
                # existing may contain an 'address' entry of form {"ip":..., "port":...}
                existing_addr = None
                if isinstance(existing, dict):
                    existing_addr = existing.get("address") or existing.get("addr")
                # normalize existing_addr to tuple if possible
                ex_addr_tup = None
                if isinstance(existing_addr, dict):
                    try:
                        ex_ip = existing_addr.get("ip")
                        ex_port = existing_addr.get("port")
                        if ex_ip is not None and ex_port is not None:
                            ex_addr_tup = (str(ex_ip), int(ex_port))
                    except Exception:
                        ex_addr_tup = None
                elif isinstance(existing_addr, (tuple, list)) and len(existing_addr) >= 2:
                    try:
                        ex_addr_tup = (str(existing_addr[0]), int(existing_addr[1]))
                    except Exception:
                        ex_addr_tup = None

                # If we have a normalized incoming addr and it differs from stored one, re-register
                if norm_addr and ex_addr_tup is None:
                    should_register = True
                elif norm_addr and ex_addr_tup and (norm_addr[0] != ex_addr_tup[0] or int(norm_addr[1]) != int(ex_addr_tup[1])):
                    should_register = True
                # If stored had no addr and incoming also none -> don't re-register
                # If both present and equal -> no-op

            if should_register:
                try:
                    # Try preferred kwarg signature first
                    try:
                        mission_store.register_rover(rover_id, address=norm_addr)
                        logger.info("telemetry_hooks: registered rover %s addr=%s", rover_id, norm_addr)
                        return
                    except TypeError:
                        pass
                    # Fallback positional
                    try:
                        mission_store.register_rover(rover_id, norm_addr)
                        logger.info("telemetry_hooks: registered rover %s addr=%s", rover_id, norm_addr)
                        return
                    except TypeError:
                        pass
                    # Last resort: register by id only
                    try:
                        mission_store.register_rover(rover_id)
                        logger.info("telemetry_hooks: registered rover %s (no addr)", rover_id)
                        return
                    except Exception:
                        logger.exception("telemetry_hooks: failed to register rover %s", rover_id)
                except Exception:
                    logger.exception("telemetry_hooks: unexpected error while registering rover %s", rover_id)
            else:
                # avoid INFO spam — debug is sufficient to indicate repeated registrations
                logger.debug("telemetry_hooks: rover %s already registered addr=%s (no-op)", rover_id, norm_addr)
        except Exception:
            logger.exception("telemetry_hooks: unexpected error while attempting _core_register")

    def _handle_mission_from_telemetry(rover_id: str, telemetry: Dict[str, Any]) -> None:
        """
        Forward telemetry to mission_store. Prefer mission_store.update_from_telemetry if available.
        Fallback to calling update_progress/complete as appropriate.
        """
        try:
            if hasattr(mission_store, "update_from_telemetry") and callable(getattr(mission_store, "update_from_telemetry")):
                try:
                    mission_store.update_from_telemetry(rover_id, telemetry)
                    return
                except Exception:
                    logger.exception("telemetry_hooks: update_from_telemetry raised exception; falling back")
            # Fallback behaviour: if telemetry contains mission_id + progress_pct -> update_progress
            mid = telemetry.get("mission_id")
            prog = telemetry.get("progress_pct")
            status = telemetry.get("status")
            prog_num = None
            if prog is not None:
                try:
                    prog_num = float(prog)
                except Exception:
                    prog_num = None
            if mid and prog is not None and hasattr(mission_store, "update_progress"):
                try:
                    mission_store.update_progress(mid, rover_id, telemetry)
                except Exception:
                    logger.exception("telemetry_hooks: update_progress failed for %s", mid)
            # If mission appears completed, call complete_mission
            completed = False
            if prog_num is not None and prog_num >= 100.0:
                completed = True
            if isinstance(status, str) and status.lower() in ("completed", "complete", "success", "done"):
                completed = True
            if completed and mid and hasattr(mission_store, "complete_mission"):
                try:
                    mission_store.complete_mission(mid, rover_id, telemetry)
                except Exception:
                    logger.exception("telemetry_hooks: complete_mission failed for %s", mid)
        except Exception:
            logger.exception("telemetry_hooks: unexpected error in mission forwarding")

    def _universal_handler(*args, **kwargs):
        """
        Universal wrapper that tolerates different hook signatures:
          - (event_type, payload)
          - (payload,)
          - (payload, addr)
          - (event_type, payload, addr, ...)
        Extract payload and possible addr then call core register and mission forwarding.
        """
        try:
            event_type = None
            payload = None
            maybe_addr = None

            # Parse positional args
            if len(args) >= 2:
                # common: (event_type, payload) or (payload, addr)
                if isinstance(args[0], str) and isinstance(args[1], dict):
                    event_type = args[0]
                    payload = args[1]
                    maybe_addr = args[2] if len(args) > 2 else None
                elif isinstance(args[0], dict):
                    payload = args[0]
                    maybe_addr = args[1]
                else:
                    # fallback: second arg might be payload
                    if isinstance(args[1], dict):
                        payload = args[1]
                        maybe_addr = args[2] if len(args) > 2 else None
                    else:
                        payload = args[0]
                        maybe_addr = args[1]
            elif len(args) == 1:
                payload = args[0]
            else:
                # kwargs fallback
                payload = kwargs.get("payload") or kwargs.get("data") or kwargs.get("telemetry")
                maybe_addr = kwargs.get("addr") or kwargs.get("address")

            # If payload is a wrapper like {"rover_id":..., "telemetry": {...}} flatten
            if isinstance(payload, dict) and "telemetry" in payload and isinstance(payload["telemetry"], dict):
                inner = payload["telemetry"]
                # Merge header fields into inner if present
                merged = dict(inner)
                # Copy potential metadata
                for k in ("_addr", "addr", "timestamp", "ts", "ts_ms", "_ts_server_received_ms"):
                    if k in payload and k not in merged:
                        merged[k] = payload[k]
                payload = merged

            # Only handle telemetry-like events primarily, but keep tolerant to other event names
            rover_id, addr = _extract_rover_and_addr(payload, maybe_addr)

            # Register rover (updates last_seen/address) only when necessary
            _core_register(rover_id, addr)

            # Forward telemetry to mission store if we have a telemetry dict
            telemetry_payload = None
            if isinstance(payload, dict):
                # If the payload wrapper had 'rover_id' and 'telemetry' then we already flattened above
                telemetry_payload = payload.get("telemetry") if payload.get("telemetry") and isinstance(payload.get("telemetry"), dict) else payload
            # If telemetry_payload is None and payload is a string id, nothing to forward
            if rover_id and telemetry_payload and isinstance(telemetry_payload, dict):
                # ensure telemetry has rover_id and addr included for convenience
                if "rover_id" not in telemetry_payload:
                    telemetry_payload["rover_id"] = rover_id
                if "_addr" not in telemetry_payload and addr:
                    telemetry_payload["_addr"] = addr
                # Call mission forwarding helper
                _handle_mission_from_telemetry(rover_id, telemetry_payload)

        except Exception:
            logger.exception("telemetry_hooks: unexpected error in universal handler")

    attached = False
    try:
        # Try classic evented APIs first (order matters: prefer specific APIs)
        if hasattr(telemetry_store, "on"):
            try:
                telemetry_store.on("telemetry", _universal_handler)
                attached = True
            except Exception:
                try:
                    telemetry_store.on("line", _universal_handler)
                    attached = True
                except Exception:
                    pass

        if not attached and hasattr(telemetry_store, "add_listener"):
            try:
                telemetry_store.add_listener("telemetry", _universal_handler)
                attached = True
            except Exception:
                try:
                    telemetry_store.add_listener("line", _universal_handler)
                    attached = True
                except Exception:
                    pass

        if not attached and hasattr(telemetry_store, "subscribe"):
            try:
                telemetry_store.subscribe("telemetry", _universal_handler)
                attached = True
            except Exception:
                try:
                    telemetry_store.subscribe(_universal_handler)
                    attached = True
                except Exception:
                    pass

        if not attached and hasattr(telemetry_store, "register_callback"):
            try:
                telemetry_store.register_callback(_universal_handler)
                attached = True
            except Exception:
                pass

        # Support TelemetryStore.register_hook(fn) (the implementation in this repo)
        if not attached and hasattr(telemetry_store, "register_hook"):
            try:
                telemetry_store.register_hook(_universal_handler)
                attached = True
            except Exception:
                pass

        # Alternative common names
        if not attached and hasattr(telemetry_store, "register_handler"):
            try:
                telemetry_store.register_handler("telemetry", _universal_handler)
                attached = True
            except Exception:
                try:
                    telemetry_store.register_handler(_universal_handler)
                    attached = True
                except Exception:
                    pass

        if not attached and hasattr(telemetry_store, "register"):
            try:
                telemetry_store.register(_universal_handler)
                attached = True
            except Exception:
                pass

        # If the telemetry_store exposes an 'emit' or '_emit' method, we can wrap it
        if not attached:
            emitter_name = None
            if hasattr(telemetry_store, "_emit"):
                emitter_name = "_emit"
            elif hasattr(telemetry_store, "emit"):
                emitter_name = "emit"

            if emitter_name is not None:
                orig_emit = getattr(telemetry_store, emitter_name)

                def _wrapped_emit(event, *args, **kwargs):
                    try:
                        if event in ("telemetry", "line", "data"):
                            if len(args) >= 1:
                                payload = args[0]
                                maybe_addr = args[1] if len(args) > 1 else kwargs.get("addr")
                                try:
                                    rid, a = _extract_rover_and_addr(payload, maybe_addr)
                                    _core_register(rid, a)
                                    # forward mission telemetry if payload is dict
                                    if isinstance(payload, dict):
                                        tp = payload.get("telemetry") if payload.get("telemetry") and isinstance(payload.get("telemetry"), dict) else payload
                                        if rid and isinstance(tp, dict):
                                            if "rover_id" not in tp:
                                                tp["rover_id"] = rid
                                            if "_addr" not in tp and a:
                                                tp["_addr"] = a
                                            _handle_mission_from_telemetry(rid, tp)
                                except Exception:
                                    logger.exception("telemetry_hooks: error calling core_register from wrapped emit")
                        return orig_emit(event, *args, **kwargs)
                    except Exception:
                        logger.exception("telemetry_hooks: error in wrapped emitter; falling back to orig_emit")
                        return orig_emit(event, *args, **kwargs)

                setattr(telemetry_store, emitter_name, _wrapped_emit)
                attached = True

    except Exception:
        logger.exception("telemetry_hooks: failed while attempting to attach hook")

    if attached:
        logger.info("Registered telemetry -> MissionStore hook")
    else:
        logger.warning("Could not attach telemetry -> MissionStore hook automatically; telemetry events may not update MissionStore")