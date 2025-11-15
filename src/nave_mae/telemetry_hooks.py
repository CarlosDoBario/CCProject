"""
telemetry_hooks.py

Register hooks that map incoming telemetry events into MissionStore updates.

This module tries to be robust against a few possible TelemetryStore APIs:
- telemetry_store.on(event, handler)
- telemetry_store.add_listener(event, handler)
- telemetry_store.register_callback(handler)
- telemetry_store.register_hook(handler)
- telemetry_store._emit / telemetry_store.emit (wraps emitter to intercept events)

The registered callback attempts to extract a rover_id (from payload or header) and
call mission_store.register_rover(rover_id, address) when possible. It logs an INFO
message each time a rover is (re)registered so it's obvious in the ML server logs.
"""
import logging
from typing import Any, Callable, Optional

logger = logging.getLogger("ml.telemetry_hooks")


def _extract_rover_and_addr(payload: Any, maybe_addr: Any = None):
    """
    Try to find a rover id and address from the payload / args emitted by TelemetryServer.
    Returns (rover_id: Optional[str], addr: Optional[tuple(ip, port)])
    """
    rover_id = None
    addr = None

    # If payload is a dict, try common keys
    if isinstance(payload, dict):
        rover_id = payload.get("rover_id") or payload.get("rover") or payload.get("id")
        # payload may embed header
        if not rover_id and "header" in payload and isinstance(payload["header"], dict):
            rover_id = payload["header"].get("rover_id")
        # payload may include an address dict
        addr_info = payload.get("addr") or payload.get("address")
        if isinstance(addr_info, dict):
            ip = addr_info.get("ip")
            port = addr_info.get("port")
            try:
                if ip and port is not None:
                    addr = (ip, int(port))
            except Exception:
                addr = None

    # If not found in payload, maybe emitter passed (payload, addr) style
    if not rover_id and maybe_addr and isinstance(maybe_addr, (tuple, list)) and len(maybe_addr) >= 2:
        if isinstance(payload, str):
            rover_id = payload

    # If maybe_addr could be an address and we didn't parse it yet
    if addr is None and isinstance(maybe_addr, (tuple, list)) and len(maybe_addr) >= 2:
        try:
            addr = (maybe_addr[0], int(maybe_addr[1]))
        except Exception:
            addr = None

    # Normalize rover_id if it is nested dict
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

    def _core_register(rover_id: Optional[str], addr: Optional[tuple]):
        if not rover_id:
            return
        try:
            # Try preferred kwarg signature first
            try:
                mission_store.register_rover(rover_id, address=addr)
                logger.info("telemetry_hooks: registered rover %s addr=%s", rover_id, addr)
                return
            except TypeError:
                pass
            # Fallback positional
            try:
                mission_store.register_rover(rover_id, addr)
                logger.info("telemetry_hooks: registered rover %s addr=%s", rover_id, addr)
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

    def _universal_handler(*args, **kwargs):
        """
        Universal wrapper that tolerates different hook signatures:
          - (event_type, payload)
          - (payload,)
          - (payload, addr)
          - (event_type, payload, addr, ...)
        Extract payload and possible addr then call core register.
        """
        try:
            event_type = None
            payload = None
            maybe_addr = None

            if len(args) >= 2:
                # common: (event_type, payload) or (payload, addr)
                # Decide by type of first arg
                if isinstance(args[0], str) and isinstance(args[1], dict):
                    event_type = args[0]
                    payload = args[1]
                    if len(args) > 2:
                        maybe_addr = args[2]
                elif isinstance(args[0], dict):
                    payload = args[0]
                    maybe_addr = args[1]
                else:
                    # fallback: treat second arg as payload if dict
                    if isinstance(args[1], dict):
                        payload = args[1]
                        maybe_addr = args[2] if len(args) > 2 else None
                    else:
                        payload = args[0]
            elif len(args) == 1:
                payload = args[0]
            else:
                # try kwargs
                payload = kwargs.get("payload") or kwargs.get("data") or kwargs.get("telemetry")
                maybe_addr = kwargs.get("addr") or kwargs.get("address")

            # If event_type present and it's not telemetry, ignore unless payload contains telemetry
            if event_type and event_type.lower() not in ("telemetry", "line", "data", "hello", "heartbeat"):
                # still try to extract payload if present
                pass

            rover_id, addr = _extract_rover_and_addr(payload, maybe_addr)
            _core_register(rover_id, addr)
        except Exception:
            logger.exception("telemetry_hooks: unexpected error in universal handler")

    attached = False
    try:
        # Try classic evented APIs first
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
                                    _core_register(*_extract_rover_and_addr(payload, maybe_addr))
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