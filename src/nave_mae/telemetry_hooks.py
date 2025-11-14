"""
Helper to connect TelemetryStore -> MissionStore.

Provides a small, safe hook that you can register in the TelemetryStore so that every
telemetry update:
 - registers/updates the rover in the MissionStore (last_seen/address),
 - optionally updates the rover 'state' in the MissionStore._rovers record (best-effort),
 - emits a mission_store event ("telemetry_received") so other parts of the system can react.

Usage (example at Nave-Mãe startup or in tests):

    from nave_mae.telemetry_store import TelemetryStore
    from nave_mae.telemetry_hooks import register_telemetry_hooks

    ts = TelemetryStore()
    ms = MissionStore()
    register_telemetry_hooks(ms, ts)

Notes:
 - TelemetryStore.register_hook accepts sync or async callables. We register a sync hook
   (simple function) so it executes quickly and non-blocking behavior is preserved by the store.
 - This file intentionally uses MissionStore public API where possible (register_rover, _emit).
   It touches mission_store._rovers to set the rover state if telemetry includes a state field;
   that is a pragmatic choice to keep the MissionStore in sync with telemetry. If you prefer to
   avoid private attribute access, remove that part and rely on listeners of "telemetry_received".
"""

from typing import Any, Dict
import logging

logger = logging.getLogger("ml.telemetry_hooks")


def register_telemetry_hooks(mission_store, telemetry_store) -> None:
    """
    Register the default hook(s) that forward telemetry updates into the MissionStore.

    mission_store: instance of nave_mae.mission_store.MissionStore
    telemetry_store: instance of TelemetryStore (has register_hook(fn))
    """
    def _hook(event_type: str, payload: Dict[str, Any]) -> None:
        # We expect event_type == "telemetry" and payload {"rover_id": str, "telemetry": dict}
        try:
            if event_type != "telemetry":
                return
            rover_id = payload.get("rover_id")
            telemetry = payload.get("telemetry", {}) or {}

            if not rover_id:
                logger.debug("telemetry hook called with no rover_id; ignoring")
                return

            # Ensure MissionStore knows about this rover and update last_seen / address
            try:
                # mission_store.register_rover will update last_seen and address if provided
                # We don't have a network address here (the TelemetryServer already registers address),
                # so call register_rover with rover_id only to refresh last_seen and ensure an entry exists.
                mission_store.register_rover(rover_id)
            except Exception:
                logger.exception("Failed to register rover %s in MissionStore from telemetry", rover_id)

            # If telemetry contains an operational state, reflect it in the MissionStore rovers table.
            # This is a convenience to keep rover state visible alongside missions. It's best-effort.
            try:
                state = telemetry.get("state")
                if state:
                    # MissionStore exposes get_rover() and register_rover(); use those where possible,
                    # but to set state we update the internal _rovers dict (pragmatic).
                    # This keeps the existing MissionStore API unchanged while synchronizing state.
                    rv = mission_store.get_rover(rover_id)
                    if rv is not None:
                        # set the explicit state value
                        mission_store._rovers.setdefault(rover_id, {})["state"] = state
                    else:
                        # if no entry exists, register_rover already created one; set state anyway
                        mission_store._rovers.setdefault(rover_id, {})["state"] = state
            except Exception:
                logger.exception("Failed to update rover state in MissionStore for %s", rover_id)

            # Emit a telemetry-specific event via the MissionStore so other hooks can react
            try:
                mission_store._emit("telemetry_received", {"rover_id": rover_id, "telemetry": telemetry})
            except Exception:
                logger.exception("Failed to emit telemetry_received for %s", rover_id)

        except Exception:
            logger.exception("Unhandled exception in telemetry -> mission store hook")

    # Register the hook with the telemetry store
    telemetry_store.register_hook(_hook)
    logger.info("Registered telemetry -> MissionStore hook")