#!/usr/bin/env python3
"""
tests/test_telemetry_persistence.py

Integration test that demonstrates NDJSON persistence of telemetry via a simple hook.

This test:
 - starts an in-process TelemetryServer (via telemetry_launcher.start_telemetry_server)
 - attaches a small hook to the TelemetryStore that appends canonical records as NDJSON
   lines into a daily file under a temporary directory
 - sends a few telemetry messages using rover.telemetry_client.send_once
 - asserts that the NDJSON file was created and contains the expected records

This test purposefully keeps the persistence implementation simple and local to the test,
so it does not depend on scripts/persist_telemetry.py being present.
"""
import asyncio
import json
import time
from pathlib import Path

import pytest

from common import binary_proto
from nave_mae.telemetry_launcher import start_telemetry_server
from rover.telemetry_client import send_once


@pytest.mark.asyncio
async def test_telemetry_persistence_ndjson(tmp_path):
    outdir = tmp_path / "telemetry"
    outdir.mkdir(parents=True, exist_ok=True)

    # Start telemetry server in-process and obtain TelemetryStore
    services = await start_telemetry_server(mission_store=None, telemetry_store=None, host="127.0.0.1", port=0, persist_file=None)
    server = services.get("telemetry_server")
    ts = services.get("ts")
    assert server is not None, "TelemetryServer not started by telemetry_launcher"
    assert ts is not None, "TelemetryStore not started by telemetry_launcher"

    sock = server._server.sockets[0]
    host, port = sock.getsockname()[:2]

    # Create a simple NDJSON hook that writes records to the outdir based on timestamp date
    def simple_persist_hook(event_type: str, payload: dict):
        try:
            # payload shape often {"rover_id": rid, "telemetry": {...}}
            if isinstance(payload, dict) and "telemetry" in payload and isinstance(payload["telemetry"], dict):
                tel = dict(payload["telemetry"])
                rid = payload.get("rover_id") or tel.get("rover_id") or "<unknown>"
            elif isinstance(payload, dict) and "rover_id" in payload and "telemetry" not in payload:
                rid = payload.get("rover_id")
                tel = dict(payload)
            else:
                rid = payload.get("rover_id") if isinstance(payload, dict) else "<unknown>"
                tel = payload if isinstance(payload, dict) else {"value": str(payload)}

            ts_ms = int(tel.get("ts_ms") or tel.get("timestamp_ms") or int(time.time() * 1000))
            rec = {
                "ts_ms": ts_ms,
                "rover_id": rid,
                "position": tel.get("position"),
                "progress_pct": tel.get("progress_pct"),
                "battery_level_pct": tel.get("battery_level_pct"),
                "status": tel.get("status"),
                "payload_json": tel,
            }
            ymd = time.strftime("%Y%m%d", time.gmtime(ts_ms / 1000.0))
            fname = outdir / f"telemetry-{ymd}.ndjson"
            # append JSON line (sync, small writes acceptable for test)
            with open(fname, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            # In tests, we want errors to surface; re-raise to fail early
            raise

    # Register hook on TelemetryStore if API available
    if hasattr(ts, "register_hook"):
        ts.register_hook(simple_persist_hook)
    else:
        # If the TelemetryStore implementation doesn't support hooks, skip the test
        pytest.skip("TelemetryStore.register_hook API not available in this implementation")

    # Send telemetry messages for three distinct rover ids
    rover_ids = ["R-P-1", "R-P-2", "R-P-3"]
    for rid in rover_ids:
        telemetry = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": 1.0, "y": 2.0, "z": 0.0},
            "battery_level_pct": 75,
            "status": "IN_MISSION",
            "progress_pct": 10.0,
        }
        await send_once(host, port, rid, telemetry, ack_requested=False, include_crc=False)
        # tiny pause to allow server to process and invoke hooks
        await asyncio.sleep(0.05)

    # Allow some time for hook writes to complete
    await asyncio.sleep(0.2)

    # Determine the expected NDJSON filename for today
    ymd_now = time.strftime("%Y%m%d", time.gmtime(time.time()))
    fname = outdir / f"telemetry-{ymd_now}.ndjson"
    assert fname.exists(), f"Expected NDJSON file {fname} to exist"

    # Read last lines and validate content
    text = fname.read_text(encoding="utf-8").strip()
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) >= 3, f"Expected at least 3 persisted lines, found {len(lines)}"

    # Parse last three lines and ensure rover_ids present
    found_ids = set()
    for ln in lines[-3:]:
        obj = json.loads(ln)
        found_ids.add(obj.get("rover_id"))
    for rid in rover_ids:
        assert rid in found_ids, f"Persisted file missing telemetry for rover {rid}"

    # Cleanup: stop server
    await server.stop()