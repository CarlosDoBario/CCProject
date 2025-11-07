#!/usr/bin/env python3
"""
tests/test_retransmit_exhaustion.py

Unit test that validates server behavior when pending_outgoing retransmissions exhaust:
- Create mission and assign to rover
- Insert a PendingOutgoing with attempts already > N_RETX and next_timeout in the past
- Run server._retransmit_loop for a short while and assert mission assignment was reverted
  (mission.assigned_rover == None) and that history contains ASSIGN_FAILED.
"""

import asyncio
import time
import socket
import pytest

from nave_mae.mission_store import MissionStore
from nave_mae.ml_server import MLServerProtocol, PendingOutgoing
from common import config


@pytest.mark.asyncio
async def test_assign_revert_on_retransmit_exhaustion():
    ms = MissionStore()
    # create a mission and assign it
    mid = ms.create_mission({"task": "collect_samples", "params": {"sample_count": 1, "depth_mm": 10}})
    # manually assign to a rover id to simulate prior assign step
    ms.assign_mission_to_rover(ms.get_mission(mid), "R-EXH")

    server = MLServerProtocol(ms)

    # create a PendingOutgoing that represents an undeliverable MISSION_ASSIGN
    po = PendingOutgoing(
        msg_id="po-test-1",
        packet=b"dummy",
        addr=("127.0.0.1", 9999),
        created_at=time.time() - 60.0,
        timeout_s=0.5,
        message_type="MISSION_ASSIGN",
        mission_id=mid,
    )
    # mark attempts exceed config.N_RETX so logic will treat as exhausted immediately
    po.attempts = config.N_RETX + 1
    po.next_timeout = time.time() - 1.0

    server.pending_outgoing[po.msg_id] = po

    # run retransmit loop for a short while and then cancel
    task = asyncio.create_task(server._retransmit_loop())
    try:
        await asyncio.sleep(0.7)  # one iteration expected
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    m = ms.get_mission(mid)
    assert m is not None
    assert m.get("assigned_rover") is None, "Mission should have been unassigned after exhaust"
    # last history entry should indicate ASSIGN_FAILED
    h = m.get("history", [])
    assert any(entry.get("type") == "ASSIGN_FAILED" for entry in h), f"History entries: {h}"