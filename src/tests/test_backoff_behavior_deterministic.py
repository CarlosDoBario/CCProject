#!/usr/bin/env python3
"""
tests/test_backoff_behavior_deterministic.py

Deterministic unit test for TelemetryClient reconnection backoff logic.

Strategy:
- Monkeypatch common.utils.exponential_backoff() to return a deterministic generator
  that yields known delays (e.g. [0.01, 0.02, 0.03]).
- Monkeypatch random.uniform to return 0.0 so no jitter is added.
- Monkeypatch asyncio.open_connection to always raise ConnectionRefusedError so the client
  fails to connect and exercises the backoff path.
- Monkeypatch asyncio.sleep to a fake coroutine that records the durations requested and
  returns immediately (using the original asyncio.sleep for a brief yield).
- Start the TelemetryClient in persistent mode (client.start()) and wait until the
  expected number of backoff sleeps have been requested, then stop the client and assert
  the recorded delays match the deterministic generator.
"""
import time
import asyncio
import random
import pytest

from common import utils
from rover.telemetry_client import TelemetryClient


@pytest.mark.asyncio
async def test_backoff_behavior_deterministic(monkeypatch):
    # deterministic delays to be yielded by the backoff generator
    delays = [0.01, 0.02, 0.03]

    # Replace exponential_backoff with a generator that yields our deterministic delays
    def make_gen(base=0.0, factor=0.0, max_delay=0.0):
        for d in delays:
            yield d
        while True:
            yield delays[-1]

    monkeypatch.setattr(utils, "exponential_backoff", lambda base, factor, max_delay: make_gen(base, factor, max_delay))

    # Remove jitter by forcing random.uniform to return 0.0
    monkeypatch.setattr(random, "uniform", lambda a, b: 0.0)

    # Save original asyncio.sleep to use inside the fake sleep and in wait loops
    orig_sleep = asyncio.sleep

    recorded = []

    async def fake_sleep(duration):
        # record the requested duration and yield control briefly using original sleep
        recorded.append(duration)
        await orig_sleep(0)

    # Patch open_connection so connection attempt always fails (force backoff path)
    async def fake_open_connection(*args, **kwargs):
        raise ConnectionRefusedError("forced failure for test")

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    client = TelemetryClient(rover_id="R-BACK", host="127.0.0.1", port=12345, interval_s=0.0, reconnect=True, backoff_base=0.1, backoff_factor=2.0)

    try:
        # start background runner
        await client.start()

        # wait until we've recorded at least the number of expected delays, or timeout
        deadline = time.time() + 2.0
        while time.time() < deadline and len(recorded) < len(delays):
            # use original sleep to avoid interfering with recorded list
            await orig_sleep(0.01)

        assert len(recorded) >= len(delays), f"Expected at least {len(delays)} backoff sleeps, recorded: {recorded}"

        # Because we forced random.uniform to 0.0, the sleep durations should match delays exactly
        # Only compare the first N recorded entries (there may be extra sleeps from cleanup)
        for i, expected in enumerate(delays):
            # allow small floating point tolerance
            assert abs(recorded[i] - expected) < 1e-6, f"Backoff delay mismatch at index {i}: expected {expected}, got {recorded[i]}"

    finally:
        # restore and cleanup client
        # stop() will cancel the background task and call _disconnect
        try:
            await client.stop()
        except Exception:
            pass