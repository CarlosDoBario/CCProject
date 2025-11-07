#!/usr/bin/env python3
"""
run_demo.py

Run a visual demo of the MissionLink server + one client, streaming logs live.

Usage:
  PYTHONPATH=src python src/scripts/run_demo.py --rover-id R-001 --port 50000 --exit-on-complete

This script runs the server and client as subprocesses and prefixes their stdout/stderr
so you can follow the interaction live in a single terminal.

Note: ml_server.py currently reads ML_UDP_PORT from config; this script starts server
as a subprocess with environment PYTHONPATH so it will bind to the configured port.
"""
import os
import sys
import subprocess
import threading
import argparse
import time

PREFIX_COLORS = {
    "SERVER": "\033[1;34m",   # blue
    "CLIENT": "\033[1;32m",   # green
    "RESET": "\033[0m",
}


def forward_stream(prefix: str, stream, is_err: bool = False):
    out = sys.stderr if is_err else sys.stdout
    color = PREFIX_COLORS.get(prefix.split("-")[0], "")
    reset = PREFIX_COLORS["RESET"]
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            out.write(f"{color}[{prefix}] {line.rstrip()}{reset}\n")
            out.flush()
    finally:
        try:
            stream.close()
        except Exception:
            pass


def start_process(cmd, env=None):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rover-id", default="R-001")
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50000)
    parser.add_argument("--exit-on-complete", action="store_true")
    args = parser.parse_args()

    # Ensure PYTHONPATH contains src
    env = os.environ.copy()
    if "PYTHONPATH" not in env or "src" not in env["PYTHONPATH"].split(os.pathsep):
        env["PYTHONPATH"] = os.pathsep.join(filter(None, ["src", env.get("PYTHONPATH", "")]))

    server_cmd = [sys.executable, "-u", os.path.join("src", "nave_mae", "ml_server.py")]
    client_cmd = [sys.executable, "-u", os.path.join("src", "rover", "ml_client.py"),
                  "--rover-id", args.rover_id, "--server", args.server_host, "--port", str(args.port)]
    if args.exit_on_complete:
        client_cmd.append("--exit-on-complete")

    print("Starting server...")
    server = start_process(server_cmd, env=env)
    t_s_out = threading.Thread(target=forward_stream, args=("SERVER", server.stdout), daemon=True)
    t_s_err = threading.Thread(target=forward_stream, args=("SERVER", server.stderr, True), daemon=True)
    t_s_out.start()
    t_s_err.start()

    time.sleep(0.5)  # let server boot

    print("Starting client...")
    client = start_process(client_cmd, env=env)
    t_c_out = threading.Thread(target=forward_stream, args=(f"CLIENT-{args.rover_id}", client.stdout), daemon=True)
    t_c_err = threading.Thread(target=forward_stream, args=(f"CLIENT-{args.rover_id}", client.stderr, True), daemon=True)
    t_c_out.start()
    t_c_err.start()

    try:
        # Wait for client to exit if exit_on_complete was used, else run until KeyboardInterrupt
        if args.exit_on_complete:
            client.wait()
            time.sleep(0.2)
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted, shutting down...")
    finally:
        for p in (client, server):
            try:
                if p and p.poll() is None:
                    p.terminate()
                    p.wait(timeout=2)
            except Exception:
                pass


if __name__ == "__main__":
    main()