#!/usr/bin/env python3
"""
Wrapper para correr ml_client com logging activo (PowerShell friendly).
Uso:
  $env:PYTHONPATH="src"; python src/scripts/run_ml_client_wrapper.py --rover-id Rover1 --server 127.0.0.1 --port 64070
"""
import logging
import sys

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

# Pass-through of CLI args to rover.ml_client.main (which reads sys.argv)
if len(sys.argv) == 1:
    print("Usage: python run_ml_client_wrapper.py --rover-id <id> --server <host> --port <port>")
    sys.exit(1)

# Prepare argv for the imported main()
sys.argv = ["ml_client"] + sys.argv[1:]

from rover.ml_client import main

if __name__ == "__main__":
    main()