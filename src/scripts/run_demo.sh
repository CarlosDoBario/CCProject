#!/usr/bin/env bash
export PYTHONPATH=src
# start server in background
python3 src/nave_mae/ml_server.py &
SERVER_PID=$!
echo "Server started (PID $SERVER_PID), sleeping 1s"
sleep 1
# create demo missions via small python one-liner
python3 - <<'PY'
from nave_mae.mission_store import MissionStore
ms = MissionStore()
ms.create_demo_missions()
print("Demo missions created")
PY
# start a client
python3 src/rover/ml_client.py --rover-id R-001 --server 127.0.0.1 --port 50000
# when done kill server
kill $SERVER_PID