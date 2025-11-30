#!/usr/bin/env python3
"""
WebSocket listener de diagnóstico.

Uso (PowerShell):
  $env:PYTHONPATH="src"; python src\scripts\ws_listener.py

Mostra todas as mensagens JSON/texto que a API enviar pelo /ws/subscribe.
Requer a biblioteca 'websockets' (pip install websockets).
"""
import asyncio
import json
import sys
from common import config

try:
    import websockets
except Exception:
    print("Instala websockets: pip install websockets")
    raise

WS_URL = f"ws://{('127.0.0.1' if config.API_HOST == '0.0.0.0' else config.API_HOST)}:{config.API_PORT}/ws/subscribe"

async def listen():
    print("Connecting to", WS_URL)
    try:
        async with websockets.connect(WS_URL) as ws:
            print("Connected. Listening for messages (CTRL+C to quit)...")
            while True:
                msg = await ws.recv()
                # tenta parsear JSON, senão imprime raw
                try:
                    data = json.loads(msg)
                    print("RECV JSON:", json.dumps(data, indent=2, ensure_ascii=False))
                except Exception:
                    print("RECV RAW:", repr(msg))
    except Exception as e:
        print("Connection error:", e)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(listen())
    except KeyboardInterrupt:
        print("Stopped by user")
    except Exception as e:
        print("Listener error:", e)
        sys.exit(1)