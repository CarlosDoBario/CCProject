#!/usr/bin/env python3
"""
start_all.py - arranca TelemetryServer + API (uvicorn) no mesmo event loop.

Uso:
  $env:PYTHONPATH="src"; python src\scripts\start_all.py
"""
import asyncio
import time
import signal
import sys
from common import config as cfg, utils
from nave_mae import api_server
from nave_mae.telemetry_launcher import start_telemetry_server

logger = utils.get_logger("start_all")

async def main():
    # 1) Arranca o TelemetryServer (assíncrono)
    services = await start_telemetry_server(host=cfg.TELEMETRY_HOST, port=cfg.TELEMETRY_PORT)
    ms = services["ms"]
    ts = services["ts"]
    srv = services["telemetry_server"]
    logger.info("TelemetryServer started: %s", srv)

    # 2) Injeta os stores no módulo api_server (mesma memória/processo)
    api_server.setup_stores(ms, ts)

    # 3) Arranca o uvicorn ASGI server no mesmo loop (usa Server.serve() que é awaitable)
    try:
        import uvicorn
    except Exception:
        logger.exception("uvicorn não encontrado. Instala: pip install uvicorn")
        raise

    uv_conf = uvicorn.Config(api_server.app, host=cfg.API_HOST, port=cfg.API_PORT,
                             log_level=cfg.LOG_LEVEL.lower(), access_log=False)
    server = uvicorn.Server(uv_conf)

    # Executar server.serve() como tarefa para que possamos continuar a correr no mesmo loop
    server_task = asyncio.create_task(server.serve())
    logger.info("uvicorn Server started in current event loop on %s:%d", uv_conf.host, uv_conf.port)

    # Espera até que o api_server capture o seu api_loop (startup do FastAPI)
    timeout = 6.0
    t0 = time.time()
    while getattr(api_server, "api_loop", None) is None and (time.time() - t0) < timeout:
        await asyncio.sleep(0.05)

    if getattr(api_server, "api_loop", None) is None:
        logger.warning("API loop not ready after %.1fs; register hooks anyway", timeout)
    else:
        logger.info("API loop ready; registering broadcast hooks")
    api_server.register_broadcast_hook()

    # Mantém o programa a correr até receber SIGINT/SIGTERM
    stop_event = asyncio.Event()
    def _on_sig(sig, frame):
        logger.info("Signal received; stopping")
        stop_event.set()
    signal.signal(signal.SIGINT, _on_sig)
    signal.signal(signal.SIGTERM, _on_sig)

    await stop_event.wait()

    # Shutdown: parar uvicorn e TelemetryServer
    try:
        # signal the uvicorn server to exit
        server.should_exit = True
        await server_task
    except Exception:
        logger.exception("Error stopping uvicorn server")

    try:
        await srv.stop()
    except Exception:
        logger.exception("Error stopping telemetry server")

    pers = services.get("persister")
    if pers:
        try:
            pers.stop(wait=True)
        except Exception:
            pass

    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())