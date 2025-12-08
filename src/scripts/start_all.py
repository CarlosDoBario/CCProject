#!/usr/bin/env python3
"""
start_all.py - arranca TelemetryServer + API (uvicorn) no mesmo event loop.

Uso:
  $env:PYTHONPATH="src"; python src/scripts/start_all.py [--create-demo]

Opções:
  --create-demo    cria missões demo no MissionStore após arrancar os serviços
"""
import asyncio
import time
import signal
import sys
import argparse
from common import config as cfg, utils
from nave_mae.ml_server import MLServerProtocol
from nave_mae import api_server
from nave_mae.telemetry_launcher import start_telemetry_server

logger = utils.get_logger("start_all")

async def main(create_demo: bool = False):
    # 1) Arranca o TelemetryServer (assíncrono)
    services = await start_telemetry_server(host=cfg.TELEMETRY_HOST, port=cfg.TELEMETRY_PORT)
    ms = services["ms"]
    ts = services["ts"]
    srv = services["telemetry_server"]
    logger.info("TelemetryServer started: %s", srv)

    # Opcional: criar missões demo se solicitado (ou se o store estiver vazio)
    try:
        if create_demo:
            # use the helper if available
            try:
                ms.create_demo_missions()
                logger.info("Demo missions created in MissionStore")
            except Exception:
                # fallback: create a single demo mission
                try:
                    ms.create_mission({
                        "mission_id": f"M-DEMO-{int(time.time())}",
                        "task": "collect_samples",
                        "params": {"sample_count": 3},
                        "priority": 1
                    })
                    logger.info("Single demo mission created in MissionStore")
                except Exception:
                    logger.exception("Failed to create demo mission(s)")
    except Exception:
        logger.exception("Error while creating demo missions (continuing)")

    # 1.5) ARRANCAR O ML SERVER (UDP)
    try:
        loop = asyncio.get_event_loop()
        ml_transport, ml_protocol = await loop.create_datagram_endpoint(
            lambda: MLServerProtocol(mission_store=ms),
            local_addr=(cfg.ML_HOST, cfg.ML_UDP_PORT)
        )
        logger.info("ML Server (UDP) started on %s:%d", cfg.ML_HOST, cfg.ML_UDP_PORT)
        services["ml_transport"] = ml_transport
        services["ml_protocol"] = ml_protocol
    except Exception:
        logger.exception("Falha ao arrancar o ML Server")
        raise

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

    # Stop ML transport if present
    try:
        ml_proto = services.get("ml_protocol")
        if ml_proto and hasattr(ml_proto, "stop"):
            await ml_proto.stop(wait_timeout=2.0)
        ml_transport = services.get("ml_transport")
        if ml_transport:
            ml_transport.close()
    except Exception:
        logger.exception("Error stopping ML server")

    logger.info("Shutdown complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start TelemetryServer + API + optional ML demo missions")
    parser.add_argument("--create-demo", action="store_true", help="Create demo missions in MissionStore on startup")
    args = parser.parse_args()
    asyncio.run(main(create_demo=args.create_demo))