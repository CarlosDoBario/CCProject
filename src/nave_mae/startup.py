#!/usr/bin/env python3
"""
startup.py

Helpers para iniciar a Nave‑Mãe com TelemetryServer integrado.

- start_services(...) cria MissionStore e TelemetryStore, regista hooks e inicia o TelemetryServer.
- run_forever(...) é um CLI que arranca os serviços e bloqueia até Ctrl-C.
- try_start_ml_server(...) tenta iniciar o ML server se o módulo nave_mae.ml_server expuser uma função pública
  compatível (por exemplo run_server, main, start_server). A chamada é feita em background (thread ou task)
  para não bloquear a coroutine que chamou start_services.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import inspect
from typing import Optional, Dict, Any 
from common import config, utils

logger = utils.get_logger("ml.startup")


def _safe_import(name: str):
    try:
        mod = __import__(name, fromlist=["*"])
        return mod
    except Exception:
        return None

_API_MOD = _safe_import("nave_mae.api_server")

async def start_services(
    host: str = "127.0.0.1",
    telemetry_port: int = None,
    start_ml_server: bool = False,
    ml_host: str = "127.0.0.1",
    ml_port: Optional[int] = None,
    persist_file: Optional[str] = None,
    start_api_server: bool = True, # <-- VARIÁVEL UTILIZADA AQUI
) -> Dict[str, Any]:
    """
    Create MissionStore and TelemetryStore, register hooks and start TelemetryServer.
    Optionally attempt to start the ML server if start_ml_server=True and a suitable entry is found.

    Returns a dict:
      { "ms": MissionStore, "ts": TelemetryStore, "telemetry_server": TelemetryServer, "ml_server": ml_server_handle_or_None, "api_server": api_server_handle_or_None }

    ml_server_handle may be:
      - an asyncio.Task if the entrypoint was a coroutine and was scheduled on the loop
      - a threading.Thread (daemon) if a blocking sync entrypoint was started in background
      - None if no ml server was started
    """
    telemetry_port = telemetry_port if telemetry_port is not None else config.TELEMETRY_PORT

    # Lazy imports to avoid circular import issues and to let telemetry_launcher handle details
    from nave_mae.telemetry_launcher import start_telemetry_server  # type: ignore

    ms = None
    ts = None
    telemetry_server = None
    ml_server_handle = None
    api_server_handle = None # Inicializamos o handle da API

    # Try to create MissionStore early (start_telemetry_server will also create one if None)
    try:
        try:
            from nave_mae.mission_store import MissionStore  # type: ignore
        except Exception:
            MissionStore = None

        if MissionStore is not None:
            try:
                ms = MissionStore(persist_file=persist_file) if persist_file else MissionStore()
            except TypeError:
                ms = MissionStore()
    except Exception:
        logger.exception("Failed to create MissionStore in startup.start_services")
        ms = None

    # Start telemetry server via telemetry_launcher helper (it will create stores if ms/ts is None)
    try:
        services = await start_telemetry_server(mission_store=ms, telemetry_store=None, host=host, port=telemetry_port, persist_file=persist_file)
        ms = services.get("ms")
        ts = services.get("ts")
        telemetry_server = services.get("telemetry_server")
    except Exception:
        logger.exception("Failed to start telemetry server via startup.start_services")
        raise
    
    if _API_MOD and ms and ts:
        try:
            _API_MOD.setup_stores(ms, ts)
            _API_MOD.register_broadcast_hook()
            logger.info("API broadcast hooks registrados e stores injetados.")
        except Exception:
            logger.exception("Falha ao registrar broadcast hooks.")

    # Optionally try to start ML server module if requested (best-effort; non-fatal)
    if start_ml_server:
        ml_mod = _safe_import("nave_mae.ml_server")
        if ml_mod is None:
            logger.warning("nave_mae.ml_server module not found; skipping ML server auto-start")
        else:
            started = False
            # Candidate entrypoints to try
            for attr in ("run_server", "start_server", "main", "serve"):
                fn = getattr(ml_mod, attr, None)
                if callable(fn):
                    try:
                        logger.info("Attempting to start ML server using %s.%s", ml_mod.__name__, attr)
                        # If coroutine function -> schedule it as asyncio task
                        if inspect.iscoroutinefunction(fn):
                            try:
                                # try to call with host/port if supported
                                kwargs = {}
                                if "host" in fn.__code__.co_varnames:
                                    kwargs["listen_host"] = ml_host
                                if "port" in fn.__code__.co_varnames:
                                    kwargs["listen_port"] = ml_port if ml_port is not None else config.ML_UDP_PORT
                                task = asyncio.create_task(fn(**kwargs) if kwargs else fn())
                                ml_server_handle = task
                                logger.info("ML server coroutine scheduled as task")
                            except Exception:
                                logger.exception("Failed to schedule ML server coroutine")
                                ml_server_handle = None
                        else:
                            # blocking sync function: run in a daemon thread so we don't block caller
                            def _run_blocking_server(callable_fn, host_arg, port_arg):
                                try:
                                    # try to call with common param names
                                    try:
                                        # prefer (listen_host, listen_port) signature if present
                                        if "listen_host" in callable_fn.__code__.co_varnames and "listen_port" in callable_fn.__code__.co_varnames:
                                            callable_fn(listen_host=host_arg, listen_port=port_arg)
                                        elif "host" in callable_fn.__code__.co_varnames and "port" in callable_fn.__code__.co_varnames:
                                            callable_fn(host=host_arg, port=port_arg)
                                        else:
                                            # try calling with positional args (host, port)
                                            try:
                                                callable_fn(host_arg, port_arg)
                                            except TypeError:
                                                # last resort: call without args
                                                callable_fn()
                                    except Exception:
                                        # try calling without args
                                        try:
                                            callable_fn()
                                        except Exception:
                                            logger.exception("ML server function raised when called in background")
                                except Exception:
                                    logger.exception("Uncaught exception while running ML server in background thread")

                            th = threading.Thread(target=_run_blocking_server, args=(fn, ml_host, ml_port if ml_port is not None else config.ML_UDP_PORT), daemon=True)
                            th.start()
                            ml_server_handle = th
                            logger.info("ML server started in background thread")
                        started = True
                        break
                    except Exception:
                        logger.exception("Failed to start ML server using %s.%s", ml_mod.__name__, attr)
            if not started:
                logger.warning("Could not find runnable entrypoint in nave_mae.ml_server; ML server not started automatically")

    # -------------------------------------------------------------------
    # CORREÇÃO AQUI: Lançar Servidor API (FastAPI) dentro da função
    # -------------------------------------------------------------------
    if start_api_server and _API_MOD:
        try:
            # Uvicorn (FastAPI) é um servidor síncrono, lançamos numa Thread Daemon
            def _run_api():
                _API_MOD.run_api_server(host=config.API_HOST, port=config.API_PORT)
                
            th = threading.Thread(target=_run_api, name="api-server-thread", daemon=True)
            th.start()
            api_server_handle = th
            logger.info("API Server (FastAPI) iniciado em thread.")
        except Exception:
            logger.exception("Falha ao iniciar API Server")
            
    # -------------------------------------------------------------------
    # RETORNO FINAL: Incluir o handle do API Server
    # -------------------------------------------------------------------
    return {
        "ms": ms, 
        "ts": ts, 
        "telemetry_server": telemetry_server, 
        "ml_server": ml_server_handle,
        "api_server": api_server_handle # <-- NOVO
    }


def run_forever(host: str = "127.0.0.1", telemetry_port: int = None, start_ml_server: bool = False, ml_host: str = "127.0.0.1", ml_port: Optional[int] = None, persist_file: Optional[str] = None, start_api_server: bool = True):
    """
    CLI helper: start services and block until Ctrl-C.
    """
    telemetry_port = telemetry_port if telemetry_port is not None else config.TELEMETRY_PORT

    # configure logging early
    try:
        config.configure_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    services = {}
    try:
        # start_services agora gere o arranque da API com base em start_api_server
        services = loop.run_until_complete(start_services(host=host, telemetry_port=telemetry_port, start_ml_server=start_ml_server, ml_host=ml_host, ml_port=ml_port, persist_file=persist_file, start_api_server=start_api_server))
        logger.info("All services started; press Ctrl-C to stop")
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutdown requested (KeyboardInterrupt)")
    finally:
        try:
            server = services.get("telemetry_server")
            if server:
                loop.run_until_complete(server.stop())
        except Exception:
            logger.exception("Error while stopping telemetry server")
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start Nave-Mãe services (TelemetryServer + optional ML server)")
    parser.add_argument("--host", default="127.0.0.1", help="Telemetry server host")
    parser.add_argument("--port", type=int, default=config.TELEMETRY_PORT, help="Telemetry server port")
    parser.add_argument("--start-ml", action="store_true", help="Attempt to auto-start the ML UDP server if available")
    parser.add_argument("--ml-host", default="127.0.0.1", help="ML server host (if auto-started)")
    parser.add_argument("--ml-port", type=int, default=None, help="ML server port (if auto-started)")
    parser.add_argument("--persist-file", default=None, help="Optional persist file to pass to MissionStore (standalone)")
    args = parser.parse_args()
    run_forever(host=args.host, telemetry_port=args.port, start_ml_server=args.start_ml, ml_host=args.ml_host, ml_port=args.ml_port, persist_file=args.persist_file)