"""
startup.py

Helpers para iniciar a Nave-Mãe com TelemetryServer integrado.

- start_services(...) cria MissionStore e TelemetryStore, regista hooks e inicia o TelemetryServer.
- run_forever(...) é um pequeno CLI para arrancar os serviços manualmente.
- try_start_ml_server(...) tenta iniciar o ML server se o módulo nave_mae.ml_server expuser uma função pública
  compatível (por exemplo run_server, main, start_server). Isto é opcional e faz-se em try/except para não forçar dependências.
"""

import asyncio
import logging
from typing import Optional, Dict, Any

_logger = logging.getLogger("ml.startup")


def _safe_import(name: str):
    try:
        mod = __import__(name, fromlist=["*"])
        return mod
    except Exception:
        return None


async def start_services(
    host: str = "127.0.0.1",
    telemetry_port: int = 65080,
    start_ml_server: bool = False,
    ml_host: str = "127.0.0.1",
    ml_port: Optional[int] = None,
    persist_file: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create MissionStore and TelemetryStore, register hooks and start TelemetryServer.
    Optionally attempt to start the ML server if start_ml_server=True and a suitable entry is found.

    Returns a dict:
      { "ms": MissionStore, "ts": TelemetryStore, "telemetry_server": TelemetryServer, "ml_server": ml_server_obj_or_None }
    """
    # Lazy imports to avoid circular import issues
    from nave_mae.telemetry_launcher import start_telemetry_server
    ms = None
    ts = None
    telemetry_server = None
    ml_server_obj = None

    # If user wants the telemetry server integrated, create MissionStore/TelemetryStore in this function
    try:
        # Try to import MissionStore (if available)
        try:
            from nave_mae.mission_store import MissionStore  # type: ignore
        except Exception:
            MissionStore = None  # fallback handled below

        if MissionStore is not None:
            try:
                ms = MissionStore(persist_file=persist_file) if persist_file else MissionStore()
            except TypeError:
                # MissionStore constructor may not accept persist_file
                ms = MissionStore()
        else:
            # If MissionStore module missing, raise: telemetry_launcher.start_telemetry_server will try too.
            ms = None

    except Exception:
        _logger.exception("Failed to create MissionStore in startup.start_services")
        ms = None

    # If start_telemetry_server expects mission_store None it will create its own (standalone mode)
    try:
        services = await start_telemetry_server(mission_store=ms, telemetry_store=None, host=host, port=telemetry_port, persist_file=persist_file)
        # services is expected to contain "ms", "ts", "telemetry_server"
        ms = services.get("ms")
        ts = services.get("ts")
        telemetry_server = services.get("telemetry_server")
    except Exception:
        _logger.exception("Failed to start telemetry server via startup.start_services")
        raise

    # Optionally try to start ML server module if requested (best-effort; non-fatal)
    if start_ml_server:
        ml_mod = _safe_import("nave_mae.ml_server")
        if ml_mod is None:
            _logger.warning("nave_mae.ml_server module not found; skipping ML server auto-start")
        else:
            # Try common entrypoints in order of preference
            started = False
            for attr in ("run_server", "start_server", "main", "serve"):
                fn = getattr(ml_mod, attr, None)
                if callable(fn):
                    try:
                        _logger.info("Attempting to start ML server using %s.%s", ml_mod.__name__, attr)
                        if asyncio.iscoroutinefunction(fn):
                            ml_server_obj = asyncio.create_task(fn(host=ml_host, port=ml_port) if ("host" in fn.__code__.co_varnames or "port" in fn.__code__.co_varnames) else fn())
                        else:
                            loop = asyncio.get_event_loop()
                            ml_server_obj = await loop.run_in_executor(None, lambda: fn(host=ml_host, port=ml_port) if ("host" in fn.__code__.co_varnames or "port" in fn.__code__.co_varnames) else fn())
                        started = True
                        _logger.info("ML server startup function %s.%s executed", ml_mod.__name__, attr)
                        break
                    except Exception:
                        _logger.exception("Failed to start ML server using %s.%s", ml_mod.__name__, attr)
            if not started:
                _logger.warning("Could not find runnable entrypoint in nave_mae.ml_server; ML server not started automatically")

    return {"ms": ms, "ts": ts, "telemetry_server": telemetry_server, "ml_server": ml_server_obj}


def run_forever(host: str = "127.0.0.1", telemetry_port: int = 65080, start_ml_server: bool = False, ml_host: str = "127.0.0.1", ml_port: Optional[int] = None, persist_file: Optional[str] = None):
    """
    CLI helper: start services and block until Ctrl-C.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        services = loop.run_until_complete(start_services(host=host, telemetry_port=telemetry_port, start_ml_server=start_ml_server, ml_host=ml_host, ml_port=ml_port, persist_file=persist_file))
        _logger.info("All services started; press Ctrl-C to stop")
        loop.run_forever()
    except KeyboardInterrupt:
        _logger.info("Shutdown requested (KeyboardInterrupt)")
    finally:
        # Attempt to stop telemetry server gracefully
        try:
            server = services.get("telemetry_server")
            if server:
                loop.run_until_complete(server.stop())
        except Exception:
            _logger.exception("Error while stopping telemetry server")
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start Nave-Mãe services (TelemetryServer + optional ML server)")
    parser.add_argument("--host", default="127.0.0.1", help="Telemetry server host")
    parser.add_argument("--port", type=int, default=65080, help="Telemetry server port")
    parser.add_argument("--start-ml", action="store_true", help="Attempt to auto-start the ML UDP server if available")
    parser.add_argument("--ml-host", default="127.0.0.1", help="ML server host (if auto-started)")
    parser.add_argument("--ml-port", type=int, default=None, help="ML server port (if auto-started)")
    parser.add_argument("--persist-file", default=None, help="Optional persist file to pass to MissionStore (standalone)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    run_forever(host=args.host, telemetry_port=args.port, start_ml_server=args.start_ml, ml_host=args.ml_host, ml_port=args.ml_port, persist_file=args.persist_file)