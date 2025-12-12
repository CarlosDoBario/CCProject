import asyncio
import logging
import multiprocessing
import os
import sys
import time

# Importação dos módulos do projeto
from common import config, utils 

# Importar as funções main/run dos servidores
try:
    from nave_mae.ml_server import main as ml_server_main
    from nave_mae.telemetry_server import main as ts_server_main
    # Importar a função run_api_server_uvicorn modificada
    from nave_mae.api_server import run_api_server_uvicorn as api_server_run
except ImportError as e:
    print(f"Erro ao importar módulos do servidor. Certifique-se de que as dependências estão instaladas: {e}")
    sys.exit(1)


# Configurar o logging antes de tudo 
config.configure_logging()
logger = utils.get_logger("nave_mae.launcher")


def run_server_process(target_func, name):
    """Função auxiliar para iniciar um servidor (bloqueante) em um processo separado."""
    logger.info(f"Starting {name} server process...")
    try:
        target_func()
    except KeyboardInterrupt:
        logger.info(f"{name} server process terminated via KeyboardInterrupt.")
    except Exception as e:
        logger.error(f"Error in {name} server process: {e}", exc_info=True)

def main():
    """Entry point para iniciar todos os servidores (ML, TS, API) em processos separados."""
    
    processes = []
    
    # Adicionar um pequeno atraso para permitir que o logger e outros recursos sejam inicializados
    time.sleep(1.0) 

    # 1. Limpar Mission Store para garantir uma inicialização limpa da demo
    try:
        if os.path.exists(config.MISSION_STORE_FILE):
             os.remove(config.MISSION_STORE_FILE)
             logger.info(f"Removed old mission store file: {config.MISSION_STORE_FILE}")
    except Exception:
        logger.exception("Failed to remove mission store file. Continuing...")

    # 2. ML Server (UDP/Asyncio)
    p_ml = multiprocessing.Process(target=run_server_process, args=(ml_server_main, "MissionLink (ML)"), daemon=True)
    processes.append(p_ml)

    # 3. TS Server (TCP/Asyncio)
    p_ts = multiprocessing.Process(target=run_server_process, args=(ts_server_main, "TelemetryStream (TS)"), daemon=True)
    processes.append(p_ts)
    
    # 4. API Server (HTTP/FastAPI/Uvicorn) - Usa a função modificada
    p_api = multiprocessing.Process(target=run_server_process, args=(api_server_run, "API Observation"), daemon=True)
    processes.append(p_api)

    # Iniciar todos os processos
    for p in processes:
        p.start()
    
    # Pausa para dar tempo ao Uvicorn inicializar completamente
    time.sleep(3.0) 
    
    logger.info("=======================================================================")
    logger.info(" All servers (ML, TS, API) started in separate processes on Nave-Mãe. ")
    logger.info("=======================================================================")
    logger.info("Pressione Ctrl+C para parar todos os processos do servidor.")

    try:
        # Loop principal para monitorizar os filhos
        while any(p.is_alive() for p in processes):
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Main launcher received KeyboardInterrupt. Shutting down servers gracefully...")
        
    except Exception:
        logger.exception("Unexpected error in main launcher loop.")
        
    finally:
        # Terminar todos os processos filhos
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=3)
                if p.is_alive():
                    logger.warning(f"Process {p.name} did not terminate gracefully. Forcing kill.")

    logger.info("All server processes have been stopped. Exiting.")

if __name__ == "__main__":
    main()