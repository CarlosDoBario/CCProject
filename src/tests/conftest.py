import os
import sys
import logging
import asyncio
import pytest
import time
import socket
import subprocess

# Ensure project src is on sys.path so tests can import modules without
# requiring PYTHONPATH to be set externally. conftest.py lives in src/tests,
# so one parent up is the project src directory.
SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Importações internas necessárias
from common import config, utils
from nave_mae.mission_store import MissionStore

# --- CONSTANTES GLOBAIS NECESSÁRIAS PARA OS TESTES ---
TEST_ML_HOST = "127.0.0.1"
TEST_ML_UDP_PORT = 50000 
TEST_TS_TCP_PORT = 65080

# Configurar o logging para o conftest.py para ser visível.
config.configure_logging(level="INFO") 
logger = utils.get_logger("test.conftest")


# --- FIXTURES CRÍTICAS PARA ASYNC (RESOLUÇÃO DO ERRO) ---

@pytest.fixture(scope="session")
def event_loop(request):
    """
    FIX: Define explicitamente o event loop com escopo de sessão para ser 
    descoberto corretamente pelas fixtures assíncronas.
    """
    try:
        policy = asyncio.get_event_loop_policy()
        loop = policy.new_event_loop()
    except Exception:
        loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)
    
    yield loop
    
    # Encerra o loop de forma limpa
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


# --- FIXTURES GERAIS ---

@pytest.fixture(scope="session", autouse=True)
def cleanup_mission_store(tmp_path_factory):
    """Garante que a Mission Store é criada numa pasta temporária para cada sessão de teste."""
    temp_dir = tmp_path_factory.mktemp("test_data")
    
    # Sobrescreve o ficheiro de configuração para apontar para o ficheiro temporário
    original_mission_store_file = config.MISSION_STORE_FILE
    config.MISSION_STORE_FILE = str(temp_dir / "test_mission_store.json")

    logger.info(f"Using temp mission store file: {config.MISSION_STORE_FILE}")
    
    yield
    
    # Restaura o valor original
    config.MISSION_STORE_FILE = original_mission_store_file
    
# Fixture para garantir que o MissionStore é inicializado e preenchido para testes e2e
@pytest.fixture
def mission_store_instance(cleanup_mission_store):
    """Fornece uma MissionStore nova, limpa e preenchida com missões demo."""
    if os.path.exists(config.MISSION_STORE_FILE):
        os.remove(config.MISSION_STORE_FILE)
    
    ms = MissionStore()
    
    if hasattr(ms, "create_demo_missions") and callable(ms.create_demo_missions):
        ms.create_demo_missions()
    else:
        ms.create_mission({"task": "default", "priority": 1})

    return ms

@pytest.fixture(scope="session")
def ml_server_process(cleanup_mission_store):
    """
    Inicia o ML Server num processo separado e aguarda a sua inicialização, 
    usando a porta estática TEST_ML_UDP_PORT.
    """
    original_ml_port = config.ML_UDP_PORT
    config.ML_UDP_PORT = TEST_ML_UDP_PORT
    
    cmd = [sys.executable, "-m", "nave_mae.ml_server"]

    logger.info("Starting ML Server server process...")
    proc = subprocess.Popen(cmd, start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logger.info(f"ML Server process started (PID: {proc.pid}).")

    start_time = time.time()
    server_ready = False
    while time.time() - start_time < 5.0:
        line = proc.stdout.readline().decode().strip()
        if "ML server listening" in line:
            server_ready = True
            logger.info(f"ML Server is ready.")
            break
        elif not line and proc.poll() is not None:
             raise RuntimeError(f"ML Server process exited early with code {proc.returncode}")
        elif line:
            pass

    if not server_ready:
         raise RuntimeError("ML Server did not start listening in time.")

    yield proc 

    # --- TEARDOWN ---
    logger.info("Stopping ML Server process...")
    try:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=1)
    except Exception as e:
        logger.warning(f"Error during ML Server process teardown: {e}")
        
    config.ML_UDP_PORT = original_ml_port

    
# --- FIX para o `ValueError: I/O operation on closed file.` ---
# Isto impede o erro de logging que ocorre no encerramento do pytest (linha 53).
@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config):
    if 'test.conftest' in logging.root.manager.loggerDict:
        cleanup_logger = logging.getLogger('test.conftest')
        try:
            cleanup_logger.info("Running general cleanup hook...")
        except ValueError:
            pass
