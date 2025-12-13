import os
import sys
import logging 
import pytest 

# Ensure project src is on sys.path so tests can import modules without
# requiring PYTHONPATH to be set externally. conftest.py lives in src/tests,
# so one parent up is the project src directory.
SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Configuração de logging 
logger = logging.getLogger("test.conftest")


# --- CORREÇÃO para o ValueError: I/O operation on closed file. ---
# Mantemos este hook para evitar o erro de I/O durante o encerramento do pytest.
@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config):
    if logger:
        try:
            # A linha que provavelmente falha quando o stream é fechado
            logger.info("Running cleanup hook...") 
        except ValueError:
            # Ignorar o erro de I/O que ocorre quando o pytest fecha os streams.
            pass
