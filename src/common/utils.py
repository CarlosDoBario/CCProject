"""
utils.py
Utilitários comuns para o projeto MissionLink (ML).

Contém:
- configuração de logging consistente (texto legível; fácil trocar para JSON).
- helpers para backoff exponencial.
- helpers para timestamps ISO8601 UTC.
- small in-memory metrics counters (opcional, simples).
- função safe_sleep com jitter para evitar sincronização.
"""

import time
import logging
import json
import random
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Iterator

from common import config

# --- Logging setup ----------------------------------------------------------
def get_logger(name: str = "ml") -> logging.Logger:
    """
    Retorna um logger configurado com nível a partir de config.LOG_LEVEL.
    Garante uma única StreamHandler bem formatada (remove handlers pré-existentes),
    e envia a saída para stdout (reduz a probabilidade de mistura com stderr/prints).
    """
    logger = logging.getLogger(name)

    # Normalize name: allow callers to call get_logger many vezes without duplicating handlers
    # Remove existing handlers to avoid duplicate output if modules are reloaded.
    if logger.handlers:
        for h in list(logger.handlers):
            try:
                logger.removeHandler(h)
            except Exception:
                pass

    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    # Create handler explicitly bound to stdout to keep all logs on same stream
    handler = logging.StreamHandler(stream=sys.stdout)
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.propagate = False

    # Route warnings (from the warnings module) to logging
    logging.captureWarnings(True)

    return logger


# --- Time helpers ----------------------------------------------------------
def now_iso() -> str:
    """Timestamp atual em ISO8601 UTC (segundos de precisão)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def now_epoch_ms() -> int:
    """Timestamp atual em epoch milliseconds."""
    return int(time.time() * 1000)

def iso_to_epoch_ms(iso_ts: str) -> int:
    """Converte ISO8601 UTC para epoch milliseconds. Levanta ValueError se inválido."""
    dt = datetime.fromisoformat(iso_ts)
    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

# --- Backoff / sleep helpers ----------------------------------------------
def exponential_backoff(base: float = 1.0, factor: float = 2.0, max_delay: float = 60.0) -> Iterator[float]:
    """
    Gerador infinito de delays: base, base*factor, base*factor^2, ... até max_delay.
    Uso: for delay in exponential_backoff(...): sleep(delay); tentar novamente
    """
    attempt = 0
    while True:
        delay = min(base * (factor ** attempt), max_delay)
        yield delay
        attempt += 1

def safe_sleep(seconds: float, jitter: float = 0.0) -> None:
    """
    Dorme pela duração especificada adicionando jitter aleatório +/- jitter.
    Útil para evitar sincronização de retransmissões entre vários nós.
    """
    if jitter:
        jitter_amount = random.uniform(-jitter, jitter)
    else:
        jitter_amount = 0.0
    to_sleep = max(0.0, seconds + jitter_amount)
    time.sleep(to_sleep)


# --- Simple metrics --------------------------------------------------------
class SimpleMetrics:
    """
    Contador muito simples para métricas em memória.
    Não é persistente nem thread-safe por design simples; se precisares de concorrência,
    proteger com locks ou usar uma biblioteca de metrics.
    """
    def __init__(self) -> None:
        self.counters: Dict[str, int] = {}
        self.gauges: Dict[str, float] = {}

    def incr(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def set_gauge(self, name: str, value: float) -> None:
        self.gauges[name] = value

    def get_counter(self, name: str) -> int:
        return self.counters.get(name, 0)

    def snapshot(self) -> Dict[str, Any]:
        return {"counters": dict(self.counters), "gauges": dict(self.gauges)}

# --- Utility helpers ------------------------------------------------------
def pretty_json(obj: Any) -> str:
    """Retorna JSON prettified (útil para logs de debug)."""
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)

def gen_msg_id() -> str:
    """Gera um identificador simples para mensagens (UUID4 string)."""
    import uuid
    return str(uuid.uuid4())

# --- Module-level globals -------------------------------------------------
logger = get_logger("ml.utils")
metrics = SimpleMetrics()