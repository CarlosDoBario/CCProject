"""
config.py
Parâmetros configuráveis do projeto MissionLink (ML).
Valores por defeito que podem ser sobrepostos por variáveis de ambiente.
"""

import os


# ML / UDP
ML_PROTOCOL = os.getenv("ML_PROTOCOL", "ML/1.0")
ML_UDP_PORT = int(os.getenv("ML_UDP_PORT", "50000")) # definir a porta UDP como 50000
ML_MAX_DATAGRAM_SIZE = int(os.getenv("ML_MAX_DATAGRAM_SIZE", "1200"))

# Retransmissão / fiabilidade
TIMEOUT_TX_INITIAL = float(os.getenv("TIMEOUT_TX_INITIAL", "4.0"))  # segundos
N_RETX = int(os.getenv("N_RETX", "4"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "2.0"))
ACK_JITTER = float(os.getenv("ACK_JITTER", "0.5"))  # jitter para evitar sincronização (s)

# Deduplicação / retenção de mensagens vistas
DEDUPE_RETENTION_S = int(os.getenv("DEDUPE_RETENTION_S", "3600"))  # 1 hora

# Heartbeat / RTT estimation (opcional)
HEARTBEAT_INTERVAL_S = int(os.getenv("HEARTBEAT_INTERVAL_S", "30"))
RTT_EWMA_ALPHA = float(os.getenv("RTT_EWMA_ALPHA", "0.125"))

# Logging / metrics
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
METRICS_ENABLE = os.getenv("METRICS_ENABLE", "true").lower() in ("1", "true", "yes")

# Timeouts for server tasks
PENDING_CLEANUP_INTERVAL_S = int(os.getenv("PENDING_CLEANUP_INTERVAL_S", "60"))
DEDUPE_CLEANUP_INTERVAL_S = int(os.getenv("DEDUPE_CLEANUP_INTERVAL_S", "300"))

# Misc
DEFAULT_UPDATE_INTERVAL_S = int(os.getenv("DEFAULT_UPDATE_INTERVAL_S", "30"))  # fallback telemetry/progress
ENV = os.getenv("ENV", "development")

def as_dict():
    """
    Retorna um dicionário com a configuração atual (útil para logging/startup).
    """
    return {
        "ML_PROTOCOL": ML_PROTOCOL,
        "ML_UDP_PORT": ML_UDP_PORT,
        "ML_MAX_DATAGRAM_SIZE": ML_MAX_DATAGRAM_SIZE,
        "TIMEOUT_TX_INITIAL": TIMEOUT_TX_INITIAL,
        "N_RETX": N_RETX,
        "BACKOFF_FACTOR": BACKOFF_FACTOR,
        "ACK_JITTER": ACK_JITTER,
        "DEDUPE_RETENTION_S": DEDUPE_RETENTION_S,
        "HEARTBEAT_INTERVAL_S": HEARTBEAT_INTERVAL_S,
        "RTT_EWMA_ALPHA": RTT_EWMA_ALPHA,
        "LOG_LEVEL": LOG_LEVEL,
        "METRICS_ENABLE": METRICS_ENABLE,
        "PENDING_CLEANUP_INTERVAL_S": PENDING_CLEANUP_INTERVAL_S,
        "DEDUPE_CLEANUP_INTERVAL_S": DEDUPE_CLEANUP_INTERVAL_S,
        "DEFAULT_UPDATE_INTERVAL_S": DEFAULT_UPDATE_INTERVAL_S,
        "ENV": ENV,
    }