import json
import os
from datetime import datetime
from threading import Lock

from common import utils, config

logger = utils.get_logger("nave_mae.telemetry_persister")

class TelemetryPersister:
    def __init__(self, data_dir: str = config.TELEMETRY_DATA_DIR):
        """
        Initializes the persister and creates the data directory if it doesn't exist.
        """
        self.data_dir = data_dir
        self._lock = Lock()
        os.makedirs(self.data_dir, exist_ok=True)
        logger.info(f"Telemetry Persister initialized. Saving data to: {self.data_dir}")

    def _get_filepath(self, date_str: str) -> str:
        return os.path.join(self.data_dir, f"telemetry-{date_str}.ndjson")

    def handle_update(self, event_type: str, payload: dict) -> None:
        if event_type != "telemetry_update":
            return

        rover_id = payload.get("rover_id")
        telemetry = payload.get("telemetry", {})
        
        # Garante que o timestamp está presente e é convertido para o formato correto
        if "timestamp_ms" not in telemetry:
            logger.error(f"Telemetry from {rover_id} is missing timestamp_ms.")
            return

        try:
            # 1. Preparar o Registo (Log Entry)
            # Adicionar metadata (Rover ID) ao payload
            log_entry = {
                "rover_id": rover_id,
                "timestamp_ms": telemetry["timestamp_ms"],
                "data": telemetry,
            }
            log_line = json.dumps(log_entry, ensure_ascii=False) + "\n"

            # 2. Determinar o Ficheiro
            timestamp_dt = datetime.fromtimestamp(telemetry["timestamp_ms"] / 1000.0)
            date_str = timestamp_dt.strftime("%Y%m%d")
            filepath = self._get_filepath(date_str)

            # 3. Escrever para o Ficheiro (Thread-safe)
            with self._lock:
                with open(filepath, "a", encoding="utf-8") as f:
                    f.write(log_line)
                
            logger.debug(f"Persisted telemetry for {rover_id} to {os.path.basename(filepath)}")
            
        except Exception:
            logger.exception(f"Error persisting telemetry for {rover_id}")

    def __call__(self, event_type: str, payload: dict) -> None:
        self.handle_update(event_type, payload)

# Exemplo de como esta classe é usada no Nave-Mãe/startup.py:
# persister = TelemetryPersister()
# telemetry_store.register_hook(persister.handle_update)