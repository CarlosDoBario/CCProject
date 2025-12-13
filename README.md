**Nós da Topologia:**
* **Ground Control (192.168.0.2/24):** Consome a API da Nave-Mãe.
* **Nave-Mãe (10.0.10.1/24 (ligação com os Rover) e 192.168.0.1/24 (ligação com o GC)):** Servidor ML (UDP) e TS (TCP) e API de Observação (HTTP).
* [cite_start]**Satélites:** Rovers (R-001, R-002, R-003):** Clientes ML e TS.

---

##  Instruções de Execução da Simulação

Para iniciar uma simulação completa, siga os passos na ordem indicada:

### 1. Preparação Inicial

> **ATENÇÃO:** É necessário **eliminar o ficheiro JSON**  que contém o estado da simulação persistido, geralmente localizado na subdiretoria `data/` da Nave-Mãe, sempre que se inicia uma nova simulação.

### 2. Navegação para a Diretoria Base

Em **todos os terminais CORE** (Nave-Mãe, Ground Control e Rovers), navegue para a diretoria `src/`:

```bash
cd /home/core/CCProjectFinal/src

No terminal da Nave Mãe: python3 -u -m nave_mae.start_all_servers

No terminal Ground Control : python3 -u -m groundcontrol.gc_cliente

No terminal dos Rovers: python3 -u -m rover.rover_agent --rover-id R-00X (numero do rover)

# Execute a partir da diretoria raiz do projeto
$env:PYTHONPATH = "src"; python -m pytest -q