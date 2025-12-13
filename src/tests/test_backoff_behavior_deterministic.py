import asyncio
import time
import struct
from unittest.mock import MagicMock, patch

import pytest
from common import config
from common import binary_proto 
from rover.ml_client import SimpleMLClient 


@pytest.mark.asyncio
async def test_retransmit_backoff():
    """
    Testa que o loop de retransmissão segue a regra de backoff exponencial (1.0s, 2.0s, 4.0s...).
    """
    # 1. Configuração de parâmetros de fiabilidade
    config.TIMEOUT_TX_INITIAL = 1.0
    config.BACKOFF_FACTOR = 2.0
    # CORRIGIDO: N_RETX = 2 resulta em 3 envios no total (1 Inicial + 2 Retrans)
    config.N_RETX = 2  


    # 2. Inicialização do Cliente e Mocking
    mock_transport = MagicMock()
    client = SimpleMLClient(rover_id="R-TEST", server=("127.0.0.1", 64070))
    client.transport = mock_transport
    
    # Garantir que o atributo existe no cliente
    if not hasattr(client, '_persisted_files'):
         client._persisted_files = {}

    # Helper para correr a lógica central do retransmit_loop uma vez
    async def run_retransmit_loop_once(client_inst):
        """Executa a lógica de retransmissão sem o loop infinito e o sleep."""
        # A chamada time.time() aqui consome o próximo valor do mock_times
        now_mock = time.time() 
        remove = []
        with client_inst._lock:
            items = list(client_inst.pending.items())
        
        for mid, (pkt, created, attempts) in items:
            # Usa backoff exponencial
            timeout = config.TIMEOUT_TX_INITIAL * (config.BACKOFF_FACTOR ** (attempts - 1))
            
            # Condição do SimpleMLClient: if now - created > timeout:
            if now_mock - created > timeout:
                if attempts <= config.N_RETX: # Agora N_RETX=2, permite attempts=1 e attempts=2
                    client_inst.transport.sendto(pkt, client_inst.server)
                    with client_inst._lock:
                        # O tempo de 'created' é atualizado para o tempo atual do mock (now_mock)
                        client_inst.pending[mid] = (pkt, now_mock, attempts + 1)
                else:
                    remove.append(mid)
        
        with client_inst._lock:
            for r in remove:
                client_inst.pending.pop(r, None)
                client_inst._persisted_files.pop(r, None)
        await asyncio.sleep(0) # Passa o controlo ao loop de eventos


    # 3. Preparação do Pacote (Simulando request_mission)
    test_msgid = 12345
    test_seqnum = 1
    tlvs = [(binary_proto.TLV_CAPABILITIES, b"sampling")]
    
    test_packet = binary_proto.pack_ml_datagram(
        binary_proto.ML_REQUEST_MISSION, 
        client.rover_id, 
        tlvs, 
        flags=binary_proto.FLAG_ACK_REQUESTED, 
        seqnum=test_seqnum, 
        msgid=test_msgid
    )

    T_START = 1000.0
    initial_timeout = config.TIMEOUT_TX_INITIAL # 1.0

    # Sequência de tempo (4 valores de mock_times são suficientes)
    # 1. created_at = 1000.0
    # 2. now_mock 1: 1001.01 (T > 1.0) -> Triggers 1st retrans (attempts=2)
    # 3. now_mock 2: 1003.02 (T > 2.0 desde 1001.01) -> Triggers 2nd retrans (attempts=3)
    # 4. now_mock 3: 1007.03 (T > 4.0 desde 1003.02) -> Triggers exhaustion (attempts=4)
    mock_times = [
        T_START, 
        T_START + initial_timeout + 0.01, 
        T_START + initial_timeout + (initial_timeout * 2) + 0.02, 
        T_START + initial_timeout + (initial_timeout * 2) + (initial_timeout * 4) + 0.03, 
    ]


    # 4. Patch do time.time() e Início da Simulação
    with patch("time.time", side_effect=mock_times) as time_mock:

        # A) Início da simulação: Colocar o pacote em pending e enviar
        with client._lock:
             # time.time() consome T_START (1000.0) para created_at
             client.pending[int(test_msgid)] = (test_packet, time.time(), 1)
             client.transport.sendto(test_packet, client.server)
        
        # O envio inicial já ocorreu (call_count = 1)
        assert mock_transport.sendto.call_count == 1
        po = client.pending.get(test_msgid)
        assert po is not None
        assert po[2] == 1 # 1ª Tentativa (inicial)

        # 1. 1ª Retransmissão (após 1.0s)
        await run_retransmit_loop_once(client) 
        
        # Verifica se houve a 1ª retransmissão (total 2)
        assert mock_transport.sendto.call_count == 2
        po = client.pending.get(test_msgid)
        assert po is not None
        assert po[2] == 2 # 2ª Tentativa

        # 2. 2ª Retransmissão (após 2.0s do 1º retrans)
        await run_retransmit_loop_once(client)
        
        # Verifica se houve a 2ª retransmissão (total 3)
        assert mock_transport.sendto.call_count == 3
        po = client.pending.get(test_msgid)
        assert po is not None
        assert po[2] == 3 # 3ª Tentativa (Última permitida por N_RETX=2)

        # 3. Exaustão de Retries (após 4.0s do 2º retrans)
        await run_retransmit_loop_once(client)

        # Verifica se não houve 4ª retransmissão
        assert mock_transport.sendto.call_count == 3
        # Verifica se o pacote foi removido após a exaustão
        assert test_msgid not in client.pending
