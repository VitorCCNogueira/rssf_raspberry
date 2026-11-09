#mqtt/handlers.py
import logging 
import json
import queue
import threading
from models.medicao import MedicaoModel
from core.lotes import preparar_lote, LoteInvalido

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
ultimo_ack = {}
em_andamento = set()

def on_connect(client, userdata, flags, reason_code, properties): #conexao com o mqtt
    if reason_code == 0:
        logging.info(f"Conexão bem-estabelecida: {reason_code}")
        client.subscribe("rssf/#") # entra em todos os tópicos
    else:
        logging.error(f"Erro ao conectar. Codigo: {reason_code}")

def on_subscribe(client, userdata, mid, reason_code_list, properties): #inscricao no topico
    if reason_code_list[0].is_failure:
        logging.error(f"Broker não aceitou a inscrição: {reason_code_list[0]}")
    else:
        logging.info(f"Broker informou o seguinte QoS: {reason_code_list[0].value}")

            
def on_ack(client,mac,lote_id):
    ultimo_ack[mac] = lote_id
    client.publish(f"rssf/{mac}/ack",lote_id,qos=1,retain=True)  


fila = queue.Queue(maxsize=2000) 

def on_worker(client):
    while True:
        gateway_id, dados, mac, lote_id = fila.get()
        try:
            lote_num = int(lote_id) if lote_id else None

            duplicado = lote_num is not None and MedicaoModel.received(mac,lote_num)
            if duplicado:
                logging.info(f"Lote {lote_id} de {mac} já gravado")
            else:
                if 'medicoes' in dados:
                    MedicaoModel.salvar_lote(dados,gateway_id, mac, lote_num)
                else:
                    MedicaoModel.salvar(dados,gateway_id, mac, lote_num)
                qtd = len(dados['medicoes']) if 'medicoes' in dados else 1
                if qtd != 1:
                    logging.info(f"{qtd} medições de {gateway_id} gravadas")
                else:
                    logging.info(f"{qtd} medição de {gateway_id} gravada")
                    
            if lote_id:
                on_ack(client,mac,lote_id)
        except Exception:
            logging.exception(f"Falha ao gravar dados de {gateway_id}")
        finally:
            if lote_id:
                em_andamento.discard((mac,lote_id))
            fila.task_done()

def descartar_lote(client, mac, lote_id, motivo):
    logging.warning(f"Lote {lote_id} de {mac} descartado: {motivo}")
    if lote_id:
        on_ack(client, mac, lote_id)

def on_message(client, userdata, msg): #mensagens recebidas pelo mqtt
    try:
        payload = msg.payload.decode('utf-8')

        partes = msg.topic.split('/')
        mac,tipo = partes[1], partes[2]

        if tipo == "ack":
            ultimo_ack[mac] = payload
            return

        if tipo == "perdidos":
            logging.warning(f"Lotes descartados em {mac}: {payload}")
            return

        if tipo == "status":
            logging.info(f"Status de {mac}: {payload}")
            return

        if tipo != "telemetria":
            logging.info(f"Tópico ignorado: {msg.topic}")
            return

        lote_id = partes[3] if len(partes) > 3 else None

        if lote_id:
            if ultimo_ack.get(mac) == lote_id:
                on_ack(client, mac, lote_id)
                return
            
            if (mac,lote_id) in em_andamento:
                return
            
        
        try:
            dados = json.loads(payload)
            gateway_id = preparar_lote(dados)
        except (json.JSONDecodeError,LoteInvalido) as e:
            descartar_lote(client, mac, lote_id, e)
            return

        if lote_id:
            em_andamento.add((mac,lote_id))
        fila.put_nowait((gateway_id,dados,mac,lote_id))

    except UnicodeDecodeError:
        logging.error("Falha ao decodificar: Payload não é UTF-8.")
    except queue.Full:
        em_andamento.discard((mac,lote_id))
        logging.error(f"Fila cheia: pacote de {gateway_id} descartado")
    except Exception:
        logging.exception(f"Falha inesperada ao processar o tópico {msg.topic}")


def iniciar_worker(client):
    threading.Thread(target=on_worker, args=(client,), daemon=True).start()