# sync.py
import itertools
import logging
import os
import time
from datetime import datetime, timezone

import requests
from models.medicao import MedicaoModel, _iso  # models já importa o config (load_dotenv)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
CLOUD_URL = os.getenv('CLOUD_URL')
INTERVALO = int(os.getenv('SYNC_INTERVALO', 60))


def montar(linhas, agora):
    p = linhas[0]
    medicoes = []
    for l in linhas:
        t = datetime.strptime(l['data_hora'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        idade = max(0, round((agora - t).total_seconds() / 60))
        medicoes.append({'idade_min': idade,
                         **{f'temp_ds{i}': l[f'temp_ds{i}'] for i in range(1, 7)}})
    # mac e lote_id só servem ao Pi (agrupar e marcar enviado); a nuvem não os guarda
    return {'senderAddress': p['sensor_id'], 'gateway_id': p['gateway_id'],
            'rssi': p['rssi'], 'medicoes': medicoes}


def sincronizar():
    linhas = MedicaoModel.pendentes()
    for (mac, lote_id), grupo in itertools.groupby(linhas, key=lambda l: (l['mac'], l['lote_id'])):
        payload = montar(list(grupo), datetime.now(timezone.utc))
        try:
            resp = requests.post(CLOUD_URL, json=payload, timeout=10)
        except requests.RequestException as e:
            logging.warning(f"Nuvem inacessível, tenta no próximo ciclo: {e}")
            return
        if resp.ok:
            MedicaoModel.marcar_enviado(mac, lote_id, _iso(datetime.now(timezone.utc)))
            logging.info(f"Lote {lote_id} de {mac} enviado")
        elif resp.status_code == 400:
            logging.warning(f"Lote {lote_id} de {mac} descartado pela nuvem: {resp.text}")
            MedicaoModel.marcar_enviado(mac, lote_id, 'descartado')
        else:
            logging.error(f"Nuvem respondeu {resp.status_code}; tenta no próximo ciclo")
            return


if __name__ == '__main__':
    if not CLOUD_URL:
        raise SystemExit("CLOUD_URL não definido no .env")
    while True:
        try:
            sincronizar()
        except Exception:
            logging.exception("Falha inesperada no sync")
        time.sleep(INTERVALO)