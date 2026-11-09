#worker.py
import logging
from mqtt.client import criar_client
from mqtt.handlers import iniciar_worker
from config import MQTT_CONFIG

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    mqttc = criar_client()
    iniciar_worker(mqttc)
    logging.info(f"Conectando ao broker {MQTT_CONFIG['host']}: {MQTT_CONFIG['port']}...")
    mqttc.connect_async(MQTT_CONFIG['host'], MQTT_CONFIG['port'])
    mqttc.loop_forever(retry_first_connection=True)

if __name__ == "__main__":
    main()