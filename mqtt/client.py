#mqtt/client.py

import paho.mqtt.client as mqtt
import os
from config import MQTT_CONFIG
from mqtt.handlers import on_connect, on_subscribe, on_message

def criar_client():
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=os.getenv("MQTT_CLIENT_ID","backend_rssf"))
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqttc.on_message = on_message

    if MQTT_CONFIG.get('username') and MQTT_CONFIG.get('password'):
        mqttc.username_pw_set(MQTT_CONFIG['username'], MQTT_CONFIG['password'])
    return mqttc
