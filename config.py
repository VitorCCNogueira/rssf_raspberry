import os
import re
from dotenv import load_dotenv

load_dotenv()

DEFAULT_GATEWAY_ID = os.getenv('DEFAULT_GATEWAY_ID', 'gateway_unknown')

DB_CONFIG = {
    'path':     os.getenv('SQLITE_PATH','pi.db'),
    'dedup_min': 17*60,
    'timeout': 5,
}


MQTT_CONFIG = {
    'host':     os.getenv('MQTTHOST'),
    'username':     os.getenv('MQTTUSERNAME'),
    'password': os.getenv('MQTTPASSWORD'),
    'port':     int(os.getenv('MQTTPORT') or 1883),
}

# Preserve the deployed nodes' cadence. Lab thresholds are point-specific.
STATUS_ONLINE_MINUTES = int(os.getenv('STATUS_ONLINE_MINUTES', 60))
STATUS_OFFLINE_MINUTES = int(os.getenv('STATUS_OFFLINE_MINUTES', 180))
if not 0 < STATUS_ONLINE_MINUTES < STATUS_OFFLINE_MINUTES:
    raise ValueError('Status exige 0 < STATUS_ONLINE_MINUTES < STATUS_OFFLINE_MINUTES')

LAB_SENSOR_ID = os.getenv('LAB_SENSOR_ID', '').strip()
LAB_GATEWAY_ID = os.getenv('LAB_GATEWAY_ID', 'gateway_lab_01').strip()
LAB_ONLINE_MINUTES = int(os.getenv('LAB_ONLINE_MINUTES', 540))
LAB_OFFLINE_MINUTES = int(os.getenv('LAB_OFFLINE_MINUTES', 1440))
if LAB_SENSOR_ID and (not LAB_SENSOR_ID.isdecimal() or not 1 <= int(LAB_SENSOR_ID) <= 65534):
    raise ValueError('LAB_SENSOR_ID deve estar entre 1 e 65534')
if not 0 < LAB_ONLINE_MINUTES < LAB_OFFLINE_MINUTES:
    raise ValueError('Limites de status do laboratorio invalidos')
if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', LAB_GATEWAY_ID):
    raise ValueError('LAB_GATEWAY_ID invalido')
