import json, os, sys, time
import paho.mqtt.client as mqtt
nos, intervalo, duracao = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="gerador")
c.connect("localhost", 1883); c.loop_start()
macs = [f"{i:012X}" for i in range(1, nos + 1)]
lote = {m: 0 for m in macs}
fim = time.time() + duracao
while time.time() < fim:
    for m in macs:
        lote[m] = lote[m] % 65535 + 1
        payload = {"senderAddress": int(os.environ["LAB_SENSOR_ID"]),
                   "gateway_id": "gateway_lab_01", "rssi": -80,
                   "medicoes": [{"idade_min": (15 - j) * 30, **{f"temp_ds{k}": 25.0 for k in range(1, 7)}} for j in range(16)]}
        c.publish(f"rssf/{m}/telemetria/{lote[m]}", json.dumps(payload), qos=1)
    time.sleep(intervalo)
c.loop_stop()