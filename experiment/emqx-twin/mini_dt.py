import os
import time
import paho.mqtt.client as mqtt

# Legge l'IP del Broker dalle variabili d'ambiente di Kubernetes
BROKER_IP = os.getenv("MQTT_BROKER_IP", "127.0.0.1")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TOPIC = "factory/sensors/#"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[SUCCESS] Cyber-Physical Entanglement ESTABLISHED with {BROKER_IP}:{BROKER_PORT}")
        client.subscribe(TOPIC)
    else:
        print(f"[ERROR] Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    print(f"[DATA] Received physical state update on {msg.topic}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

print(f"[*] Starting Mini DT Lifecycle...")
print(f"[*] Attempting network binding to Physical Broker at {BROKER_IP}...")

try:
    # Tenta la connessione. Se la rete è segregata (Nodo C), andrà in eccezione.
    client.connect(BROKER_IP, BROKER_PORT, keepalive=10)
    client.loop_forever()
except Exception as e:
    print(f"[CRITICAL] Network Timeout or Unreachable Route to {BROKER_IP}!")
    print(f"[CRITICAL] Cyber-Physical Entanglement FAILED: {e}")
    # Ciclo infinito per impedire al pod di spegnersi (facilita la lettura dei log)
    while True:
        time.sleep(30)
