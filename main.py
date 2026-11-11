#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import random
import json
import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, db

# 1. CONFIGURACIÓN DE SEGURIDAD (ISO 27000) Y FIREBASE
# Rutas corregidas para Ubuntu WSL
PATH_CRED = 'cred.json' 
URL_DB = 'https://logistica-iot-u3-default-rtdb.firebaseio.com/' 

cred = credentials.Certificate(PATH_CRED)
firebase_admin.initialize_app(cred, {
    'databaseURL': URL_DB
})

# Referencias en la Base de Datos No Estructurada (Árbol JSON)
ref_sensores = db.reference('logistica/camion_01/sensores')
ref_actuador = db.reference('logistica/camion_01/actuadores/enfriador_emergencia')

# 2. CONFIGURACIÓN DEL BROKER PÚBLICO MQTT (Paso 5 de la Guía)
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "inacap/logistica/camion_01/telemetria"

# Inicializar estado del actuador si no existe
try:
    if ref_actuador.get() is None:
        ref_actuador.set(False)
except Exception as e:
    print(f"[AVISO FIREBASE] No se pudo inicializar el actuador: {e}")

def on_message(client, userdata, msg):
    try:
        datos = json.loads(msg.payload.decode('utf-8'))
        print(f"\n[MQTT RECIBIDO DEL BROKER] Tópico: {msg.topic} | Datos: {datos}")
        ref_sensores.set(datos)
        print("[FIREBASE OK] Guardado en DB No Estructurada en tiempo real.")
    except Exception as e:
        print(f"[ERROR FIREBASE] Guardando en almacenamiento temporal: {e}")

try:
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
    mqtt_client = mqtt.Client()

mqtt_client.on_message = on_message

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.subscribe(MQTT_TOPIC)
    mqtt_client.loop_start()
    print(f"Conectado al Broker MQTT Público: {MQTT_BROKER}")
except Exception as e:
    print(f"Modo Offline inicial activado: {e}")

print("Iniciando monitoreo de logística (Presiona Ctrl+C para detener)...")

# 3. BUCLE PRINCIPAL
while True:
    try:
        estado_enfriador = ref_actuador.get()
        if estado_enfriador:
            print("[ACTUADOR ACTIVO] Enfriador de emergencia: ENCENDIDO desde App Android")
        else:
            print("[ACTUADOR INACTIVO] Enfriador de emergencia: APAGADO")
    except Exception:
        pass

    lectura = {
        "temperatura": round(random.uniform(-19.5, -16.0), 1),
        "humedad": round(random.uniform(55.0, 68.0), 1),
        "estado_puerta": "CERRADA",
        "timestamp": int(time.time())
    }
    payload_json = json.dumps(lectura)

    try:
        info = mqtt_client.publish(MQTT_TOPIC, payload_json, qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise Exception("Desconectado del Broker MQTT")
        print(f"[MQTT ENVIADO] {payload_json}")
    except Exception as e:
        print("[CONTINGENCIA OFFLINE] Sin red. Guardando lectura en buffer_local.json...")
        # Ruta corregida para Ubuntu WSL
        with open('buffer_local.json', 'a') as archivo_local:
            archivo_local.write(payload_json + "\n")
        try:
            mqtt_client.reconnect()
        except Exception:
            pass

    time.sleep(4)