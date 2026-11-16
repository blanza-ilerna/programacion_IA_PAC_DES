"""
sensor_simulator.py - Simulador de sensores IoT → MQTT (ILERNA Smart-Industry)

Emula N máquinas industriales enviando temperatura en C/F/K con:
  · Hash-Chaining SHA256: cada mensaje incluye su hash y el hash del anterior.
    Esto permite verificar la integridad de la cadena en Flink (no repudio).
  · Fallos simulados: JSON malformado, campos faltantes, unidades incorrectas,
    temperaturas imposibles y rotura deliberada de la cadena de hashes.

Schema publicado:
  {
    "device_id":   "machine-001",
    "temperature": 176.5,
    "unit":        "F",
    "ts":          "2026-03-10T12:00:00Z",
    "prev_hash":   "abc123...",   ← hash del mensaje anterior (GENESIS="0"*64 si es el primero)
    "hash":        "def456..."    ← SHA256(payload_sin_hash + prev_hash)
  }

Uso:
  python src/01_ingestion/sensor_simulator.py [--machines N] [--interval S] [--fault-rate F]
"""

import argparse
import hashlib
import json
import logging
import math
import os
import random
import signal
import time

import paho.mqtt.client as mqtt

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("sensor-simulator")

# ── Configuración ─────────────────────────────────────────────
MQTT_HOST  = os.getenv("MQTT_HOST",  "mosquitto")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/telemetry")

MACHINE_PROFILES = {
    "machine-001": {"base_c": 65.0, "amplitude": 8.0},
    "machine-002": {"base_c": 75.0, "amplitude": 10.0},
    "machine-003": {"base_c": 55.0, "amplitude": 5.0},
    "machine-004": {"base_c": 82.0, "amplitude": 12.0},  # suele superar 80°C → alerta
    "machine-005": {"base_c": 70.0, "amplitude": 6.0},
}

UNITS = ["C", "F", "K"]
GENESIS_HASH = "0" * 64   # hash inicial de la cadena (bloque génesis)

_running = True
# Estado de la cadena de hashes por dispositivo (en memoria del simulador)
_chain_state: dict[str, str] = {}   # device_id → último hash publicado


def signal_handler(sig, frame):
    global _running
    log.info("Señal recibida, deteniendo simulador...")
    _running = False


# ── Hash-Chaining ─────────────────────────────────────────────

def compute_hash(payload: dict, prev_hash: str) -> str:
    """
    Calcula SHA256 del contenido del mensaje + prev_hash.
    Los campos 'hash' y 'prev_hash' se excluyen del cálculo para evitar
    circularidad. El sort_keys garantiza determinismo independiente del orden.
    """
    content = {k: v for k, v in payload.items() if k not in ("hash", "prev_hash")}
    raw = json.dumps(content, sort_keys=True) + prev_hash
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def sign_message(payload: dict, device_id: str) -> dict:
    """Añade prev_hash y hash al payload y actualiza el estado de la cadena."""
    prev_hash = _chain_state.get(device_id, GENESIS_HASH)
    payload["prev_hash"] = prev_hash
    payload["hash"]      = compute_hash(payload, prev_hash)
    _chain_state[device_id] = payload["hash"]
    return payload


# ── Generación de lecturas ────────────────────────────────────

def celsius_to_unit(temp_c: float, unit: str) -> float:
    if unit == "F":
        return temp_c * 9 / 5 + 32
    if unit == "K":
        return temp_c + 273.15
    return temp_c


def generate_reading(machine_id: str, t: float) -> dict:
    profile = MACHINE_PROFILES[machine_id]
    temp_c = (
        profile["base_c"]
        + profile["amplitude"] * math.sin(2 * math.pi * t / 120)
        + random.gauss(0, 1.5)
    )
    unit = random.choice(UNITS)
    payload = {
        "device_id":   machine_id,
        "temperature": round(celsius_to_unit(temp_c, unit), 2),
        "unit":        unit,
        "ts":          time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    return sign_message(payload, machine_id)


def generate_fault(machine_id: str) -> str:
    """Genera un mensaje defectuoso. Puede incluir rotura de cadena."""
    fault_type = random.choice([
        "bad_json", "missing_field", "extreme_temp", "bad_unit", "broken_chain"
    ])
    if fault_type == "bad_json":
        return "{device_id: machine-bad, INVALID..."
    elif fault_type == "missing_field":
        payload = {"device_id": machine_id, "unit": "C",
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        return json.dumps(sign_message(payload, machine_id))
    elif fault_type == "extreme_temp":
        payload = {"device_id": machine_id, "temperature": 99999.0, "unit": "C",
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        return json.dumps(sign_message(payload, machine_id))
    elif fault_type == "bad_unit":
        payload = {"device_id": machine_id, "temperature": 75.0, "unit": "RANKINE",
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        return json.dumps(sign_message(payload, machine_id))
    else:  # broken_chain: mensaje válido pero con hash manipulado
        last_good = _chain_state.get(machine_id, GENESIS_HASH)
        payload = generate_reading(machine_id, time.time())   # avanza _chain_state...
        _chain_state[machine_id] = last_good                  # ...así que lo restauramos
        payload["prev_hash"] = "tampered_" + "0" * 56   # rompe la cadena
        payload["hash"]      = compute_hash(payload, payload["prev_hash"])
        # El mensaje manipulado va al DLQ y Flink no actualiza su estado; como el
        # simulador tampoco avanza, el siguiente mensaje vuelve a encadenar bien.
        log.warning("💀 CADENA ROTA simulada para %s", machine_id)
        return json.dumps(payload)


# ── MQTT callbacks ────────────────────────────────────────────

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info("Conectado a MQTT %s:%s | topic=%s", MQTT_HOST, MQTT_PORT, MQTT_TOPIC)
    else:
        log.error("Error MQTT reason_code=%s", reason_code)


def run(machines: list[str], interval: float, fault_rate: float, once: bool):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="ilerna-sensor-simulator",
    )
    client.on_connect = on_connect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    time.sleep(0.5)

    log.info("Simulando %d máquina(s) | intervalo=%.1fs | fault_rate=%.0f%% | hash-chaining=ON",
             len(machines), interval, fault_rate * 100)

    start = time.time()
    while _running:
        t = time.time() - start
        for machine_id in machines:
            if random.random() < fault_rate:
                payload = generate_fault(machine_id)
                log.warning("💥 FALLO simulado para %s", machine_id)
            else:
                data    = generate_reading(machine_id, t)
                payload = json.dumps(data)
                log.info("→ %s | %.2f%s | hash=%.8s...",
                         machine_id, data["temperature"], data["unit"], data["hash"])

            result = client.publish(MQTT_TOPIC, payload, qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                log.error("Error MQTT publish rc=%s", result.rc)

        if once:
            break
        time.sleep(interval)

    client.loop_stop()
    client.disconnect()
    log.info("Simulador detenido. Estado final de cadenas: %s",
             {k: v[:16] + "..." for k, v in _chain_state.items()})


if __name__ == "__main__":
    signal.signal(signal.SIGINT,  signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Simulador de sensores industriales IoT")
    parser.add_argument("--machines",   type=int,   default=5,    help="Número de máquinas (1-5)")
    parser.add_argument("--interval",   type=float, default=2.0,  help="Segundos entre rondas")
    parser.add_argument("--fault-rate", type=float, default=0.05, help="Tasa de fallos 0.0-1.0")
    parser.add_argument("--once",       action="store_true",      help="Una ronda y salir")
    args = parser.parse_args()

    n = max(1, min(args.machines, len(MACHINE_PROFILES)))
    run(list(MACHINE_PROFILES.keys())[:n], args.interval, args.fault_rate, args.once)
