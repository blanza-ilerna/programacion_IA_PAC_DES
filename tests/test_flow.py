"""
test_flow.py - Prueba el pipeline de datos de extremo a extremo.

Servicios probados:
  MQTT → Redpanda (Kafka) → InfluxDB → RustFS → Grafana

Uso:
  python tests/test_flow.py

Dependencias (todas ya en config/requirements.txt):
  paho-mqtt, confluent-kafka, influxdb-client, minio
"""

import os
import sys
import time
import json
import uuid

# ── Configuración ────────────────────────────────────────────
MQTT_HOST       = os.getenv("MQTT_HOST",      "mosquitto")
MQTT_PORT       = int(os.getenv("MQTT_PORT",  "1883"))
MQTT_TOPIC      = "test/sensor"

KAFKA_BROKER    = os.getenv("KAFKA_BROKER",   "redpanda:29092")
KAFKA_TOPIC     = "test-flow"

INFLUX_URL      = os.getenv("INFLUX_URL",     "http://influxdb:8086")
INFLUX_TOKEN    = os.getenv("INFLUX_TOKEN",   "supersecrettoken")
INFLUX_ORG      = os.getenv("INFLUX_ORG",     "ilerna")
INFLUX_BUCKET   = os.getenv("INFLUX_BUCKET",  "sensores")

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS",   "admin")
MINIO_SECRET    = os.getenv("MINIO_SECRET",   "Ilerna_Programaci0n")
MINIO_BUCKET    = "test-flow"

GRAFANA_URL     = "http://grafana:3000"
GRAFANA_USER    = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASS    = os.getenv("GRAFANA_PASS", "Ilerna_Programaci0n")

# ── Helpers ──────────────────────────────────────────────────
PASS = 0
FAIL = 0

GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
NC     = "\033[0m"


def ok(name: str, detail: str = "") -> None:
    global PASS
    PASS += 1
    print(f"  {GREEN}✅ {name}{NC}" + (f" — {detail}" if detail else ""))


def fail(name: str, detail: str = "") -> None:
    global FAIL
    FAIL += 1
    print(f"  {RED}❌ {name}{NC}" + (f" — {detail}" if detail else ""))


def section(title: str) -> None:
    print(f"\n── {title} {'─' * (50 - len(title))}")


# ── Tests ────────────────────────────────────────────────────

def test_mqtt() -> None:
    section("MQTT (Mosquitto)")
    try:
        import paho.mqtt.client as mqtt

        received = []
        client = mqtt.Client(client_id=f"test-{uuid.uuid4().hex[:8]}")
        client.on_message = lambda c, u, m: received.append(m.payload.decode())

        client.connect(MQTT_HOST, MQTT_PORT, keepalive=5)
        client.subscribe(MQTT_TOPIC)
        client.loop_start()

        payload = json.dumps({"sensor": "temp", "value": 23.5, "ts": time.time()})
        client.publish(MQTT_TOPIC, payload)
        time.sleep(1)
        client.loop_stop()
        client.disconnect()

        if received and received[0] == payload:
            ok("MQTT publish/subscribe", f"topic={MQTT_TOPIC}")
        else:
            fail("MQTT publish/subscribe", f"mensaje no recibido (recibidos: {received})")
    except ImportError:
        fail("MQTT", "paho-mqtt no instalado — pip install paho-mqtt")
    except Exception as e:
        fail("MQTT", str(e))


def test_redpanda() -> None:
    section("Redpanda (Kafka API)")
    try:
        from confluent_kafka import Producer, Consumer, KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic

        # Crear topic
        admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
        fs = admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
        for topic, f in fs.items():
            try:
                f.result()
                ok("Redpanda topic creado", topic)
            except KafkaException as e:
                if "already exists" in str(e).lower() or e.args[0].code() == 36:
                    ok("Redpanda topic ya existe", topic)
                else:
                    raise

        # Producir mensaje
        test_id = uuid.uuid4().hex[:8]
        delivered = []

        def on_delivery(err, msg):
            if err:
                delivered.append(("error", str(err)))
            else:
                delivered.append(("ok", msg.offset()))

        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        producer.produce(KAFKA_TOPIC, value=json.dumps({"test_id": test_id, "ts": time.time()}).encode(), callback=on_delivery)
        producer.flush(timeout=10)

        if delivered and delivered[0][0] == "ok":
            ok("Redpanda produce", f"test_id={test_id} offset={delivered[0][1]}")
        else:
            fail("Redpanda produce", str(delivered))

        # Consumir mensaje
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([KAFKA_TOPIC])
        found = False
        deadline = time.time() + 8
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                data = json.loads(msg.value().decode())
                if data.get("test_id") == test_id:
                    found = True
                    break
        consumer.close()

        if found:
            ok("Redpanda consume", f"test_id={test_id} encontrado")
        else:
            fail("Redpanda consume", f"test_id={test_id} no encontrado en 8s")

    except ImportError:
        fail("Redpanda", "confluent-kafka no instalado — pip install confluent-kafka")
    except Exception as e:
        fail("Redpanda", str(e))


def test_influxdb() -> None:
    section("InfluxDB")
    try:
        from influxdb_client import InfluxDBClient, Point
        from influxdb_client.client.write_api import SYNCHRONOUS

        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

        # Escribir punto
        write_api = client.write_api(write_options=SYNCHRONOUS)
        test_val = round(20.0 + (time.time() % 10), 2)
        point = Point("test_sensor").tag("source", "test_flow").field("temperature", test_val)
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        ok("InfluxDB write", f"temperature={test_val}")

        # Leer el punto escrito
        time.sleep(1)
        query_api = client.query_api()
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r._measurement == "test_sensor" and r.source == "test_flow")
          |> last()
        '''
        tables = query_api.query(query, org=INFLUX_ORG)
        records = [r for table in tables for r in table.records]

        if records:
            ok("InfluxDB query", f"{len(records)} registros encontrados")
        else:
            fail("InfluxDB query", "no se encontraron registros en el último minuto")

        client.close()
    except ImportError:
        fail("InfluxDB", "influxdb-client no instalado — pip install influxdb-client")
    except Exception as e:
        fail("InfluxDB", str(e))


def test_minio() -> None:
    section("RustFS (S3)")
    try:
        from minio import Minio
        from minio.error import S3Error
        import io

        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

        # Crear bucket si no existe
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            ok("RustFS bucket creado", MINIO_BUCKET)
        else:
            ok("RustFS bucket ya existe", MINIO_BUCKET)

        # Subir objeto
        obj_name = f"test-{uuid.uuid4().hex[:8]}.json"
        data = json.dumps({"test": "flow", "ts": time.time()}).encode()
        client.put_object(MINIO_BUCKET, obj_name, io.BytesIO(data), length=len(data))
        ok("RustFS put object", obj_name)

        # Verificar que existe
        objects = [o.object_name for o in client.list_objects(MINIO_BUCKET)]
        if obj_name in objects:
            ok("RustFS list objects", f"{len(objects)} objeto(s) en {MINIO_BUCKET}")
        else:
            fail("RustFS list objects", f"{obj_name} no encontrado")

        # Limpiar
        client.remove_object(MINIO_BUCKET, obj_name)

    except ImportError:
        fail("RustFS", "minio no instalado — pip install minio")
    except Exception as e:
        fail("RustFS", str(e))


def test_grafana() -> None:
    section("Grafana")
    try:
        import urllib.request
        import base64

        creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
        headers = {"Authorization": f"Basic {creds}"}

        # Health check
        req = urllib.request.Request(f"{GRAFANA_URL}/api/health", headers=headers)
        with urllib.request.urlopen(req, timeout=5) as r:
            body = json.loads(r.read())
        if body.get("database") == "ok":
            ok("Grafana health", f"db={body.get('database')}")
        else:
            fail("Grafana health", str(body))

        # Listar datasources
        req = urllib.request.Request(f"{GRAFANA_URL}/api/datasources", headers=headers)
        with urllib.request.urlopen(req, timeout=5) as r:
            ds = json.loads(r.read())
        ok("Grafana datasources", f"{len(ds)} datasource(s) configurado(s)")

    except Exception as e:
        fail("Grafana", str(e))


# ── Main ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print("")
    print("╔══════════════════════════════════════════════════════╗")
    print("║     ILERNA PAC DES - Test de Flujo Completo          ║")
    print("╚══════════════════════════════════════════════════════╝")

    test_mqtt()
    test_redpanda()
    test_influxdb()
    test_minio()
    test_grafana()

    total = PASS + FAIL
    print("\n──────────────────────────────────────────────────────")
    if FAIL == 0:
        print(f"  {GREEN}✅ Todos los tests pasaron ({PASS}/{total}){NC}")
    else:
        print(f"  {YELLOW}⚠️  {PASS}/{total} OK — {FAIL} fallos{NC}")
    print("")
    sys.exit(FAIL)
