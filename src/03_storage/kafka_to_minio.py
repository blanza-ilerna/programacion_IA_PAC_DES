"""
kafka_to_minio.py — Consumidor directo sensors_clean → RustFS JSON

Alternativa al flink_to_minio_job.py que no requiere JARs de Flink.
Lee de 'sensors_clean', acumula en ventanas de 60 s y escribe
archivos JSON particionados por Hive en RustFS:

  s3a://datalake/clean/year=2026/month=03/day=15/hour=21/<ts>.json

DuckDB puede leerlos con:
  read_json('s3://datalake/clean/**/*.json', hive_partitioning=true)

Uso:
  python src/03_storage/kafka_to_minio.py

Variables de entorno (opcionales):
  KAFKA_BROKER      (default: redpanda:29092)
  KAFKA_TOPIC       (default: sensors_clean)
  KAFKA_GROUP       (default: py-minio-writer)
  MINIO_ENDPOINT    (default: minio:9000)
  MINIO_ACCESS      (default: admin)
  MINIO_SECRET      (default: Ilerna_Programaci0n)
  MINIO_BUCKET      (default: datalake)
  MINIO_PATH        (default: clean)
  WINDOW_SECONDS    (default: 60)
"""

import io
import json
import os
import signal
import time
from datetime import datetime, timezone, timedelta

from confluent_kafka import Consumer, KafkaError, KafkaException
from minio import Minio

# ── Configuración ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:29092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "sensors_clean")
KAFKA_GROUP  = os.getenv("KAFKA_GROUP",  "py-minio-writer")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "Ilerna_Programaci0n")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",   "datalake")
MINIO_PATH     = os.getenv("MINIO_PATH",     "clean")

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))

_running = True
_stats = {"consumed": 0, "files_written": 0, "errors": 0}


def _signal_handler(sig, frame):
    global _running
    print("\n[rustfs-writer] Señal recibida, volcando y deteniendo...")
    _running = False


def _ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"[rustfs-writer] Bucket '{MINIO_BUCKET}' creado")


def _hive_path(ts: datetime, filename: str) -> str:
    """Construye la ruta Hive: clean/year=.../month=.../day=.../hour=.../<file>"""
    return (
        f"{MINIO_PATH}/"
        f"year={ts.strftime('%Y')}/"
        f"month={ts.strftime('%m')}/"
        f"day={ts.strftime('%d')}/"
        f"hour={ts.strftime('%H')}/"
        f"{filename}"
    )


def _flush(client: Minio, records: list[dict]) -> bool:
    if not records:
        return True
    try:
        now = datetime.now(timezone.utc)
        filename = now.strftime("%Y%m%d_%H%M%S") + ".json"
        object_name = _hive_path(now, filename)

        # Una línea JSON por registro (NDJSON — DuckDB lo lee directamente)
        data = "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode("utf-8")

        client.put_object(
            MINIO_BUCKET,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        _stats["files_written"] += 1
        print(
            f"[rustfs-writer] ✓ {len(records)} registros → "
            f"s3://{MINIO_BUCKET}/{object_name} ({len(data)/1024:.1f} KB)"
        )
        return True
    except Exception as e:
        _stats["errors"] += 1
        print(f"[rustfs-writer] ✗ Error escribiendo en RustFS: {e}")
        return False


def run():
    print("[rustfs-writer] Iniciando sensors_clean → RustFS JSON writer")
    print(f"  Kafka : {KAFKA_BROKER}  topic={KAFKA_TOPIC}  group={KAFKA_GROUP}")
    print(f"  RustFS : {MINIO_ENDPOINT}  bucket={MINIO_BUCKET}/{MINIO_PATH}  ventana={WINDOW_SECONDS}s")
    print()

    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    _ensure_bucket(minio_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id":          KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"[rustfs-writer] Suscrito a '{KAFKA_TOPIC}'. Ctrl+C para detener.\n")

    window: list[dict] = []
    window_start = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
            else:
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    window.append(record)
                    _stats["consumed"] += 1
                    if len(window) % 50 == 0:
                        print(f"[rustfs-writer] ← {len(window)} en ventana actual...")
                except Exception as e:
                    _stats["errors"] += 1
                    print(f"[rustfs-writer] ✗ Mensaje inválido: {e}")

            if time.monotonic() - window_start >= WINDOW_SECONDS:
                if window and _flush(minio_client, window):
                    try:
                        consumer.commit(asynchronous=False)
                    except Exception:
                        pass
                    window = []
                window_start = time.monotonic()

    finally:
        if window:
            _flush(minio_client, window)
        consumer.close()
        print(f"\n[rustfs-writer] Detenido. Stats: {_stats}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT,  _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    run()
