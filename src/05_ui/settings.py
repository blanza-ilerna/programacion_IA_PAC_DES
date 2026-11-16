"""Configuración del dashboard (variables de entorno con valores por defecto Docker)."""

import os

# ── Configuración ─────────────────────────────────────────────
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "supersecrettoken")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "ilerna")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensores")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "Ilerna_Programaci0n")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",   "datalake")

ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "80.0"))
REFRESH_SEC     = int(os.getenv("REFRESH_SECONDS",   "5"))
FASTAPI_URL     = os.getenv("FASTAPI_URL", "http://localhost:8000")
FLINK_URL       = os.getenv("FLINK_URL",   "http://jobmanager:8081")
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "redpanda:29092")
