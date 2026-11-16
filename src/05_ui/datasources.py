"""Acceso a datos del dashboard: InfluxDB (hot), RustFS/DuckDB (cold), Flink y Kafka."""

import warnings
import json
import time
import duckdb
import pandas as pd
import requests
import streamlit as st
from influxdb_client.client.warnings import MissingPivotFunction
from confluent_kafka import Consumer, TopicPartition
from influxdb_client import InfluxDBClient

from settings import (
    INFLUX_URL,
    INFLUX_TOKEN,
    INFLUX_ORG,
    INFLUX_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS,
    MINIO_SECRET,
    MINIO_BUCKET,
    ALERT_THRESHOLD,
    FASTAPI_URL,
    FLINK_URL,
    KAFKA_BROKER,
    REFRESH_SEC,
)

warnings.simplefilter("ignore", MissingPivotFunction)

# ── Helpers InfluxDB ──────────────────────────────────────────

@st.cache_resource
def get_influx():
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


@st.cache_data(ttl=REFRESH_SEC, show_spinner=False)
def query_machine_status(range_min: int) -> pd.DataFrame:
    """Última lectura por máquina desde machine_stats (escritas por Flink)."""
    api = get_influx().query_api()
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{range_min}m)
      |> filter(fn: (r) => r._measurement == "machine_stats")
      |> filter(fn: (r) => r._field == "avg_temp_c" or r._field == "alert")
      |> group(columns: ["device_id", "_field"])
      |> last()
      |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
    """
    try:
        df = api.query_data_frame(query, org=INFLUX_ORG)
        if df.empty:
            return pd.DataFrame()
        df = df[["_time", "device_id", "avg_temp_c", "alert"]].dropna(subset=["avg_temp_c"])
        df["_time"] = pd.to_datetime(df["_time"], utc=True)
        return df
    except Exception as e:
        st.warning(f"InfluxDB: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEC, show_spinner=False)
def query_history(range_minutes: int) -> pd.DataFrame:
    """Serie temporal de temperatura por máquina."""
    api = get_influx().query_api()
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{range_minutes}m)
      |> filter(fn: (r) => r._measurement == "machine_stats")
      |> filter(fn: (r) => r._field == "avg_temp_c")
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    try:
        df = api.query_data_frame(query, org=INFLUX_ORG)
        if isinstance(df, list):
            if not df:
                return pd.DataFrame()
            df = pd.concat(df, ignore_index=True)
        if df.empty:
            return pd.DataFrame()
        df = df[["_time", "device_id", "avg_temp_c"]].rename(columns={"_time": "ts"})
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df.sort_values("ts")
    except Exception as e:
        st.warning(f"InfluxDB historial: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEC, show_spinner=False)
def query_alerts(range_minutes: int) -> pd.DataFrame:
    """Registros con alerta activa."""
    api = get_influx().query_api()
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{range_minutes}m)
      |> filter(fn: (r) => r._measurement == "machine_stats")
      |> filter(fn: (r) => r._field == "avg_temp_c")
      |> filter(fn: (r) => r._value > {ALERT_THRESHOLD})
      |> sort(columns: ["_time"], desc: true)
    """
    try:
        df = api.query_data_frame(query, org=INFLUX_ORG)
        if df.empty:
            return pd.DataFrame()
        df = df[["_time", "device_id", "_value"]].rename(
            columns={"_time": "ts", "_value": "avg_temp_c"}
        )
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df
    except Exception:
        return pd.DataFrame()


# ── DuckDB helpers ────────────────────────────────────────────

def _init_duckdb_s3(conn: duckdb.DuckDBPyConnection):
    """Configura DuckDB con credenciales RustFS (S3-compatible)."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        SET s3_endpoint          = '{MINIO_ENDPOINT}';
        SET s3_access_key_id     = '{MINIO_ACCESS}';
        SET s3_secret_access_key = '{MINIO_SECRET}';
        SET s3_use_ssl           = false;
        SET s3_url_style         = 'path';
    """)


@st.cache_data(ttl=120, show_spinner="Cargando datos históricos desde RustFS...")
def query_cold_path() -> pd.DataFrame:
    """Lee NDJSON desde RustFS cold path (kafka_to_minio.py).

    Los ficheros están en: datalake/clean/year=YYYY/month=MM/day=DD/hour=HH/*.json
    Escritos por kafka_to_minio.py con schema de sensors_clean:
      device_id, temperature_c, unit_original, ts, ...
    """
    conn = duckdb.connect()
    try:
        _init_duckdb_s3(conn)
        df = conn.execute(f"""
            SELECT device_id,
                   CAST(temperature_c AS DOUBLE) AS value,
                   ts,
                   CAST(year  AS VARCHAR) AS year,
                   CAST(month AS VARCHAR) AS month,
                   CAST(day   AS VARCHAR) AS day,
                   CAST(hour  AS VARCHAR) AS hour
            FROM read_json(
                's3://{MINIO_BUCKET}/clean/**/*.json',
                hive_partitioning = true,
                columns = {{
                    device_id:     'VARCHAR',
                    temperature_c: 'DOUBLE',
                    ts:            'VARCHAR'
                }}
            )
            WHERE temperature_c IS NOT NULL
              AND device_id IS NOT NULL
            ORDER BY ts DESC
            LIMIT 5000
        """).df()
        if not df.empty:
            df["source"] = "minio-json"
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def query_lambda_federated(hot_df: pd.DataFrame, cold_df: pd.DataFrame) -> pd.DataFrame:
    """
    Query federada Lambda Architecture con DuckDB en memoria.

    Combina:
      · hot_df  → datos recientes de InfluxDB (hot path, baja latencia)
      · cold_df → datos históricos de RustFS Parquet (cold path, alta escala)

    DuckDB hace un UNION ALL en memoria sin necesidad de mover datos a disco.
    """
    if hot_df.empty and cold_df.empty:
        return pd.DataFrame()

    conn = duckdb.connect()
    try:
        # Registrar DataFrames como tablas virtuales en DuckDB
        if not hot_df.empty:
            conn.register("hot_data",  hot_df)
        if not cold_df.empty:
            conn.register("cold_data", cold_df)

        if not hot_df.empty and not cold_df.empty:
            query = """
            SELECT device_id, value, ts, 'InfluxDB (hot)'  AS source FROM hot_data
            UNION ALL
            SELECT device_id, value, ts, 'RustFS (cold)' AS source FROM cold_data
            ORDER BY ts DESC
            LIMIT 5000
            """
        elif not hot_df.empty:
            query = "SELECT device_id, value, ts, 'InfluxDB (hot)' AS source FROM hot_data ORDER BY ts DESC"
        else:
            query = "SELECT device_id, value, ts, 'RustFS (cold)' AS source FROM cold_data ORDER BY ts DESC LIMIT 5000"

        return conn.execute(query).df()
    finally:
        conn.close()


# ── Helpers Pipeline Health ───────────────────────────────────

def get_flink_jobs() -> list:
    """Consulta la API REST de Flink y devuelve lista de jobs con estado."""
    try:
        jobs_resp = requests.get(f"{FLINK_URL}/jobs", timeout=3).json().get("jobs", [])
        result = []
        for j in jobs_resp:
            try:
                detail = requests.get(f"{FLINK_URL}/jobs/{j['id']}", timeout=3).json()
                result.append({
                    "id":       j["id"][:8] + "…",
                    "nombre":   detail.get("name", "—"),
                    "estado":   detail.get("state", j.get("status", "?")),
                    "inicio":   pd.to_datetime(detail.get("start-time", 0), unit="ms", utc=True).strftime("%H:%M:%S") if detail.get("start-time") else "—",
                    "duracion": f"{int(detail.get('duration', 0) / 1000 / 60)} min" if detail.get("duration") else "—",
                })
            except Exception:
                result.append({"id": j["id"][:8] + "…", "nombre": "—", "estado": j.get("status", "?"), "inicio": "—", "duracion": "—"})
        return result
    except Exception:
        return []


def get_kafka_offsets(topics: list) -> dict:
    """Obtiene el offset máximo (número de mensajes) de cada topic via Kafka Admin."""
    result = {}
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BROKER, "socket.timeout.ms": 3000})
        meta = admin.list_topics(timeout=3)
        for topic in topics:
            if topic in meta.topics:
                partitions = meta.topics[topic].partitions
                result[topic] = {"particiones": len(partitions), "estado": "OK"}
            else:
                result[topic] = {"particiones": 0, "estado": "no existe"}
    except Exception:
        for topic in topics:
            result[topic] = {"particiones": "?", "estado": "sin conexión"}
    return result


def get_dlq_messages(max_msgs: int = 20) -> list:
    """Lee los últimos N mensajes del topic sensors_invalid (DLQ)."""
    msgs = []
    try:
        c = Consumer({
            "bootstrap.servers":  KAFKA_BROKER,
            "group.id":           "streamlit-dlq-viewer",
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,
            "socket.timeout.ms":  3000,
            "session.timeout.ms": 6000,
        })
        c.assign([TopicPartition("sensors_invalid", 0, 0)])
        deadline = time.time() + 3
        while len(msgs) < max_msgs and time.time() < deadline:
            msg = c.poll(0.5)
            if msg is None:
                break
            if msg.error():
                break
            try:
                msgs.append(json.loads(msg.value().decode("utf-8")))
            except Exception:
                pass
        c.close()
    except Exception:
        pass
    return msgs


def get_verified_count() -> int:
    """Estima mensajes en sensors_verified leyendo el offset máximo."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BROKER, "socket.timeout.ms": 3000})
        meta = admin.list_topics(timeout=3)
        if "sensors_verified" not in meta.topics:
            return 0
        c = Consumer({
            "bootstrap.servers":  KAFKA_BROKER,
            "group.id":           "streamlit-offset-checker",
            "auto.offset.reset":  "latest",
            "enable.auto.commit": False,
            "socket.timeout.ms":  3000,
        })
        partitions = [TopicPartition("sensors_verified", p) for p in meta.topics["sensors_verified"].partitions]
        _, high = c.get_watermark_offsets(partitions[0], timeout=3)
        c.close()
        return high
    except Exception:
        return -1


@st.cache_data(ttl=10, show_spinner=False)
def check_services() -> dict:
    """Estado (True/False) de FastAPI, Flink, Kafka e InfluxDB. Cacheado 10 s."""
    status = {}
    for name, url in {
        "FastAPI":  FASTAPI_URL + "/health",
        "Flink":    FLINK_URL + "/overview",
        "InfluxDB": INFLUX_URL + "/health",
    }.items():
        try:
            status[name] = requests.get(url, timeout=2).ok
        except Exception:
            status[name] = False
    try:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": KAFKA_BROKER, "socket.timeout.ms": 2000}).list_topics(timeout=2)
        status["Kafka"] = True
    except Exception:
        status["Kafka"] = False
    return {k: status[k] for k in ("FastAPI", "Flink", "Kafka", "InfluxDB")}
