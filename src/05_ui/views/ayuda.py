"""Pestaña ❓ Ayuda: arquitectura, endpoints y comandos."""

import pandas as pd
import requests
import streamlit as st

from settings import (
    INFLUX_URL,
    INFLUX_ORG,
    INFLUX_BUCKET,
    MINIO_ENDPOINT,
    MINIO_BUCKET,
    ALERT_THRESHOLD,
    REFRESH_SEC,
    FASTAPI_URL,
    FLINK_URL,
    KAFKA_BROKER,
)


def render() -> None:
    st.subheader("❓ Guía rápida del dashboard")

    st.markdown("""
    Este dashboard monitoriza el pipeline de telemetría IoT en tiempo real.
    Usa las pestañas de arriba para navegar entre las diferentes vistas.
    """)

    st.divider()

    # ── Pestañas ─────────────────────────────────────────────
    st.markdown("### Pestañas disponibles")
    st.markdown("""
    | Pestaña | Descripción |
    |---------|-------------|
    | **📡 Tiempo Real** | Temperatura actual de cada máquina. Indicadores con alerta si supera el umbral. |
    | **📈 Historial** | Serie temporal de temperatura normalizada. Selecciona una o varias máquinas. |
    | **⚠️ Alertas** | Registros donde la temperatura promedio superó el umbral configurado. |
    | **🗄️ Lambda Query** | Query federada: combina InfluxDB (hot path) y RustFS (cold path) con DuckDB. |
    | **🤖 IA Anomalías** | IsolationForest desde FastAPI. Envía datos en batch y colorea por probabilidad de fallo. |
    | **🏥 Pipeline Health** | Estado de servicios, jobs Flink activos y topics Kafka. |
    | **🔐 Hash Chain** | Integridad SHA256 por dispositivo. Muestra mensajes rechazados en el DLQ. |
    | **🔬 Modelos Avanzados** | Prophet (forecasting), RandomForest, CUSUM (cambio de régimen), K-Means (clustering). |
    """)

    st.divider()

    # ── Arquitectura ──────────────────────────────────────────
    st.markdown("### Arquitectura del pipeline")
    st.markdown("""
    ```
    [sensor_simulator.py]  →  MQTT  →  [Mosquitto]
                                              |
                                  [mqtt_to_redpanda_bridge.py]
                                              |  sensors_raw
                                        [Redpanda]
                                         |        |
                           [flink_hash_verifier]  [flink_normalization]
                           sensors_verified/       sensors_clean
                           sensors_invalid (DLQ)        |
                                              [flink_analytics]
                                                    |
                                ┌───────────────────┤
                           [InfluxDB]           [kafka_to_minio.py]
                        (hot path, <1min)       [RustFS NDJSON]
                                |               (cold path, 60s)
                           [FastAPI]
                           [Streamlit ← este dashboard]
    ```
    """)

    st.divider()

    # ── Endpoints FastAPI ─────────────────────────────────────
    st.markdown("### Endpoints FastAPI")
    st.markdown(f"Base URL: `{FASTAPI_URL}` — Documentación interactiva: `{FASTAPI_URL}/docs`")

    endpoints_data = [
        ("GET",  "/health",          "Health check del servicio"),
        ("GET",  "/machines/status", "Estado actual de todas las máquinas"),
        ("GET",  "/machines/{id}",   "Detalle de una máquina específica"),
        ("GET",  "/alerts",          "Últimas alertas activas"),
        ("POST", "/model/train",     "Entrena el modelo IsolationForest con datos recientes"),
        ("POST", "/model/predict",   "Predice anomalías para un batch de lecturas"),
    ]
    for method, path, desc in endpoints_data:
        badge = ":blue[GET]" if method == "GET" else ":orange[POST]"
        st.markdown(f"- {badge} `{path}` — {desc}")

    col_try1, col_try2 = st.columns(2)
    with col_try1:
        if st.button("🔗 Abrir /docs (FastAPI)"):
            st.markdown(f"[Abre {FASTAPI_URL}/docs]({FASTAPI_URL}/docs)")
    with col_try2:
        if st.button("🩺 Probar /health"):
            try:
                r = requests.get(f"{FASTAPI_URL}/health", timeout=3)
                st.json(r.json())
            except Exception as e:
                st.error(f"No se puede conectar: {e}")

    st.divider()

    # ── Variables de entorno ───────────────────────────────────
    st.markdown("### Configuración activa")
    with st.expander("Ver variables de entorno"):
        config_rows = [
            ("INFLUX_URL",    INFLUX_URL),
            ("INFLUX_ORG",    INFLUX_ORG),
            ("INFLUX_BUCKET", INFLUX_BUCKET),
            ("MINIO_ENDPOINT", MINIO_ENDPOINT),
            ("MINIO_BUCKET",  MINIO_BUCKET),
            ("FASTAPI_URL",   FASTAPI_URL),
            ("FLINK_URL",     FLINK_URL),
            ("KAFKA_BROKER",  KAFKA_BROKER),
            ("ALERT_THRESHOLD", str(ALERT_THRESHOLD)),
            ("REFRESH_SECONDS", str(REFRESH_SEC)),
        ]
        st.dataframe(
            pd.DataFrame(config_rows, columns=["Variable", "Valor"]),
            width='stretch',
            hide_index=True,
        )

    st.divider()

    # ── Comandos de arranque ───────────────────────────────────
    st.markdown("### Comandos de arranque del pipeline")
    st.code("""
# Desde el directorio raíz del proyecto (o usar los aliases):

# 1. Simulador de sensores (publica en MQTT)
python src/01_ingestion/sensor_simulator.py --machines 5 --fault-rate 0.1
# alias: sim

# 2. Bridge MQTT → Redpanda
python src/01_ingestion/mqtt_to_redpanda_bridge.py
# alias: bridge

# 3. Jobs Flink (desde el jobmanager)
flink-restart

# 4. Cold path: Kafka → RustFS
python src/03_storage/kafka_to_minio.py
# alias: minio-writer

# 5. FastAPI
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
# alias: api

# 6. Este dashboard
streamlit run src/05_ui/app.py --server.port 8501
# alias: ui
    """, language="bash")

    st.divider()

    # ── Servicios ────────────────────────────────────────────
    st.markdown("### Servicios Docker")
    services_info = [
        ("Mosquitto",       "11883",       "Broker MQTT",                    "anónimo"),
        ("Grafana",         "13000",       "Dashboards",                     "acceso anónimo"),
        ("FastAPI",         "18000",       "API REST + modelo IA",           "—"),
        ("Redpanda Console","18080",       "UI de topics y mensajes",        "—"),
        ("Flink UI",        "18081",       "Jobs y métricas Flink",          "—"),
        ("InfluxDB",        "18086",       "Series temporales (hot path)",   "admin / Ilerna_Programaci0n"),
        ("Streamlit",       "18501",       "Este dashboard",                 "—"),
        ("JupyterLab",      "18888",       "Notebooks de análisis",          "—"),
        ("RustFS S3 API",    "19000",       "API S3 compatible",              "admin / Ilerna_Programaci0n"),
        ("RustFS Console",   "19001",       "Consola web RustFS",              "admin / Ilerna_Programaci0n"),
        ("Redpanda",        "19092",       "Broker Kafka-compatible",        "—"),
    ]
    st.dataframe(
        pd.DataFrame(services_info, columns=["Servicio", "Puerto", "Descripción", "Credenciales"]),
        width='stretch',
        hide_index=True,
    )

    st.divider()
    st.caption("ILERNA Smart-Industry — Programación IA PAC DES")
