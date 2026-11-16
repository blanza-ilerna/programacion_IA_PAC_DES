"""Pestaña 🏥 Pipeline Health: servicios, jobs Flink y topics Kafka."""

import pandas as pd
import requests
import streamlit as st

from settings import (
    INFLUX_URL,
    MINIO_ENDPOINT,
    FASTAPI_URL,
    FLINK_URL,
)
from datasources import (
    get_flink_jobs,
    get_kafka_offsets,
)


def render() -> None:
    st.subheader("🏥 Estado del Pipeline")

    if st.button("🔄 Actualizar estado", key="refresh_health"):
        st.rerun()

    # ── Servicios ────────────────────────────────────────────
    st.markdown("### Servicios")
    services = {
        "FastAPI":   (FASTAPI_URL + "/health",         "GET"),
        "InfluxDB":  (INFLUX_URL + "/health",           "GET"),
        "Flink":     (FLINK_URL + "/overview",          "GET"),
    }
    minio_url = f"http://{MINIO_ENDPOINT}/minio/health/live"

    svc_cols = st.columns(4)
    for idx, (name, (url, _)) in enumerate(services.items()):
        with svc_cols[idx]:
            try:
                r = requests.get(url, timeout=3)
                if r.ok:
                    st.success(f"🟢 **{name}**\n\nOK")
                else:
                    st.error(f"🔴 **{name}**\n\nHTTP {r.status_code}")
            except Exception:
                st.error(f"🔴 **{name}**\n\nSin respuesta")

    with svc_cols[3]:
        try:
            r = requests.get(minio_url, timeout=3)
            st.success("🟢 **RustFS**\n\nOK") if r.ok else st.error(f"🔴 **RustFS**\n\nHTTP {r.status_code}")
        except Exception:
            st.error("🔴 **RustFS**\n\nSin respuesta")

    st.divider()

    # ── Jobs Flink ───────────────────────────────────────────
    st.markdown("### Jobs Flink")
    jobs = get_flink_jobs()
    if not jobs:
        st.warning("No se puede conectar con Flink JobManager (`jobmanager:8081`)")
    else:
        jobs_df = pd.DataFrame(jobs)
        # Colorear estado
        def _color_estado(val):
            if val == "RUNNING":
                return "background-color: #1a4a1a; color: #4caf50"
            if val in ("FAILED", "CANCELED"):
                return "background-color: #4a1a1a; color: #f44336"
            return ""

        st.dataframe(
            jobs_df.style.map(_color_estado, subset=["estado"]),
            width='stretch',
            hide_index=True,
        )

        running = sum(1 for j in jobs if j["estado"] == "RUNNING")
        failed  = sum(1 for j in jobs if j["estado"] in ("FAILED", "CANCELED"))
        c1, c2, c3 = st.columns(3)
        c1.metric("Jobs RUNNING", running)
        c2.metric("Jobs FAILED",  failed,  delta=f"-{failed}" if failed else None, delta_color="inverse")
        c3.metric("Jobs total",   len(jobs))

    st.divider()

    # ── Topics Kafka ─────────────────────────────────────────
    st.markdown("### Topics Kafka (Redpanda)")
    topics_check = ["sensors_raw", "sensors_clean", "sensors_verified", "sensors_invalid"]
    offsets = get_kafka_offsets(topics_check)
    if all(v["estado"] == "sin conexión" for v in offsets.values()):
        st.warning("No se puede conectar con Redpanda (`redpanda:29092`)")
    else:
        topic_rows = []
        for t, info in offsets.items():
            topic_rows.append({"topic": t, "particiones": info["particiones"], "estado": info["estado"]})
        st.dataframe(pd.DataFrame(topic_rows), width='stretch', hide_index=True)

    st.divider()

    # ── Flink overview ───────────────────────────────────────
    st.markdown("### Recursos Flink")
    try:
        ov = requests.get(f"{FLINK_URL}/overview", timeout=3).json()
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Task Slots totales",      ov.get("slots-total", "?"))
        r2.metric("Task Slots disponibles",  ov.get("slots-available", "?"))
        r3.metric("TaskManagers",            ov.get("taskmanagers", "?"))
        r4.metric("Jobs en ejecución",       ov.get("jobs-running", "?"))
    except Exception:
        st.info("Flink overview no disponible")
