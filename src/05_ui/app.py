"""
app.py - Dashboard Streamlit — ILERNA Smart-Industry (Hito 4)

Punto de entrada: configura la página, la barra lateral y reparte cada pestaña
a su módulo en views/. Los accesos a datos están en data.py y la configuración
(variables de entorno) en config.py.

Pestañas (views/):
  📡 Tiempo Real       — realtime.py        Temperatura actual de cada máquina (InfluxDB)
  📈 Historial         — history.py         Serie temporal de temperatura normalizada
  ⚠️  Alertas          — alerts.py          Eventos con avg > 80°C
  🗄️  Lambda Query     — lambda_query.py    InfluxDB (hot) + MinIO NDJSON (cold) via DuckDB
  🤖 IA Anomalías      — ai_anomalies.py    Predicción con IsolationForest desde FastAPI
  🏥 Pipeline Health   — pipeline_health.py Estado de servicios, jobs Flink y topics Kafka
  🔐 Hash Chain        — hash_security.py   Integridad SHA256 por dispositivo (verified / DLQ)
  🔬 Modelos Avanzados — advanced_models.py Prophet, RandomForest, CUSUM, K-Means
  ❓ Ayuda            — ayuda.py           Arquitectura, endpoints FastAPI, comandos de arranque

Arquitectura Lambda:
  Hot path  → Flink → InfluxDB                    (segundos de latencia, últimas horas)
  Cold path → kafka_to_minio.py → MinIO (NDJSON)  (minutos de latencia, histórico)
  Query layer → DuckDB UNION ALL                  (visión unificada en tiempo real)

Uso:
  streamlit run src/05_ui/app.py --server.port 8501

Auto-refresco: solo las pestañas en vivo (Tiempo Real, Historial, Alertas,
Pipeline Health, Hash Chain) y el estado de servicios se refrescan; Lambda,
IA y Modelos Avanzados se ejecutan bajo demanda.
"""

import streamlit as st

from datasources import check_services
from settings import ALERT_THRESHOLD, INFLUX_URL, REFRESH_SEC
from views import (
    advanced_models,
    ai_anomalies,
    alerts,
    ayuda,
    hash_security,
    history,
    lambda_query,
    pipeline_health,
    realtime,
)

# ── Página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="ILERNA Smart-Industry",
    page_icon="🏭",
    layout="wide",
)
st.title("🏭 ILERNA Smart-Industry — Monitor de Temperatura")

# ── Auto-refresco por fragmentos ──────────────────────────────
# En vez de dormir y relanzar TODO el script (lo que bloqueaba la UI, repetía
# todas las consultas y borraba los resultados de los botones), solo las zonas
# "en vivo" se refrescan cada REFRESH_SEC. El resto reacciona solo a la interacción.
auto_refresh = st.session_state.get("auto_refresh", True)


def live(fn):
    """Envuelve fn en un fragmento que se refresca solo si el auto-refresco está activo."""
    return st.fragment(run_every=REFRESH_SEC if auto_refresh else None)(fn)


def _service_status() -> None:
    for name, ok in check_services().items():
        st.caption(("🟢" if ok else "🔴") + f" {name}")


# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Opciones")
    range_min = st.slider("Ventana tiempo real (min)", 1, 120, 15)
    st.toggle("Auto-refresco", value=True, key="auto_refresh")
    if st.button("🔄 Refrescar"):
        st.rerun()
    st.divider()
    st.caption(f"Umbral alerta: **{ALERT_THRESHOLD}°C**")
    st.caption(f"InfluxDB: `{INFLUX_URL}`")
    st.divider()
    st.markdown("**Estado servicios**")
    live(_service_status)()


# ── Pestañas ──────────────────────────────────────────────────
tab_rt, tab_hist, tab_alerts, tab_lambda, tab_ai, tab_health, tab_hash, tab_advanced, tab_help = st.tabs([
    "📡 Tiempo Real",
    "📈 Historial",
    "⚠️  Alertas",
    "🗄️  Lambda Query",
    "🤖 IA Anomalías",
    "🏥 Pipeline Health",
    "🔐 Hash Chain",
    "🔬 Modelos Avanzados",
    "❓ Ayuda",
])

with tab_rt:
    live(realtime.render)(range_min)
with tab_hist:
    live(history.render)()
with tab_alerts:
    live(alerts.render)()
with tab_lambda:
    lambda_query.render()
with tab_ai:
    ai_anomalies.render()
with tab_health:
    live(pipeline_health.render)()
with tab_hash:
    live(hash_security.render)()
with tab_advanced:
    advanced_models.render()
with tab_help:
    ayuda.render()
