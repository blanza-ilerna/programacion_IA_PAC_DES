"""Pestaña 🤖 IA Anomalías: IsolationForest vía FastAPI."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

from settings import (
    ALERT_THRESHOLD,
    FASTAPI_URL,
)
from datasources import (
    query_history,
)


def render() -> None:
    st.subheader("🤖 IA Cloud — Detección de Anomalías con IsolationForest")
    st.markdown("""
    **Edge vs Cloud Intelligence:**
    - **Flink** (Edge): regla fija `avg_temp > 80°C` — determinista, sin modelo.
    - **FastAPI + IsolationForest** (Cloud): aprende el patrón normal de cada máquina
      y detecta anomalías estadísticas, aunque no superen el umbral fijo.

    *IsolationForest* (scikit-learn): algoritmo no supervisado que "aísla" puntos
    atípicos usando árboles de decisión aleatorios. No necesita datos etiquetados.
    """)

    # ── Estado del modelo ─────────────────────────────────────
    col_status, col_train = st.columns(2)

    with col_status:
        st.markdown("**Estado del modelo**")
        try:
            resp = requests.get(f"{FASTAPI_URL}/model/status", timeout=3)
            if resp.ok:
                status = resp.json()
                if status.get("trained"):
                    st.success(f"✅ Modelo entrenado con {status['samples']:,} muestras")
                    if "stats" in status:
                        s = status["stats"]
                        st.json({
                            "media": f"{s.get('mean', 0):.1f}°C",
                            "std":   f"{s.get('std', 0):.1f}°C",
                            "rango": f"[{s.get('p5', 0):.1f}, {s.get('p95', 0):.1f}]°C (p5-p95)",
                        })
                else:
                    st.warning("⚠️ Modelo no entrenado")
        except Exception:
            st.error("FastAPI no disponible")

    with col_train:
        st.markdown("**Entrenar modelo**")
        train_range = st.slider("Minutos de histórico para entrenamiento", 10, 480, 120)
        contamination = st.slider("Tasa de anomalías esperada (%)", 1, 30, 10) / 100
        if st.button("🧠 Entrenar IsolationForest"):
            try:
                resp = requests.post(
                    f"{FASTAPI_URL}/model/train",
                    params={"range_minutes": train_range, "contamination": contamination},
                    timeout=30,
                )
                if resp.ok:
                    result = resp.json()
                    st.success(f"✅ Entrenado con {result['samples']:,} muestras")
                    st.json(result.get("stats", {}))
                else:
                    st.error(f"Error: {resp.json().get('detail', resp.text)}")
            except Exception as e:
                st.error(f"Error conectando a FastAPI: {e}")

    st.divider()

    # ── Predicción individual ─────────────────────────────────
    st.markdown("**Predicción manual**")
    col_m, col_t, col_btn = st.columns([2, 2, 1])
    with col_m:
        pred_device = st.selectbox("Máquina", list({
            "machine-001", "machine-002", "machine-003", "machine-004", "machine-005"
        }))
    with col_t:
        pred_temp = st.number_input("Temperatura (°C)", value=75.0, min_value=-50.0, max_value=200.0)
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        predict_btn = st.button("🔍 Predecir")

    if predict_btn:
        try:
            resp = requests.get(
                f"{FASTAPI_URL}/machines/{pred_device}/predict",
                params={"temperature_c": pred_temp},
                timeout=5,
            )
            if resp.ok:
                r = resp.json()
                if r.get("is_anomaly"):
                    st.error(f"⚠️ **ANOMALÍA DETECTADA** — Probabilidad de fallo: {r['failure_prob']:.1%}")
                else:
                    st.success(f"✅ Comportamiento normal — Score: {r['anomaly_score']:.3f}")
                st.info(r.get("interpretation", ""))
                st.json({k: v for k, v in r.items() if k != "model_stats"})
            else:
                try:
                    detail = resp.json().get("detail", resp.text)
                except Exception:
                    detail = resp.text or f"HTTP {resp.status_code}"
                st.error(f"Error API: {detail}")
        except Exception as e:
            st.error(f"Error: {e}")

    st.divider()

    # ── Análisis de rango completo ────────────────────────────
    st.markdown("**Análisis de anomalías en el histórico reciente**")
    if st.button("🔬 Analizar todas las máquinas (últimos 30 min)"):
        hist_df = query_history(30)
        if hist_df.empty:
            st.warning("Sin datos en InfluxDB.")
        else:
            results = []
            for _, row in hist_df.iterrows():
                try:
                    resp = requests.get(
                        f"{FASTAPI_URL}/machines/{row['device_id']}/predict",
                        params={"temperature_c": row["avg_temp_c"]},
                        timeout=3,
                    )
                    if resp.ok:
                        r = resp.json()
                        results.append({
                            "ts":           row["ts"],
                            "device_id":    row["device_id"],
                            "avg_temp_c":   row["avg_temp_c"],
                            "is_anomaly":   r["is_anomaly"],
                            "failure_prob": r["failure_prob"],
                            "score":        r["anomaly_score"],
                        })
                except Exception:
                    pass

            if not results:
                st.warning("No se obtuvo respuesta de la API. ¿Está FastAPI corriendo (`api`)? ¿Está el modelo entrenado?")
            if results:
                res_df = pd.DataFrame(results)
                anomalies = res_df[res_df["is_anomaly"]]
                st.metric("Anomalías detectadas", f"{len(anomalies)} / {len(res_df)}")

                fig = px.scatter(
                    res_df,
                    x="ts",
                    y="avg_temp_c",
                    color="failure_prob",
                    symbol="device_id",
                    size="failure_prob",
                    color_continuous_scale="RdYlGn_r",
                    title="Probabilidad de fallo por lectura (IsolationForest)",
                    template="plotly_dark",
                )
                fig.add_hline(y=ALERT_THRESHOLD, line_dash="dash", line_color="orange")
                fig.update_layout(
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.2,
                        xanchor="left",
                        x=0,
                    )
                )
                st.plotly_chart(fig, width='stretch')

                # ── Histograma de scores ──────────────────────
                st.markdown("#### Distribución de anomaly scores")
                fig_hist = go.Figure()
                for dev in res_df["device_id"].unique():
                    d_dev = res_df[res_df["device_id"] == dev]
                    fig_hist.add_trace(go.Histogram(
                        x=d_dev["score"], name=dev,
                        opacity=0.7, nbinsx=20,
                    ))
                fig_hist.update_layout(
                    barmode="overlay",
                    title="Distribución de anomaly_score por máquina (valores más negativos = más anómalo)",
                    xaxis_title="Anomaly Score", yaxis_title="Frecuencia",
                    height=300, template="plotly_dark",
                )
                st.plotly_chart(fig_hist, width='stretch')

                # ── Tabla resumen por máquina ─────────────────
                summary = res_df.groupby("device_id").agg(
                    lecturas    = ("score", "count"),
                    anomalias   = ("is_anomaly", "sum"),
                    score_medio = ("score", "mean"),
                    prob_media  = ("failure_prob", "mean"),
                ).reset_index()
                summary["score_medio"] = summary["score_medio"].round(4)
                summary["prob_media"]  = summary["prob_media"].apply(lambda x: f"{x:.1%}")
                st.dataframe(summary, width='stretch', hide_index=True)

                st.download_button(
                    "⬇️ Descargar CSV (resultados IA)",
                    data=res_df.to_csv(index=False),
                    file_name="anomalias_ia.csv",
                    mime="text/csv",
                )
