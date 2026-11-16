"""Pestaña 🗄️ Lambda Query: InfluxDB (hot) + MinIO (cold) vía DuckDB."""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from settings import (
    ALERT_THRESHOLD,
)
from datasources import (
    query_history,
    query_cold_path,
    query_lambda_federated,
)


def render() -> None:
    st.subheader("🗄️ Arquitectura Lambda — Query Federada con DuckDB")
    st.markdown("""
    **Concepto:** Los datos fluyen por dos caminos paralelos:

    | Path | Tecnología | Latencia | Uso |
    |------|-----------|----------|-----|
    | **Hot** (tiempo real) | Flink → InfluxDB | segundos | Alertas, monitorización |
    | **Cold** (histórico) | Flink → MinIO/Parquet | minutos | Análisis mensual, ML |

    **DuckDB** actúa como capa de consulta unificada: hace `UNION ALL` de ambas fuentes
    en memoria sin mover datos, gracias a la extensión `httpfs` para S3.
    """)

    col1, col2 = st.columns(2)
    with col1:
        hist_range_l = st.slider("Datos InfluxDB (min)", 5, 480, 60, key="lambda_range")
    with col2:
        run_lambda = st.button("🔗 Ejecutar Query Federada")

    if run_lambda:
        with st.spinner("Consultando InfluxDB (hot path)..."):
            hot_df = query_history(hist_range_l)
            if not hot_df.empty:
                hot_df = hot_df.rename(columns={"avg_temp_c": "value"})
                hot_df["ts"] = hot_df["ts"].astype(str)

        with st.spinner("Consultando MinIO Parquet (cold path)..."):
            cold_df = query_cold_path()
            if not cold_df.empty and "ts" in cold_df.columns:
                cold_df["ts"] = cold_df["ts"].astype(str)

        hot_count  = len(hot_df)  if not hot_df.empty  else 0
        cold_count = len(cold_df) if not cold_df.empty else 0
        total      = hot_count + cold_count

        col_h, col_c, col_pct = st.columns(3)
        col_h.metric("🔴 Hot path (InfluxDB)", f"{hot_count:,} registros")
        col_c.metric("🔵 Cold path (MinIO)", f"{cold_count:,} registros")
        if total > 0:
            col_pct.metric(
                "Proporción hot/cold",
                f"{hot_count/total*100:.0f}% / {cold_count/total*100:.0f}%",
            )

        # Indicador visual de proporción
        if total > 0:
            fig_prop = go.Figure(go.Bar(
                x=[hot_count, cold_count],
                y=["Fuentes"],
                orientation="h",
                marker_color=["#EF553B", "#636EFA"],
                text=[f"InfluxDB {hot_count:,}", f"MinIO {cold_count:,}"],
                textposition="inside",
            ))
            fig_prop.update_layout(
                height=80, showlegend=False, template="plotly_dark",
                margin=dict(l=0, r=0, t=0, b=0),
                xaxis=dict(showticklabels=False),
            )
            st.plotly_chart(fig_prop, width='stretch')

        if hot_count + cold_count == 0:
            st.warning("Sin datos en ninguna fuente. ¿Está el pipeline corriendo?")
        else:
            with st.spinner("DuckDB: unificando fuentes (UNION ALL)..."):
                unified = query_lambda_federated(hot_df, cold_df)

            st.success(f"✅ Query federada completada: {len(unified):,} registros totales")

            # ── Gráfica unificada ─────────────────────────────
            unified["ts_dt"] = pd.to_datetime(unified["ts"], errors="coerce", utc=True)

            # Colores por familia: rojos para hot, azules para cold
            _HOT_COLORS  = ["#FF2222", "#CC0000", "#FF6600", "#990000", "#FF8C69"]
            _COLD_COLORS = ["#636EFA", "#00B4D8", "#1F77B4", "#0077B6", "#48CAE4"]
            _devices = sorted(unified["device_id"].dropna().unique())
            _color_map = {}
            for i, dev in enumerate(_devices):
                _color_map[f"{dev} (hot)"]  = _HOT_COLORS[i % len(_HOT_COLORS)]
                _color_map[f"{dev} (cold)"] = _COLD_COLORS[i % len(_COLD_COLORS)]

            unified["trace_key"] = (
                unified["device_id"]
                + unified["source"].map({
                    "InfluxDB (hot)": " (hot)",
                    "MinIO (cold)":   " (cold)",
                }).fillna("")
            )

            fig = px.scatter(
                unified.dropna(subset=["ts_dt"]),
                x="ts_dt",
                y="value",
                color="trace_key",
                symbol="source",
                title="Vista unificada Lambda: Hot (InfluxDB) + Cold (MinIO Parquet)",
                labels={"ts_dt": "Tiempo", "value": "Temperatura media (°C)", "trace_key": "Fuente, device_id"},
                template="plotly_dark",
                color_discrete_map=_color_map,
                symbol_map={
                    "InfluxDB (hot)": "circle",
                    "MinIO (cold)":   "diamond",
                },
                opacity=0.7,
            )
            # Hot points más grandes para que no se pierdan entre los cold
            fig.for_each_trace(lambda t: t.update(marker_size=14) if "(hot)" in t.name else t.update(marker_size=6))
            fig.add_hline(y=ALERT_THRESHOLD, line_dash="dash", line_color="orange",
                          annotation_text=f"Umbral {ALERT_THRESHOLD}°C")
            fig.update_layout(height=400)
            st.plotly_chart(fig, width='stretch')

            # ── Estadísticas DuckDB en memoria ────────────────
            st.subheader("Estadísticas por fuente y máquina (DuckDB in-memory)")
            conn = duckdb.connect()
            conn.register("unified", unified)
            stats_df = conn.execute("""
                SELECT
                    source,
                    device_id,
                    COUNT(*)                    AS registros,
                    ROUND(AVG(value), 2)        AS media_c,
                    ROUND(MIN(value), 2)        AS min_c,
                    ROUND(MAX(value), 2)        AS max_c,
                    ROUND(STDDEV(value), 2)     AS desv_std,
                    SUM(CASE WHEN value > 80.0 THEN 1 ELSE 0 END) AS sobre_umbral
                FROM unified
                WHERE value IS NOT NULL
                GROUP BY source, device_id
                ORDER BY source, device_id
            """).df()
            conn.close()
            st.dataframe(stats_df, width='stretch', hide_index=True)

            # ── Breakdown por partición (cold path) ───────────
            if not cold_df.empty and "hour" in cold_df.columns:
                st.subheader("Breakdown Cold Path — registros por partición")
                part_df = cold_df.groupby(
                    ["year", "month", "day", "hour"]
                ).size().reset_index(name="registros")
                part_df["particion"] = (
                    part_df["year"].astype(str) + "-"
                    + part_df["month"].astype(str).str.zfill(2) + "-"
                    + part_df["day"].astype(str).str.zfill(2) + " "
                    + part_df["hour"].astype(str).str.zfill(2) + "h"
                )
                fig_part = px.bar(
                    part_df.sort_values(["year", "month", "day", "hour"]),
                    x="particion",
                    y="registros",
                    title="Registros Parquet por partición hora (MinIO cold path)",
                    template="plotly_dark",
                    color="registros",
                    color_continuous_scale="Blues",
                )
                fig_part.update_layout(height=300, xaxis_tickangle=-45)
                st.plotly_chart(fig_part, width='stretch')
