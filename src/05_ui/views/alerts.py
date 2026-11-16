"""Pestaña ⚠️ Alertas: eventos con avg > umbral."""

import pandas as pd
import plotly.express as px
import streamlit as st

from settings import (
    ALERT_THRESHOLD,
)
from datasources import (
    query_alerts,
)


def render() -> None:
    alert_range = st.slider("Buscar alertas en los últimos (min)", 10, 1440, 120, key="alert_range")
    alerts_df = query_alerts(alert_range)

    if alerts_df.empty:
        st.success(f"✅ Sin alertas en los últimos {alert_range} minutos.")
    else:
        st.error(f"⚠️  {len(alerts_df)} evento(s) sobre {ALERT_THRESHOLD}°C")

        # ── Scatter original ──────────────────────────────────
        fig = px.scatter(
            alerts_df,
            x="ts",
            y="avg_temp_c",
            color="device_id",
            size="avg_temp_c",
            title="Eventos de alerta por máquina",
            template="plotly_dark",
        )
        fig.add_hline(y=ALERT_THRESHOLD, line_dash="dash", line_color="orange")
        st.plotly_chart(fig, width='stretch')

        # ── Timeline estilo Gantt ─────────────────────────────
        st.markdown("#### Timeline de alertas por máquina")
        gantt_df = alerts_df.copy()
        gantt_df["ts"] = pd.to_datetime(gantt_df["ts"], utc=True)
        gantt_df["end"] = gantt_df["ts"] + pd.Timedelta(minutes=1)
        fig_gantt = px.timeline(
            gantt_df.rename(columns={"ts": "Start", "end": "Finish", "device_id": "Task"}),
            x_start="Start", x_end="Finish", y="Task",
            color="avg_temp_c",
            color_continuous_scale="Reds",
            title="Intervalos de alerta (cada barra = 1 ventana de 1 min)",
            labels={"avg_temp_c": "°C"},
        )
        fig_gantt.update_layout(height=250, template="plotly_dark")
        st.plotly_chart(fig_gantt, width='stretch')

        # ── Alertas por hora del día ──────────────────────────
        st.markdown("#### ¿A qué hora fallan más las máquinas?")
        hour_df = alerts_df.copy()
        hour_df["hora"] = pd.to_datetime(hour_df["ts"], utc=True).dt.hour
        hour_counts = hour_df.groupby(["hora", "device_id"]).size().reset_index(name="alertas")
        fig_hour = px.bar(
            hour_counts,
            x="hora", y="alertas",
            color="device_id",
            barmode="stack",
            title="Distribución de alertas por hora del día (UTC)",
            labels={"hora": "Hora (UTC)", "alertas": "Nº alertas"},
            template="plotly_dark",
        )
        fig_hour.update_xaxes(tickmode="linear", dtick=1)
        fig_hour.update_layout(height=300)
        st.plotly_chart(fig_hour, width='stretch')

        st.download_button(
            "⬇️ Descargar CSV (alertas)",
            data=alerts_df.to_csv(index=False),
            file_name=f"alertas_{alert_range}min.csv",
            mime="text/csv",
        )
        st.dataframe(alerts_df, width='stretch', hide_index=True)
