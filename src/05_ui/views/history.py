"""Pestaña 📈 Historial: serie temporal de temperatura normalizada."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from settings import (
    ALERT_THRESHOLD,
)
from datasources import (
    query_history,
)


def render() -> None:
    h_col1, h_col2, h_col3 = st.columns([3, 2, 2])
    with h_col1:
        hist_range = st.slider("Ventana historial (min)", 5, 480, 60, key="hist_range")
    with h_col2:
        show_band = st.toggle("Banda ±1σ", value=True, key="hist_band")
    with h_col3:
        show_rolling = st.toggle("Media móvil (5 min)", value=False, key="hist_rolling")

    hist_df = query_history(hist_range)

    if hist_df.empty:
        st.info("Sin datos en el rango seleccionado.")
    else:
        all_devices = sorted(hist_df["device_id"].unique().tolist())

        # ── Selector máquina individual ────────────────────────
        sel_device = st.selectbox(
            "Máquina (todas o individual)",
            ["— Todas —"] + all_devices,
            key="hist_device",
        )
        plot_df = hist_df if sel_device == "— Todas —" else hist_df[hist_df["device_id"] == sel_device]

        fig = go.Figure()
        colors = px.colors.qualitative.Plotly
        for ci, dev in enumerate(plot_df["device_id"].unique()):
            d = plot_df[plot_df["device_id"] == dev].sort_values("ts")
            color = colors[ci % len(colors)]
            fig.add_trace(go.Scatter(
                x=d["ts"], y=d["avg_temp_c"],
                mode="lines", name=dev,
                line=dict(color=color, width=2),
            ))
            if show_band and len(d) > 2:
                sig = d["avg_temp_c"].std()
                fig.add_trace(go.Scatter(
                    x=pd.concat([d["ts"], d["ts"].iloc[::-1]]),
                    y=list(d["avg_temp_c"] + sig) + list((d["avg_temp_c"] - sig).iloc[::-1]),
                    fill="toself",
                    fillcolor=color.replace("rgb", "rgba").replace(")", ",0.12)") if color.startswith("rgb") else color,
                    line=dict(width=0),
                    showlegend=False,
                    name=f"{dev} ±1σ",
                    hoverinfo="skip",
                ))
            if show_rolling and len(d) >= 3:
                rolling = d["avg_temp_c"].rolling(window=5, min_periods=1).mean()
                fig.add_trace(go.Scatter(
                    x=d["ts"], y=rolling,
                    mode="lines", name=f"{dev} (MA5)",
                    line=dict(color=color, width=2, dash="dot"),
                    opacity=0.7,
                ))

        fig.add_hline(
            y=ALERT_THRESHOLD, line_dash="dash", line_color="red",
            annotation_text=f"Umbral {ALERT_THRESHOLD}°C",
        )
        fig.update_layout(
            title=f"Temperatura media por minuto — últimos {hist_range} min",
            xaxis_title="Tiempo", yaxis_title="Temperatura media (°C)",
            height=420, template="plotly_dark",
        )
        st.plotly_chart(fig, width='stretch')

        # ── Comparativa de dos máquinas ────────────────────────
        if len(all_devices) >= 2:
            st.markdown("#### Comparativa directa entre dos máquinas")
            cc1, cc2 = st.columns(2)
            with cc1:
                dev_a = st.selectbox("Máquina A", all_devices, index=0, key="cmp_a")
            with cc2:
                dev_b = st.selectbox("Máquina B", all_devices, index=min(1, len(all_devices)-1), key="cmp_b")

            if dev_a != dev_b:
                da = hist_df[hist_df["device_id"] == dev_a].sort_values("ts")
                db = hist_df[hist_df["device_id"] == dev_b].sort_values("ts")
                fig_cmp = go.Figure()
                fig_cmp.add_trace(go.Scatter(x=da["ts"], y=da["avg_temp_c"], name=dev_a, mode="lines"))
                fig_cmp.add_trace(go.Scatter(x=db["ts"], y=db["avg_temp_c"], name=dev_b, mode="lines"))
                fig_cmp.add_hline(y=ALERT_THRESHOLD, line_dash="dash", line_color="red")
                fig_cmp.update_layout(
                    title=f"Comparativa {dev_a} vs {dev_b}",
                    height=300, template="plotly_dark",
                    xaxis_title="Tiempo", yaxis_title="°C",
                )
                st.plotly_chart(fig_cmp, width='stretch')

        st.download_button(
            "⬇️ Descargar CSV (historial completo)",
            data=plot_df.to_csv(index=False),
            file_name=f"historial_{hist_range}min.csv",
            mime="text/csv",
        )
        st.dataframe(
            plot_df.tail(50).sort_values("ts", ascending=False),
            width='stretch',
            hide_index=True,
        )
