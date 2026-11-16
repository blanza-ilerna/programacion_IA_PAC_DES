"""Pestaña 📡 Tiempo Real: estado actual de cada máquina (InfluxDB)."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from settings import (
    ALERT_THRESHOLD,
)
from datasources import (
    query_machine_status,
    query_history,
)


def render(range_min) -> None:
    st.subheader(f"Estado actual de máquinas (últimos {range_min} min)")
    status_df = query_machine_status(range_min)

    if status_df.empty:
        st.info(
            "Sin datos. ¿Está el pipeline corriendo?\n\n"
            "```bash\n"
            "python src/01_ingestion/sensor_simulator.py\n"
            "python src/01_ingestion/mqtt_to_redpanda_bridge.py\n"
            "# (Flink jobs en el contenedor jobmanager)\n"
            "```"
        )
    else:
        now_utc = pd.Timestamp.now(tz="UTC")
        cols = st.columns(len(status_df))
        for i, row in status_df.reset_index(drop=True).iterrows():
            temp = row.get("avg_temp_c", 0)
            is_alert = bool(row.get("alert", 0)) or temp > ALERT_THRESHOLD
            icon = "🔴" if is_alert else "🟢"
            delta_color = "inverse" if is_alert else "normal"
            secs_ago = int((now_utc - row["_time"]).total_seconds()) if "_time" in row and pd.notna(row["_time"]) else None
            label_time = f" — hace {secs_ago}s" if secs_ago is not None else ""
            with cols[i % len(cols)]:
                st.metric(
                    label=f"{icon} {row['device_id']}{label_time}",
                    value=f"{temp:.1f} °C",
                    delta="⚠ ALERTA" if is_alert else "Normal",
                    delta_color=delta_color,
                )

        # Gauges
        fig = go.Figure()
        rows_list = list(status_df.reset_index(drop=True).iterrows())
        for idx, (_, row) in enumerate(rows_list):
            temp = row.get("avg_temp_c", 0)
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=temp,
                title={"text": row["device_id"]},
                gauge={
                    "axis": {"range": [0, 120]},
                    "bar":  {"color": "red" if temp > ALERT_THRESHOLD else "green"},
                    "steps": [
                        {"range": [0,    ALERT_THRESHOLD], "color": "lightgray"},
                        {"range": [ALERT_THRESHOLD, 120],  "color": "lightyellow"},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": ALERT_THRESHOLD,
                    },
                },
                domain={"column": idx},
            ))
        fig.update_layout(
            grid={"rows": 1, "columns": len(rows_list)},
            height=280,
            template="plotly_dark",
        )
        st.plotly_chart(fig, width='stretch')

        # Sparklines minitrend por máquina
        st.markdown("#### Tendencia reciente por máquina")
        spark_df = query_history(range_min)
        if not spark_df.empty:
            spark_cols = st.columns(len(status_df))
            for i, row in status_df.reset_index(drop=True).iterrows():
                dev = row["device_id"]
                d = spark_df[spark_df["device_id"] == dev].sort_values("ts").tail(20)
                with spark_cols[i % len(spark_cols)]:
                    if not d.empty:
                        fig_sp = go.Figure(go.Scatter(
                            x=d["ts"], y=d["avg_temp_c"],
                            mode="lines", fill="tozeroy",
                            line=dict(color="red" if row.get("avg_temp_c", 0) > ALERT_THRESHOLD else "steelblue", width=1.5),
                            fillcolor="rgba(255,80,80,0.15)" if row.get("avg_temp_c", 0) > ALERT_THRESHOLD else "rgba(70,130,180,0.15)",
                        ))
                        fig_sp.update_layout(
                            height=80, margin=dict(l=0, r=0, t=0, b=0),
                            showlegend=False, template="plotly_dark",
                            xaxis=dict(visible=False), yaxis=dict(visible=False),
                        )
                        st.plotly_chart(fig_sp, width='stretch')
