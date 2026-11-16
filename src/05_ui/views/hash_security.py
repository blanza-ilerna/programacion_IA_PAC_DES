"""Pestaña 🔐 Hash Chain: integridad SHA256 por dispositivo."""

import pandas as pd
import plotly.express as px
import streamlit as st
from datasources import (
    get_dlq_messages,
    get_verified_count,
)


def render() -> None:
    st.subheader("🔐 Integridad de Mensajes — Hash Chain SHA256")
    st.markdown("""
    Cada sensor firma sus mensajes con una cadena SHA256: cada hash incluye el hash anterior,
    formando una cadena inmutable. **Flink** verifica esta cadena en tiempo real.

    | Topic | Significado |
    |-------|-------------|
    | `sensors_verified` | Mensajes con cadena íntegra ✅ |
    | `sensors_invalid`  | Mensajes con hash roto o JSON inválido ❌ (DLQ) |
    """)

    if st.button("🔄 Actualizar", key="refresh_hash"):
        st.rerun()

    # ── Métricas de alto nivel ────────────────────────────────
    verified_total = get_verified_count()
    dlq_msgs = get_dlq_messages(100)

    m1, m2, m3 = st.columns(3)
    m1.metric("Mensajes verificados (offset)", verified_total if verified_total >= 0 else "—")
    m2.metric("Mensajes en DLQ (sensors_invalid)", len(dlq_msgs))
    if verified_total > 0 and dlq_msgs:
        integrity = verified_total / (verified_total + len(dlq_msgs)) * 100
        m3.metric("Integridad estimada", f"{integrity:.1f}%")
    elif verified_total > 0:
        m3.metric("Integridad", "100%")
    else:
        m3.metric("Integridad", "—")

    st.divider()

    # ── Mensajes DLQ ─────────────────────────────────────────
    st.markdown("### Últimos mensajes rechazados (DLQ)")
    if not dlq_msgs:
        st.success("✅ DLQ vacío — ningún mensaje ha sido rechazado.")
    else:
        dlq_rows = []
        for m in dlq_msgs:
            dlq_rows.append({
                "device_id": m.get("device_id", m.get("raw", "?")[:20]),
                "reason":    m.get("reason", "—"),
                "ts":        m.get("ts", "—"),
                "hash":      m.get("hash", "—")[:16] + "…" if m.get("hash") else "—",
            })
        st.dataframe(pd.DataFrame(dlq_rows), width='stretch', hide_index=True)

        # Gráfico de razones de rechazo
        if dlq_rows:
            reason_df = pd.DataFrame(dlq_rows)
            reason_counts = reason_df["reason"].value_counts().reset_index()
            reason_counts.columns = ["motivo", "count"]
            fig = px.bar(
                reason_counts,
                x="count",
                y="motivo",
                orientation="h",
                title="Motivos de rechazo en DLQ",
                template="plotly_dark",
                color="count",
                color_continuous_scale="Reds",
            )
            st.plotly_chart(fig, width='stretch')

        # Distribución por máquina
        device_counts = pd.DataFrame(dlq_rows)["device_id"].value_counts().reset_index()
        device_counts.columns = ["device_id", "rechazados"]
        fig2 = px.bar(
            device_counts,
            x="device_id",
            y="rechazados",
            title="Mensajes rechazados por máquina",
            template="plotly_dark",
            color="rechazados",
            color_continuous_scale="OrRd",
        )
        st.plotly_chart(fig2, width='stretch')

    st.divider()

    # ── Algoritmo explicado ───────────────────────────────────
    with st.expander("ℹ️ Cómo funciona el Hash Chain"):
        st.markdown("""
        ```
        Mensaje N:
          content = {device_id, temperature, unit, ts, _ingested_at}
          hash    = SHA256(json(content, sort_keys=True) + prev_hash)
          prev_hash = hash del mensaje N-1 (o "0"*64 para el primero)

        Flink verifica:
          1. ¿prev_hash declarado == último hash válido en estado Flink?
          2. ¿hash declarado == SHA256(content + prev_hash)?
          Si alguna falla → sensors_invalid con reason="hash_chain_broken"
        ```
        **Garantía:** Si alguien modifica un mensaje en tránsito, el hash cambia
        y Flink lo detecta. Los mensajes válidos van a `sensors_verified`.
        """)
