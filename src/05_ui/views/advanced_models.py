"""Pestaña 🔬 Modelos Avanzados: Prophet, RandomForest, CUSUM, K-Means."""

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
    st.subheader("🔬 Modelos Avanzados de Machine Learning")
    st.markdown("""
    Modelos complementarios al IsolationForest para análisis más profundo:

    | Modelo | Tipo | Objetivo |
    |--------|------|----------|
    | **Prophet** | Forecasting temporal | Predice temperatura futura con intervalos de confianza |
    | **RandomForest** | Clasificación supervisada | Predice probabilidad de alerta con features de ingeniería |
    | **CUSUM** | Detección de cambio de régimen | Detecta degradación gradual antes de que dispare alertas |
    | **K-Means** | Clustering no supervisado | Agrupa máquinas por patrón de comportamiento |
    """)

    adv_range = st.slider("Datos históricos para análisis (min)", 30, 480, 120, key="adv_range")

    adv_df = query_history(adv_range)

    if adv_df.empty:
        st.warning("Sin datos en InfluxDB. ¿Está el pipeline corriendo?")
    else:
        adv_devices = sorted(adv_df["device_id"].unique().tolist())

        # ── 1. Prophet Forecasting ─────────────────────────────────────
        st.divider()
        st.markdown("### 📈 Prophet — Forecasting de Temperatura")
        st.markdown(
            "Modelo de series temporales de Meta/Facebook. Predice la temperatura "
            "de los próximos minutos con bandas de incertidumbre del 95%."
        )

        prop_col1, prop_col2 = st.columns([2, 1])
        with prop_col1:
            prop_device = st.selectbox("Máquina a predecir", adv_devices, key="prop_device")
        with prop_col2:
            prop_horizon = st.slider("Horizonte de predicción (min)", 10, 60, 30, key="prop_horizon")

        if st.button("📈 Entrenar Prophet y predecir", key="btn_prophet"):
            try:
                from prophet import Prophet  # noqa: PLC0415
                dev_df = adv_df[adv_df["device_id"] == prop_device].copy()
                dev_df = dev_df.sort_values("ts").rename(
                    columns={"ts": "ds", "avg_temp_c": "y"}
                )
                dev_df["ds"] = pd.to_datetime(dev_df["ds"]).dt.tz_localize(None)

                with st.spinner(f"Entrenando Prophet para {prop_device}..."):
                    m_prop = Prophet(
                        changepoint_prior_scale=0.05,
                        seasonality_mode="additive",
                        interval_width=0.95,
                        daily_seasonality=False,
                        weekly_seasonality=False,
                    )
                    m_prop.fit(dev_df[["ds", "y"]])
                    future = m_prop.make_future_dataframe(
                        periods=prop_horizon, freq="min"
                    )
                    forecast = m_prop.predict(future)

                last_ts = dev_df["ds"].max()
                future_fc = forecast[forecast["ds"] > last_ts]

                fig_prop = go.Figure()
                fig_prop.add_trace(go.Scatter(
                    x=dev_df["ds"], y=dev_df["y"],
                    mode="lines", name="Histórico",
                    line=dict(color="steelblue", width=2),
                ))
                fig_prop.add_trace(go.Scatter(
                    x=future_fc["ds"], y=future_fc["yhat"],
                    mode="lines", name=f"Predicción (+{prop_horizon} min)",
                    line=dict(color="orange", width=2, dash="dash"),
                ))
                # Confidence band
                fig_prop.add_trace(go.Scatter(
                    x=pd.concat([future_fc["ds"], future_fc["ds"].iloc[::-1]]),
                    y=list(future_fc["yhat_upper"]) + list(
                        future_fc["yhat_lower"].iloc[::-1]
                    ),
                    fill="toself",
                    fillcolor="rgba(255,165,0,0.15)",
                    line=dict(width=0),
                    name="Intervalo 95%",
                ))
                fig_prop.add_hline(
                    y=ALERT_THRESHOLD, line_dash="dash", line_color="red",
                    annotation_text=f"Umbral {ALERT_THRESHOLD}°C",
                )
                fig_prop.update_layout(
                    title=f"Prophet — Forecast temperatura {prop_device}",
                    xaxis_title="Tiempo",
                    yaxis_title="Temperatura (°C)",
                    height=420,
                    template="plotly_dark",
                )
                st.plotly_chart(fig_prop, width='stretch')

                future_alerts = future_fc[future_fc["yhat"] > ALERT_THRESHOLD]
                if not future_alerts.empty:
                    st.warning(
                        f"⚠️ Prophet predice **{len(future_alerts)}** ventanas de alerta "
                        f"en los próximos {prop_horizon} min"
                    )
                else:
                    st.success(
                        f"✅ Prophet no prevé alertas en los próximos {prop_horizon} min"
                    )

                if not future_fc.empty:
                    last_pred = future_fc.iloc[-1]
                    m1, m2, m3 = st.columns(3)
                    m1.metric(
                        f"Temperatura en +{prop_horizon} min",
                        f"{last_pred['yhat']:.1f}°C",
                    )
                    m2.metric(
                        "Límite inferior (95%)", f"{last_pred['yhat_lower']:.1f}°C"
                    )
                    m3.metric(
                        "Límite superior (95%)", f"{last_pred['yhat_upper']:.1f}°C"
                    )

            except ImportError:
                st.error(
                    "Prophet no está instalado. Ejecuta: `pip install prophet`\n\n"
                    "En el devcontainer: `pip install prophet` en el terminal."
                )
            except Exception as e:
                st.error(f"Error en Prophet: {e}")

        # ── 2. RandomForest Classifier ─────────────────────────────────
        st.divider()
        st.markdown("### 🌲 RandomForest — Clasificador de Alertas")
        st.markdown("""
        Modelo de clasificación supervisado (scikit-learn).
        **Features:** temperatura, hora del día, media/desv./max. de ventana deslizante.
        **Etiqueta:** `alert = 1` si temperatura > umbral de alerta.
        """)

        if st.button("🌲 Entrenar RandomForest", key="btn_rf"):
            from sklearn.ensemble import RandomForestClassifier  # noqa: PLC0415
            from sklearn.metrics import classification_report, confusion_matrix  # noqa: PLC0415
            from sklearn.model_selection import train_test_split  # noqa: PLC0415
            import numpy as np  # noqa: PLC0415

            with st.spinner("Preparando features y entrenando..."):
                rf_df = adv_df.copy()
                rf_df["ts"] = pd.to_datetime(rf_df["ts"], utc=True)
                rf_df["hour"] = rf_df["ts"].dt.hour
                rf_df["minute"] = rf_df["ts"].dt.minute
                rf_df["device_enc"] = (
                    rf_df["device_id"].astype("category").cat.codes
                )
                rf_df["alert"] = (
                    rf_df["avg_temp_c"] > ALERT_THRESHOLD
                ).astype(int)

                rf_df = rf_df.sort_values(["device_id", "ts"])
                for col, func in [
                    ("roll_mean", "mean"),
                    ("roll_std",  "std"),
                    ("roll_max",  "max"),
                ]:
                    rf_df[col] = rf_df.groupby("device_id")["avg_temp_c"].transform(
                        lambda x, f=func: getattr(
                            x.rolling(5, min_periods=1), f
                        )().fillna(0)
                    )

                features_rf = [
                    "avg_temp_c", "hour", "minute",
                    "device_enc", "roll_mean", "roll_std", "roll_max",
                ]
                rf_df = rf_df.dropna(subset=features_rf)
                X = rf_df[features_rf].values
                y = rf_df["alert"].values

                if len(np.unique(y)) < 2:
                    st.warning(
                        "No hay variedad suficiente en las etiquetas. "
                        "Necesitas datos con y sin alertas — amplía el rango de tiempo."
                    )
                else:
                    X_train, X_test, y_train, y_test = train_test_split(
                        X, y, test_size=0.2, random_state=42
                    )
                    clf = RandomForestClassifier(
                        n_estimators=100, random_state=42, n_jobs=-1
                    )
                    clf.fit(X_train, y_train)
                    y_pred = clf.predict(X_test)

                    report = classification_report(
                        y_test, y_pred, output_dict=True, zero_division=0
                    )
                    acc = (y_pred == y_test).mean()
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Accuracy", f"{acc:.1%}")
                    c2.metric(
                        "Precisión (alert=1)",
                        f"{report.get('1', {}).get('precision', 0):.1%}",
                    )
                    c3.metric(
                        "Recall (alert=1)",
                        f"{report.get('1', {}).get('recall', 0):.1%}",
                    )

                    # Feature importance
                    imp_df = pd.DataFrame({
                        "feature":    features_rf,
                        "importance": clf.feature_importances_,
                    }).sort_values("importance", ascending=True)

                    fig_imp = go.Figure(go.Bar(
                        x=imp_df["importance"],
                        y=imp_df["feature"],
                        orientation="h",
                        marker_color="steelblue",
                    ))
                    fig_imp.update_layout(
                        title="Importancia de features — RandomForest",
                        xaxis_title="Importancia relativa",
                        height=300,
                        template="plotly_dark",
                    )
                    st.plotly_chart(fig_imp, width='stretch')

                    # Confusion matrix
                    cm = confusion_matrix(y_test, y_pred)
                    fig_cm = px.imshow(
                        cm,
                        labels={"x": "Predicción", "y": "Real", "color": "Muestras"},
                        x=["Normal (0)", "Alerta (1)"],
                        y=["Normal (0)", "Alerta (1)"],
                        color_continuous_scale="Blues",
                        text_auto=True,
                        title="Matriz de confusión (test set)",
                    )
                    fig_cm.update_layout(height=300, template="plotly_dark")
                    st.plotly_chart(fig_cm, width='stretch')

                    # Probabilidad predicha en el tiempo para una máquina
                    rf_df["prob_alert"] = clf.predict_proba(X)[:, 1]
                    sel_rf = adv_devices[0]
                    dev_rf = rf_df[rf_df["device_id"] == sel_rf].sort_values("ts")
                    if not dev_rf.empty:
                        fig_prob = go.Figure()
                        fig_prob.add_trace(go.Scatter(
                            x=dev_rf["ts"], y=dev_rf["avg_temp_c"],
                            name="Temperatura (°C)", mode="lines",
                            line=dict(color="steelblue"),
                            yaxis="y1",
                        ))
                        fig_prob.add_trace(go.Scatter(
                            x=dev_rf["ts"], y=dev_rf["prob_alert"],
                            name="Prob. alerta (RF)", mode="lines",
                            line=dict(color="orange", dash="dash"),
                            yaxis="y2",
                        ))
                        fig_prob.update_layout(
                            title=f"Temperatura vs Probabilidad de alerta — {sel_rf}",
                            yaxis=dict(title="Temperatura (°C)", side="left"),
                            yaxis2=dict(
                                title="Prob. alerta",
                                side="right",
                                overlaying="y",
                                range=[0, 1],
                            ),
                            height=350,
                            template="plotly_dark",
                        )
                        st.plotly_chart(fig_prob, width='stretch')

        # ── 3. CUSUM Change Point Detection ────────────────────────────
        st.divider()
        st.markdown("### 📉 CUSUM — Detección de Cambios de Régimen")
        st.markdown("""
        CUSUM (Cumulative Sum) detecta **cambios graduales** en la media de temperatura.
        Es más sensible que un umbral fijo: detecta cuando el nivel base de una máquina
        cambia, aunque no supere el umbral de alerta.
        Uses `ruptures` (PELT) si está instalado, o CUSUM manual en caso contrario.
        """)

        cusum_c1, cusum_c2 = st.columns([2, 1])
        with cusum_c1:
            cusum_device = st.selectbox(
                "Máquina", adv_devices, key="cusum_device"
            )
        with cusum_c2:
            cusum_thresh = st.slider(
                "Sensibilidad",
                2.0, 10.0, 4.0, step=0.5, key="cusum_thresh",
                help="Umbral en desviaciones estándar. Menor = más sensible.",
            )

        if st.button("📉 Detectar cambios de régimen", key="btn_cusum"):
            dev_cusum = adv_df[
                adv_df["device_id"] == cusum_device
            ].sort_values("ts").copy()

            if len(dev_cusum) < 10:
                st.warning(
                    "Pocos datos para CUSUM. Amplía el rango de tiempo."
                )
            else:
                series_vals = dev_cusum["avg_temp_c"].values
                try:
                    import ruptures as rpt  # noqa: PLC0415
                    model_rpt = rpt.Pelt(model="rbf").fit(series_vals)
                    breakpoints = model_rpt.predict(pen=cusum_thresh * 10)[:-1]
                    method_used = "ruptures PELT (rbf)"
                except ImportError:
                    mean_s = series_vals.mean()
                    std_s = series_vals.std() or 1.0
                    norm = (series_vals - mean_s) / std_s
                    drift = 0.5
                    s_pos, s_neg = 0.0, 0.0
                    breakpoints = []
                    for i_c, x_c in enumerate(norm):
                        s_pos = max(0.0, s_pos + x_c - drift)
                        s_neg = max(0.0, s_neg - x_c - drift)
                        if s_pos > cusum_thresh or s_neg > cusum_thresh:
                            breakpoints.append(i_c)
                            s_pos, s_neg = 0.0, 0.0
                    method_used = "CUSUM manual (sin ruptures)"

                st.info(
                    f"Método: **{method_used}** — "
                    f"**{len(breakpoints)}** punto(s) de cambio detectado(s)"
                )

                fig_cusum = go.Figure()
                fig_cusum.add_trace(go.Scatter(
                    x=dev_cusum["ts"], y=dev_cusum["avg_temp_c"],
                    mode="lines", name="Temperatura",
                    line=dict(color="steelblue", width=1.5),
                ))
                for bp in breakpoints:
                    if bp < len(dev_cusum):
                        fig_cusum.add_vline(
                            x=dev_cusum["ts"].iloc[bp],
                            line_dash="dash", line_color="orange",
                            annotation_text="cambio",
                        )
                fig_cusum.add_hline(
                    y=ALERT_THRESHOLD, line_dash="dot", line_color="red",
                    annotation_text=f"Umbral {ALERT_THRESHOLD}°C",
                )
                fig_cusum.update_layout(
                    title=f"CUSUM — Cambios de régimen en {cusum_device}",
                    xaxis_title="Tiempo",
                    yaxis_title="Temperatura (°C)",
                    height=380,
                    template="plotly_dark",
                )
                st.plotly_chart(fig_cusum, width='stretch')

                if breakpoints:
                    st.markdown("#### Estadísticas por régimen")
                    indices = [0] + list(breakpoints) + [len(dev_cusum)]
                    regime_rows = []
                    for ri in range(len(indices) - 1):
                        seg = dev_cusum.iloc[indices[ri]:indices[ri + 1]]
                        if seg.empty:
                            continue
                        regime_rows.append({
                            "régimen": ri + 1,
                            "desde":   str(seg["ts"].iloc[0])[:19],
                            "hasta":   str(seg["ts"].iloc[-1])[:19],
                            "puntos":  len(seg),
                            "media_c": round(seg["avg_temp_c"].mean(), 2),
                            "std_c":   round(seg["avg_temp_c"].std(), 2),
                        })
                    st.dataframe(
                        pd.DataFrame(regime_rows),
                        width='stretch',
                        hide_index=True,
                    )

        # ── 4. K-Means Clustering ───────────────────────────────────────
        st.divider()
        st.markdown("### 🎯 K-Means — Clustering de Comportamiento de Máquinas")
        st.markdown("""
        Agrupa las máquinas según su **patrón de temperatura** usando K-Means (scikit-learn).
        **Features por máquina:** temperatura media, desviación estándar,
        temperatura máxima y tasa de alertas.
        """)

        km_col1, km_col2 = st.columns(2)
        with km_col1:
            n_clusters = st.slider(
                "Número de clusters",
                2, min(5, len(adv_devices)),
                min(3, len(adv_devices)),
                key="km_clusters",
            )

        if st.button("🎯 Calcular clusters K-Means", key="btn_kmeans"):
            from sklearn.cluster import KMeans  # noqa: PLC0415
            from sklearn.preprocessing import StandardScaler  # noqa: PLC0415

            feat_rows = []
            for dev in adv_devices:
                d = adv_df[adv_df["device_id"] == dev]["avg_temp_c"]
                feat_rows.append({
                    "device_id":  dev,
                    "mean_c":     d.mean(),
                    "std_c":      d.std(),
                    "max_c":      d.max(),
                    "min_c":      d.min(),
                    "alert_rate": (d > ALERT_THRESHOLD).mean(),
                    "p90_c":      d.quantile(0.90),
                })
            feat_df = pd.DataFrame(feat_rows)
            features_km = ["mean_c", "std_c", "max_c", "alert_rate"]
            X_km = feat_df[features_km].fillna(0).values

            if len(feat_df) < 2:
                st.warning(
                    "Necesitas al menos 2 máquinas con datos para hacer clustering."
                )
            else:
                scaler_km = StandardScaler()
                X_scaled = scaler_km.fit_transform(X_km)
                km_model = KMeans(
                    n_clusters=n_clusters, random_state=42, n_init=10
                )
                feat_df["cluster"] = km_model.fit_predict(X_scaled).astype(str)

                # Scatter: mean_c vs std_c
                fig_km1 = px.scatter(
                    feat_df,
                    x="mean_c", y="std_c",
                    color="cluster",
                    size="alert_rate",
                    hover_name="device_id",
                    text="device_id",
                    title="K-Means — Temperatura media vs Variabilidad",
                    labels={
                        "mean_c":     "Temperatura media (°C)",
                        "std_c":      "Desv. estándar (°C)",
                        "cluster":    "Cluster",
                        "alert_rate": "Tasa alertas",
                    },
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.qualitative.Set1,
                )
                fig_km1.add_vline(
                    x=ALERT_THRESHOLD, line_dash="dot", line_color="red",
                    annotation_text="Umbral alerta",
                )
                fig_km1.update_traces(textposition="top center")
                fig_km1.update_layout(height=420)
                st.plotly_chart(fig_km1, width='stretch')

                # Scatter: max_c vs alert_rate
                fig_km2 = px.scatter(
                    feat_df,
                    x="max_c", y="alert_rate",
                    color="cluster",
                    hover_name="device_id",
                    text="device_id",
                    title="K-Means — Temperatura máxima vs Tasa de alertas",
                    labels={
                        "max_c":      "Temperatura máxima (°C)",
                        "alert_rate": "Tasa de alertas",
                        "cluster":    "Cluster",
                    },
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.qualitative.Set1,
                )
                fig_km2.update_traces(textposition="top center")
                fig_km2.update_layout(height=350)
                st.plotly_chart(fig_km2, width='stretch')

                # Tabla de asignación
                st.markdown("#### Asignación de clusters")
                display_df = feat_df[
                    ["device_id", "cluster", "mean_c", "std_c",
                     "max_c", "alert_rate", "p90_c"]
                ].sort_values("cluster").copy()
                display_df["mean_c"]     = display_df["mean_c"].round(2)
                display_df["std_c"]      = display_df["std_c"].round(2)
                display_df["max_c"]      = display_df["max_c"].round(2)
                display_df["p90_c"]      = display_df["p90_c"].round(2)
                display_df["alert_rate"] = display_df["alert_rate"].apply(
                    lambda x: f"{x:.1%}"
                )
                st.dataframe(display_df, width='stretch', hide_index=True)

                # Centroides
                st.markdown("#### Centroides de clusters (escala original)")
                centroid_vals = scaler_km.inverse_transform(
                    km_model.cluster_centers_
                )
                centroid_df = pd.DataFrame(
                    centroid_vals, columns=features_km
                ).round(2)
                centroid_df.insert(0, "cluster", [str(i) for i in range(n_clusters)])
                st.dataframe(centroid_df, width='stretch', hide_index=True)
