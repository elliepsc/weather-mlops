"""
Streamlit — Weather Australia Dashboard
4 onglets : Prévisions | Backtesting | Performance | Pipeline

Déploiement :
  Local   → python api/app.py  (port 8001)
  Docker  → port 8000
  Render  → définir API_URL dans les secrets Streamlit Community Cloud
"""

import os
from datetime import timedelta
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ─── Config ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Weather Australia",
    page_icon="🌦",
    layout="wide",
)

API_URL: str = (
    st.secrets.get("API_URL", None)
    or os.getenv("API_URL", "http://localhost:8001")
).rstrip("/")


# ─── Data fetchers ────────────────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def fetch_health() -> dict:
    return requests.get(f"{API_URL}/health", timeout=5).json()


@st.cache_data(ttl=3600)
def fetch_cities() -> list[str]:
    r = requests.get(f"{API_URL}/api/cities", timeout=10)
    r.raise_for_status()
    return [c["city"] for c in r.json()["cities"]]


@st.cache_data(ttl=3600)
def fetch_latest(city: Optional[str] = None) -> pd.DataFrame:
    params = {"city": city} if city else {}
    r = requests.get(f"{API_URL}/api/weather/latest", params=params, timeout=10)
    r.raise_for_status()
    return pd.DataFrame(r.json()["data"])


@st.cache_data(ttl=3600)
def fetch_weather(
    city: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 10_000,
) -> pd.DataFrame:
    params: dict = {"limit": limit}
    if city:
        params["city"] = city
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    r = requests.get(f"{API_URL}/api/weather", params=params, timeout=30)
    r.raise_for_status()
    return pd.DataFrame(r.json()["data"])


@st.cache_data(ttl=3600)
def fetch_mlflow_metrics() -> Optional[dict]:
    r = requests.get(f"{API_URL}/api/mlflow/metrics", timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=3600)
def fetch_analytics(mart: str) -> Optional[pd.DataFrame]:
    r = requests.get(f"{API_URL}/api/analytics/{mart}", timeout=15)
    if r.status_code in (404, 503):
        return None
    r.raise_for_status()
    return pd.DataFrame(r.json()["data"])


# ─── Bootstrap ────────────────────────────────────────────────────────────────

try:
    health = fetch_health()
    api_ok = health.get("status") == "ok"
    demo_mode = bool(health.get("demo_mode", False))
except Exception:
    api_ok = False
    demo_mode = False
    health = {}

# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🌦 Weather Australia")

    if api_ok:
        st.success("API connectée", icon="✅")
        if demo_mode:
            st.info("Mode démo actif", icon="🎭")
    else:
        st.error("API non disponible", icon="🔴")
        st.code(f"API_URL = {API_URL}")

    st.markdown("---")

    cities: list[str] = []
    if api_ok:
        try:
            cities = fetch_cities()
        except Exception:
            cities = []

    city_choice = st.selectbox("Ville", ["Toutes les villes"] + cities)
    selected_city: Optional[str] = city_choice if city_choice != "Toutes les villes" else None

    st.markdown("---")
    if not demo_mode and api_ok:
        st.markdown("[📈 Prometheus](http://localhost:9090) · [📊 Grafana](http://localhost:3000)")

if not api_ok:
    st.title("🌦 Weather Australia")
    st.error(
        f"API inaccessible à `{API_URL}`\n\n"
        "**Local** — lancez : `python api/app.py` (port 8001)  \n"
        "**Render** — définissez `API_URL` dans les secrets Streamlit."
    )
    st.stop()

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "🌤 Prévisions actuelles",
    "📊 Prédictions vs Réalisé",
    "🎯 Performance modèle",
    "⚙️ Pipeline & Architecture",
])

# ══════════════════════════════════════════════════════════════════════════════
# Tab 1 — Prévisions actuelles
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    title_city = f" — {selected_city}" if selected_city else ""
    st.subheader(f"Prévisions actuelles{title_city}")

    try:
        df_latest = fetch_latest(selected_city)

        if df_latest.empty:
            st.info("Aucune donnée disponible — exécutez le pipeline.")
        else:
            row = df_latest.iloc[0]

            # ── Métriques clés ───────────────────────────────────────────────
            c1, c2, c3, c4, c5 = st.columns(5)
            max_t = row.get("max_temp_tomorrow")
            rain_p = row.get("rain_tomorrow_proba")
            hw = row.get("heatwave_risk")
            fr = row.get("frost_risk")

            with c1:
                st.metric("🌡 Temp max J+1",
                          f"{max_t:.1f}°C" if pd.notna(max_t) else "—")
            with c2:
                st.metric("🌧 Prob. pluie",
                          f"{rain_p:.0%}" if pd.notna(rain_p) else "—")
            with c3:
                st.metric("🌈 Type météo",
                          str(row.get("weather_type_tomorrow") or "—"))
            with c4:
                st.metric("🔥 Canicule",
                          f"{hw:.0%}" if pd.notna(hw) else "—")
            with c5:
                st.metric("❄️ Gel",
                          f"{fr:.0%}" if pd.notna(fr) else "—")

            predicted_at = row.get("predicted_at")
            if predicted_at:
                st.caption(f"Dernière mise à jour : {predicted_at}")

            st.markdown("---")

            # ── Graphique 7 jours ────────────────────────────────────────────
            try:
                # Use max date from latest data so demo mode works correctly
                max_date = pd.to_datetime(df_latest["date"].max())
                start_7d = (max_date - timedelta(days=7)).strftime("%Y-%m-%d")
                end_7d = max_date.strftime("%Y-%m-%d")

                df_week = fetch_weather(selected_city, start_7d, end_7d, limit=500)

                if not df_week.empty and "max_temp_tomorrow" in df_week.columns:
                    df_week["date"] = pd.to_datetime(df_week["date"])
                    df_week = df_week.sort_values("date")

                    if selected_city:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=df_week["date"],
                            y=df_week["max_temp_tomorrow"],
                            name="Temp max prévue J+1",
                            mode="lines+markers",
                            line=dict(color="#FF6B35", width=2),
                            marker=dict(size=6),
                        ))
                        if "max_temp" in df_week.columns:
                            fig.add_trace(go.Scatter(
                                x=df_week["date"],
                                y=df_week["max_temp"],
                                name="Temp max réelle (J)",
                                mode="lines+markers",
                                line=dict(color="#004E89", width=2, dash="dot"),
                                marker=dict(size=6),
                            ))
                        fig.update_layout(
                            title=f"Température 7 jours — {selected_city}",
                            xaxis_title="Date",
                            yaxis_title="°C",
                            legend=dict(orientation="h", yanchor="bottom", y=1.02),
                            height=340,
                            margin=dict(t=50),
                        )
                    else:
                        pivot = df_week.pivot_table(
                            index="date", columns="city",
                            values="max_temp_tomorrow", aggfunc="mean",
                        )
                        fig = px.line(
                            pivot,
                            title="Temp max prévue J+1 — Toutes villes",
                            labels={"value": "°C", "date": "Date"},
                        )
                        fig.update_layout(height=340)

                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.warning(f"Graphique indisponible : {e}")

            # ── Tableau détail ───────────────────────────────────────────────
            st.subheader("Détail" + (" par ville" if not selected_city else ""))
            show_cols = [c for c in [
                "city", "date", "max_temp_tomorrow", "rain_tomorrow_proba",
                "weather_type_tomorrow", "comfort_score",
                "heatwave_risk", "frost_risk", "storm_probability",
            ] if c in df_latest.columns]
            st.dataframe(df_latest[show_cols], use_container_width=True, hide_index=True)

    except Exception as exc:
        st.error(f"Erreur chargement prévisions : {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 2 — Prédictions vs Réalisé (Backtesting)
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.subheader("Prédictions vs Réalisé")

    col_sel_city, col_sel_period = st.columns([2, 1])
    with col_sel_city:
        bt_cities = cities or []
        city_bt: Optional[str] = (
            st.selectbox("Ville", bt_cities, key="bt_city")
            if bt_cities else None
        )
    with col_sel_period:
        period_label = st.selectbox("Période", ["7 jours", "30 jours", "90 jours"],
                                    index=1, key="bt_period")

    n_days = {"7 jours": 7, "30 jours": 30, "90 jours": 90}[period_label]

    try:
        # ── Prefer the pre-aligned analytics mart ────────────────────────────
        df_bt = fetch_analytics("forecast-timeline")
        using_mart = df_bt is not None and not df_bt.empty

        if using_mart:
            if city_bt:
                df_bt = df_bt[df_bt["city"] == city_bt].copy()

            # Filter to requested period relative to latest available date
            max_pred = pd.to_datetime(df_bt["prediction_date"]).max()
            cutoff = (max_pred - timedelta(days=n_days)).strftime("%Y-%m-%d")
            df_bt = df_bt[df_bt["prediction_date"] >= cutoff].copy()
            df_bt["date"] = pd.to_datetime(df_bt["prediction_date"])
            df_bt = df_bt.sort_values("date")
            has_actuals = bool(df_bt.get("has_actuals", pd.Series(dtype=bool)).any())

            if df_bt.empty:
                st.info("Aucune donnée pour cette période / ville.")
            else:
                # ── KPIs ─────────────────────────────────────────────────────
                if has_actuals:
                    valid = df_bt[df_bt["has_actuals"]]
                    mae = valid["temp_abs_error"].mean()
                    acc = valid["rain_correct"].mean()
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        st.metric("MAE température", f"{mae:.2f}°C")
                    with m2:
                        st.metric("Accuracy pluie", f"{acc:.1%}")
                    with m3:
                        st.metric("Jours évalués", int(valid["prediction_date"].nunique()))

                # ── Courbe temp prévue vs réelle ─────────────────────────────
                fig = go.Figure()
                if "pred_max_temp_tomorrow" in df_bt.columns:
                    fig.add_trace(go.Scatter(
                        x=df_bt["date"], y=df_bt["pred_max_temp_tomorrow"],
                        name="Temp prévue J+1", mode="lines+markers",
                        line=dict(color="#FF6B35", width=2),
                    ))
                if has_actuals and "actual_max_temp" in df_bt.columns:
                    fig.add_trace(go.Scatter(
                        x=df_bt["date"], y=df_bt["actual_max_temp"],
                        name="Temp réelle J+1", mode="lines+markers",
                        line=dict(color="#004E89", width=2, dash="dot"),
                    ))
                fig.update_layout(
                    title=f"Température prévue vs réelle — {city_bt or 'Toutes villes'} ({period_label})",
                    xaxis_title="Date", yaxis_title="°C",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    height=370,
                )
                st.plotly_chart(fig, use_container_width=True)

                # ── Distribution des erreurs ──────────────────────────────────
                if has_actuals and "temp_abs_error" in df_bt.columns:
                    valid2 = df_bt[df_bt["has_actuals"]].dropna(subset=["temp_abs_error"])
                    if not valid2.empty:
                        fig_err = px.histogram(
                            valid2, x="temp_abs_error", nbins=20,
                            title="Distribution des erreurs de température",
                            labels={"temp_abs_error": "Erreur absolue (°C)"},
                            color_discrete_sequence=["#FF6B35"],
                        )
                        fig_err.update_layout(height=260, margin=dict(t=40))
                        st.plotly_chart(fig_err, use_container_width=True)

        else:
            # ── Fallback: compute alignment from raw weather data ─────────────
            df_raw = fetch_weather(city_bt, limit=5000)
            if df_raw.empty:
                st.info("Aucune donnée disponible.")
            else:
                df_raw["date"] = pd.to_datetime(df_raw["date"])
                max_d = df_raw["date"].max()
                cutoff_dt = max_d - timedelta(days=n_days)
                df_raw = df_raw[df_raw["date"] >= cutoff_dt].sort_values(["city", "date"])

                # rain_tomorrow on day D predicts rain_today on day D+1
                df_raw["pred_temp"] = df_raw.groupby("city")["max_temp_tomorrow"].shift(1)
                df_raw["temp_error"] = (df_raw["pred_temp"] - df_raw["max_temp"]).abs()
                df_raw["pred_rain"] = df_raw.groupby("city")["rain_tomorrow"].shift(1)
                df_raw["rain_correct"] = (df_raw["pred_rain"] == df_raw["rain_today"]).astype(float)

                valid = df_raw.dropna(subset=["pred_temp"])
                if not valid.empty:
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        st.metric("MAE température", f"{valid['temp_error'].mean():.2f}°C")
                    with m2:
                        st.metric("Accuracy pluie", f"{valid['rain_correct'].mean():.1%}")
                    with m3:
                        st.metric("Jours évalués", int(valid["date"].nunique()))

                if city_bt:
                    df_c = df_raw[df_raw["city"] == city_bt]
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df_c["date"], y=df_c["pred_temp"],
                        name="Prévue", mode="lines+markers",
                        line=dict(color="#FF6B35", width=2)))
                    fig.add_trace(go.Scatter(x=df_c["date"], y=df_c["max_temp"],
                        name="Réelle", mode="lines+markers",
                        line=dict(color="#004E89", width=2, dash="dot")))
                    fig.update_layout(
                        title=f"Température — {city_bt} ({period_label})",
                        xaxis_title="Date", yaxis_title="°C", height=370)
                    st.plotly_chart(fig, use_container_width=True)

    except Exception as exc:
        st.error(f"Erreur backtesting : {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 3 — Performance modèle
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.subheader("Performance du modèle")

    # ── Métriques globales ────────────────────────────────────────────────────
    try:
        metrics = fetch_mlflow_metrics()
        source = metrics.get("source") if metrics else None

        if metrics and source != "unavailable":
            st.markdown("#### Métriques globales (dernier entraînement)")

            _source_label = {"json_cache": "📦 cache JSON", "mlflow_live": "🔗 MLflow live"}
            if source:
                run_name = metrics.get("run_name") or ""
                run_date = (metrics.get("start_time") or "")[:10]
                duration = metrics.get("duration_seconds")
                caption_parts = [_source_label.get(source, source)]
                if run_name:
                    caption_parts.append(run_name)
                if run_date:
                    caption_parts.append(run_date)
                if duration:
                    caption_parts.append(f"{duration}s")
                st.caption("  ·  ".join(caption_parts))

            label_map = {
                "rain_tomorrow":         "🌧 Pluie",
                "max_temp_tomorrow":     "🌡 Temp max",
                "weather_type_tomorrow": "🌈 Type météo",
                "heatwave_risk":         "🔥 Canicule",
                "frost_risk":            "❄️ Gel",
                "storm_probability":     "⛈ Orage",
            }

            # Prefer nested model_metrics (new format) — fall back to flat dict iteration
            model_m: dict = metrics.get("model_metrics") or {
                k: v for k, v in metrics.items() if isinstance(v, dict)
            }
            if model_m:
                cols_m = st.columns(len(model_m))
                for col, (model_key, m_vals) in zip(cols_m, model_m.items()):
                    with col:
                        st.markdown(f"**{label_map.get(model_key, model_key)}**")
                        for k, v in m_vals.items():
                            if isinstance(v, float):
                                st.metric(k.upper(), f"{v:.4f}")
            elif metrics.get("metrics"):
                flat = metrics["metrics"]
                flat_labels = {
                    "rain_accuracy": "🌧 Accuracy pluie",
                    "temp_mae": "🌡 MAE temp (°C)",
                    "temp_rmse": "🌡 RMSE temp",
                    "rain_f1": "🌧 F1 pluie",
                    "rain_precision": "🎯 Précision",
                    "rain_recall": "📈 Rappel",
                }
                delta = metrics.get("delta_vs_baseline") or {}
                cols_f = st.columns(min(len(flat), 6))
                for col, (k, v) in zip(cols_f, flat.items()):
                    with col:
                        d = delta.get(k)
                        st.metric(
                            flat_labels.get(k, k),
                            f"{v:.4f}" if isinstance(v, float) else str(v),
                            delta=f"{d:+.4f}" if isinstance(d, float) else None,
                        )
        else:
            msg = (metrics or {}).get(
                "message", "Métriques non disponibles — exécutez le pipeline d'entraînement."
            )
            st.info(msg)
    except Exception as exc:
        st.warning(f"Métriques inaccessibles : {exc}")

    st.markdown("---")

    # ── Évolution mensuelle ───────────────────────────────────────────────────
    try:
        df_overview = fetch_analytics("performance-overview")
        if df_overview is not None and not df_overview.empty:
            st.markdown("#### Évolution mensuelle")
            df_overview["month"] = pd.to_datetime(df_overview["month"])
            df_overview = df_overview.sort_values("month")

            fig_perf = go.Figure()
            fig_perf.add_trace(go.Scatter(
                x=df_overview["month"],
                y=df_overview["rain_accuracy"],
                name="Accuracy pluie",
                mode="lines+markers+text",
                yaxis="y1",
                line=dict(color="#004E89", width=2),
                text=df_overview["rain_accuracy"].map(lambda v: f"{v:.2f}"),
                textposition="top center",
            ))
            fig_perf.add_trace(go.Bar(
                x=df_overview["month"],
                y=df_overview["temp_mae"],
                name="MAE Temp (°C)",
                yaxis="y2",
                opacity=0.45,
                marker_color="#FF6B35",
            ))
            fig_perf.update_layout(
                title="Accuracy pluie & MAE température par mois",
                xaxis_title="Mois",
                yaxis=dict(title="Accuracy pluie", range=[0, 1.05], tickformat=".0%"),
                yaxis2=dict(title="MAE Temp (°C)", overlaying="y", side="right",
                            range=[0, 5]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                height=360,
                margin=dict(t=55),
            )
            st.plotly_chart(fig_perf, use_container_width=True)
    except Exception as exc:
        st.warning(f"Évolution mensuelle indisponible : {exc}")

    # ── Performance par ville (dernier mois) ─────────────────────────────────
    try:
        df_by_city = fetch_analytics("performance-by-city")
        if df_by_city is not None and not df_by_city.empty:
            st.markdown("#### Performance par ville — dernier mois")
            latest_month = df_by_city["month"].max()
            df_last = df_by_city[df_by_city["month"] == latest_month].copy()

            fig_city = px.bar(
                df_last.sort_values("rain_accuracy", ascending=True),
                x="rain_accuracy", y="city", orientation="h",
                color="temp_mae",
                color_continuous_scale="RdYlGn_r",
                title=f"Accuracy pluie par ville — {latest_month}",
                labels={"rain_accuracy": "Accuracy pluie", "temp_mae": "MAE Temp (°C)"},
                text=df_last.sort_values("rain_accuracy", ascending=True)["rain_accuracy"]
                    .map(lambda v: f"{v:.2f}"),
            )
            fig_city.update_traces(textposition="outside")
            fig_city.update_layout(height=300, margin=dict(t=45))
            st.plotly_chart(fig_city, use_container_width=True)
    except Exception as exc:
        st.warning(f"Performance par ville indisponible : {exc}")

    # ── Drift / monitoring status ─────────────────────────────────────────────
    try:
        df_health_m = fetch_analytics("health")
        if df_health_m is not None and not df_health_m.empty:
            latest_h = df_health_m.iloc[0]

            if latest_h.get("heavy_drift"):
                st.warning("⚠️ **Dérive détectée** — `heavy_drift = True` sur la dernière fenêtre.")

            action = str(latest_h.get("monitoring_action", ""))
            badge = {"trigger_retrain": "🔴", "alert_only": "🟡",
                     "alert_insufficient_data": "🟠", "no_action": "🟢"}.get(action, "⚪")
            reason = latest_h.get("monitoring_reason", "")
            st.markdown(f"**Dernière décision monitoring** : {badge} `{action}`"
                        + (f" — {reason}" if reason else ""))
    except Exception as exc:
        st.warning(f"Statut monitoring indisponible : {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 4 — Pipeline & Architecture
# ══════════════════════════════════════════════════════════════════════════════

with tab4:
    col_status, col_arch = st.columns([1, 2])

    with col_status:
        st.markdown("#### Statut du pipeline")
        if health:
            st.json({
                "status":    health.get("status"),
                "demo_mode": health.get("demo_mode"),
                "db":        health.get("db"),
                "db_exists": health.get("db_exists"),
            })

        st.markdown("#### Technologies")
        st.markdown("""
| Couche | Stack |
|---|---|
| Ingestion | Open-Meteo API |
| Stockage brut | SQLite (`weather_raw`) |
| Analytics | DuckDB + dbt (5 marts) |
| ML | XGBoost, scikit-learn |
| Tracking | MLflow |
| Orchestration | Apache Airflow |
| API | FastAPI + Prometheus |
| Dashboard | Streamlit + Plotly |
| Déploiement | Docker / Render / Streamlit Cloud |
        """)

        st.markdown("#### Liens")
        st.markdown("📂 [GitHub — weather-mlops](https://github.com/elliepsc/weather-mlops)")
        if not demo_mode:
            st.markdown(
                "📈 [Grafana](http://localhost:3000) · "
                "📊 [Prometheus](http://localhost:9090) · "
                "🔬 [MLflow](http://localhost:5000)"
            )

    with col_arch:
        st.markdown("#### Architecture")
        st.code("""
  Open-Meteo API
       │
       ▼
  ingestion_dag ──────────► data/weather.db (SQLite)
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
       gap_monitor_dag      process_weather.py      dbt (DuckDB)
                                    │               5 analytics
                                    ▼                  marts
                           XGBoost models
                                    │
                              MLflow tracking
                                    │
                                    ▼
                         weather_predictions
                                    │
                        ┌───────────┴───────────┐
                        ▼                       ▼
                   FastAPI (api/)          DuckDB marts
                   /api/weather            /api/analytics
                   /api/mlflow             /api/analytics/
                        │                       │
                        └───────────┬───────────┘
                                    ▼
                           Streamlit (cette UI)
                        ┌───────────────────────┐
                        │ 🌤 Prévisions          │
                        │ 📊 Backtesting         │
                        │ 🎯 Performance         │
                        │ ⚙️  Pipeline            │
                        └───────────────────────┘
""", language="text")

        if demo_mode:
            st.info(
                "**Mode démo actif** — données synthétiques (90 jours × 5 villes).  \n"
                "Le pipeline complet tourne en local via Apache Airflow."
            )
        else:
            st.markdown("#### Ports")
            st.markdown("""
| Service | Local | Docker |
|---|---|---|
| FastAPI | `:8001` | `:8000` |
| Airflow UI | `:8083` | `:8083` |
| Grafana | `:3000` | `:3000` |
| Prometheus | `:9090` | `:9090` |
| MLflow | `:5000` | — |
            """)
