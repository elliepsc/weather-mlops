"""
Streamlit demo — Weather Australia dashboard.
Connects to the FastAPI backend at http://localhost:8083.
"""
import streamlit as st
import requests
import pandas as pd

API_URL = "http://localhost:8083"

st.set_page_config(page_title="Weather Australia", page_icon="🌦", layout="wide")
st.title("Weather Australia — Live Dashboard")


@st.cache_data(ttl=300)
def fetch_cities():
    r = requests.get(f"{API_URL}/api/cities", timeout=10)
    r.raise_for_status()
    return [c["city"] for c in r.json()["cities"]]


@st.cache_data(ttl=300)
def fetch_latest(city=None):
    params = {"city": city} if city else {}
    r = requests.get(f"{API_URL}/api/weather/latest", params=params, timeout=10)
    r.raise_for_status()
    return pd.DataFrame(r.json()["data"])


@st.cache_data(ttl=300)
def fetch_weather(city=None, start_date=None, end_date=None, limit=5000):
    params = {"limit": limit}
    if city:
        params["city"] = city
    if start_date:
        params["start_date"] = str(start_date)
    if end_date:
        params["end_date"] = str(end_date)
    r = requests.get(f"{API_URL}/api/weather", params=params, timeout=30)
    r.raise_for_status()
    return pd.DataFrame(r.json()["data"])


@st.cache_data(ttl=600)
def fetch_mlflow_metrics():
    r = requests.get(f"{API_URL}/api/mlflow/metrics", timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


# ─── sidebar ─────────────────────────────────────────────────────────────────

try:
    cities = fetch_cities()
    health = requests.get(f"{API_URL}/health", timeout=5).json()
    api_ok = health.get("status") == "ok"
except Exception:
    cities = []
    api_ok = False

if not api_ok:
    st.error(f"API unreachable at {API_URL} — start the server with: python api/app.py")
    st.stop()

with st.sidebar:
    st.success("API connected", icon="✅")
    city_choice = st.selectbox("City", ["All"] + cities)
    selected_city = city_choice if city_choice != "All" else None

    st.markdown("---")
    st.markdown(f"[Prometheus](http://localhost:9090) · [Grafana](http://localhost:3000)")


# ─── latest predictions ──────────────────────────────────────────────────────

st.subheader("Latest predictions by city")
try:
    df_latest = fetch_latest(selected_city)
    if df_latest.empty:
        st.info("No data yet — run the pipeline first.")
    else:
        prediction_cols = [
            "city", "date",
            "rain_tomorrow", "rain_tomorrow_proba",
            "max_temp_tomorrow", "weather_type_tomorrow",
            "comfort_score",
            "heatwave_risk", "frost_risk", "storm_probability",
        ]
        visible = [c for c in prediction_cols if c in df_latest.columns]
        st.dataframe(df_latest[visible], use_container_width=True)
except Exception as exc:
    st.error(f"Error loading latest data: {exc}")


# ─── historical chart ────────────────────────────────────────────────────────

st.subheader("Historical temperature trend")
col1, col2 = st.columns(2)
with col1:
    start = st.date_input("Start date", value=pd.Timestamp("2024-01-01"))
with col2:
    end = st.date_input("End date", value=pd.Timestamp.today())

try:
    df_hist = fetch_weather(selected_city, start, end, limit=10000)
    if not df_hist.empty and "max_temp" in df_hist.columns:
        df_hist["date"] = pd.to_datetime(df_hist["date"])
        df_plot = df_hist.sort_values("date")
        if selected_city:
            st.line_chart(df_plot.set_index("date")[["max_temp", "min_temp"]])
        else:
            pivot = df_plot.pivot_table(
                index="date", columns="city", values="max_temp", aggfunc="mean"
            )
            st.line_chart(pivot)
    else:
        st.info("No historical data in selected range.")
except Exception as exc:
    st.error(f"Error loading historical data: {exc}")


# ─── model metrics ───────────────────────────────────────────────────────────

st.subheader("Model metrics (last training run)")
metrics = fetch_mlflow_metrics()
if metrics:
    cols = st.columns(len(metrics))
    for col, (model, m) in zip(cols, metrics.items()):
        with col:
            st.markdown(f"**{model}**")
            for k, v in m.items():
                if isinstance(v, float):
                    st.metric(k, f"{v:.3f}")
else:
    st.info("No model metrics yet — run: python pipeline/run_pipeline.py train")
