"""
Fetch weather data from Open-Meteo API (free, no API key required).
Covers both historical backfill (2 years) and daily incremental updates.
"""
import time
import logging
from datetime import date, timedelta

import requests
import pandas as pd

from pipeline.locations import LOCATIONS

logger = logging.getLogger(__name__)

ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
    "sunshine_duration",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "winddirection_10m_dominant",
    "weathercode",
    "precipitation_probability_max",
]

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "windspeed_10m",
    "winddirection_10m",
    "pressure_msl",
    "cloudcover",
]

_COMPASS = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]


def _degrees_to_compass(deg) -> str | None:
    if pd.isna(deg):
        return None
    return _COMPASS[round(float(deg) / 22.5) % 16]


def _extract_hour(hourly: dict, var: str, target_hour: int, dates: list[str]) -> list:
    """Pull the value at a specific hour (e.g. 9 or 15) for each date."""
    values = hourly.get(var, [])
    # hourly data has 24 entries per day, in order
    result = []
    for i in range(len(dates)):
        idx = i * 24 + target_hour
        val = values[idx] if idx < len(values) else None
        result.append(val)
    return result


def _call_api(url: str, params: dict, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:
                wait = 30 * (2 ** attempt)  # 30s, 60s, 120s
                logger.warning("Rate limited (429). Waiting %ds before retry %d/%d…",
                               wait, attempt + 1, retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise RuntimeError(f"API call failed after {retries} attempts: {exc}") from exc
    raise RuntimeError(f"Rate limited: all {retries} retries exhausted (429)")


def fetch_city(city: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch daily + hourly data for one city between start_date and end_date (inclusive).
    Returns a DataFrame with one row per day, columns matching the weatherAUS schema.
    """
    loc = LOCATIONS[city]
    lat, lon, tz = loc["lat"], loc["lon"], loc["timezone"]

    # Choose archive vs forecast endpoint depending on recency
    today = date.today().isoformat()
    if end_date >= today:
        # forecast API handles last ~90 days + next 7 days
        url = FORECAST_URL
        params_extra = {"past_days": min(92, (date.today() - date.fromisoformat(start_date)).days + 1)}
    else:
        url = ARCHIVE_URL
        params_extra = {"start_date": start_date, "end_date": end_date}

    params = {
        "latitude":   lat,
        "longitude":  lon,
        "timezone":   tz,
        "daily":      ",".join(DAILY_VARS),
        "hourly":     ",".join(HOURLY_VARS),
        **params_extra,
    }

    data = _call_api(url, params)
    daily   = data.get("daily", {})
    hourly  = data.get("hourly", {})
    dates   = daily.get("time", [])

    if not dates:
        logger.warning("No data returned for %s (%s -> %s)", city, start_date, end_date)
        return pd.DataFrame()

    df = pd.DataFrame({
        "date":    dates,
        "city":    city,
        "state":   loc["state"],
        "latitude":  lat,
        "longitude": lon,
        "min_temp":        daily.get("temperature_2m_min"),
        "max_temp":        daily.get("temperature_2m_max"),
        "rainfall":        daily.get("precipitation_sum"),
        "evaporation":     daily.get("et0_fao_evapotranspiration"),
        "sunshine_hours":  [v / 3600 if v is not None else None
                            for v in (daily.get("sunshine_duration") or [None]*len(dates))],
        "wind_gust_speed": daily.get("windgusts_10m_max"),
        "wind_gust_dir":   [_degrees_to_compass(d)
                            for d in (daily.get("winddirection_10m_dominant") or [None]*len(dates))],
        "weather_code":    daily.get("weathercode"),
        # 9am values (hour index 9)
        "temp_9am":        _extract_hour(hourly, "temperature_2m",      9, dates),
        "humidity_9am":    _extract_hour(hourly, "relative_humidity_2m", 9, dates),
        "wind_speed_9am":  _extract_hour(hourly, "windspeed_10m",        9, dates),
        "wind_dir_9am":    [_degrees_to_compass(d)
                            for d in _extract_hour(hourly, "winddirection_10m", 9, dates)],
        "pressure_9am":    _extract_hour(hourly, "pressure_msl",         9, dates),
        "cloud_9am":       [v / 12.5 if v is not None else None
                            for v in _extract_hour(hourly, "cloudcover",  9, dates)],
        # 3pm values (hour index 15)
        "temp_3pm":        _extract_hour(hourly, "temperature_2m",      15, dates),
        "humidity_3pm":    _extract_hour(hourly, "relative_humidity_2m", 15, dates),
        "wind_speed_3pm":  _extract_hour(hourly, "windspeed_10m",        15, dates),
        "wind_dir_3pm":    [_degrees_to_compass(d)
                            for d in _extract_hour(hourly, "winddirection_10m", 15, dates)],
        "pressure_3pm":    _extract_hour(hourly, "pressure_msl",         15, dates),
        "cloud_3pm":       [v / 12.5 if v is not None else None
                            for v in _extract_hour(hourly, "cloudcover",  15, dates)],
    })

    # rain_today: precipitation_sum > 1 mm
    df["rain_today"] = (df["rainfall"].fillna(0) > 1.0).astype(int)

    # filter to requested date range (forecast API may return extra days)
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].reset_index(drop=True)
    return df


def fetch_all_cities(start_date: str, end_date: str,
                     delay_seconds: float = 0.5) -> pd.DataFrame:
    """
    Fetch all 26 cities for a given date range.
    delay_seconds: polite pause between API calls.
    """
    frames = []
    for city in LOCATIONS:
        logger.info("Fetching %s  (%s -> %s)", city, start_date, end_date)
        try:
            df = fetch_city(city, start_date, end_date)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            logger.error("Failed to fetch %s: %s", city, exc)
        time.sleep(delay_seconds)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def backfill_x_years(years: int = 5, start_date: str = "2021-01-01") -> pd.DataFrame:
    """
    Fetch historical data for all cities.
    Uses start_date if provided, otherwise goes back `years` years from today.
    Default: 2021-01-01 -> yesterday.
    delay_seconds=2.0 to stay within Open-Meteo free-tier rate limits.
    """
    end = (date.today() - timedelta(days=1)).isoformat()
    if start_date is None:
        start_date = (date.today() - timedelta(days=365 * years)).isoformat()
    logger.info("Starting backfill: %s -> %s (%d cities)", start_date, end, len(LOCATIONS))
    return fetch_all_cities(start_date, end, delay_seconds=2.0)


NASA_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
NASA_VARS = "T2M_MAX,T2M_MIN,PRECTOTCORR,RH2M,WS10M_MAX,WD10M,PS"


def fetch_city_nasapower(city: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch daily weather for one city via NASA POWER (free, no rate limits, no API key).
    9am/3pm sub-daily values use daily humidity/pressure as proxies.
    Unavailable columns (evaporation, cloud, weather_code, temp_9am/3pm) -> NULL.
    """
    loc = LOCATIONS[city]
    lat, lon = loc["lat"], loc["lon"]

    params = {
        "parameters": NASA_VARS,
        "community":  "RE",
        "longitude":  lon,
        "latitude":   lat,
        "start":      start_date.replace("-", ""),
        "end":        end_date.replace("-", ""),
        "format":     "JSON",
        "user":       "wxpipeline",
    }
    data = _call_api(NASA_URL, params)
    p = data["properties"]["parameter"]

    dates_raw = list(p["T2M_MAX"].keys())
    dates = [d for d in dates_raw
             if start_date.replace("-", "") <= d <= end_date.replace("-", "")]
    n = len(dates)

    if n == 0:
        logger.warning("No NASA POWER data for %s", city)
        return pd.DataFrame()

    def col(key):
        raw = p.get(key, {})
        return [None if (v is None or v <= -990) else v for v in (raw.get(d) for d in dates)]

    rh      = col("RH2M")
    ps_hpa  = [v * 10 if v is not None else None for v in col("PS")]
    ws_kmh  = [v * 3.6 if v is not None else None for v in col("WS10M_MAX")]
    rainfall = col("PRECTOTCORR")

    df = pd.DataFrame({
        "date":           [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in dates],
        "city":           city,
        "state":          loc["state"],
        "latitude":       lat,
        "longitude":      lon,
        "min_temp":       col("T2M_MIN"),
        "max_temp":       col("T2M_MAX"),
        "rainfall":       rainfall,
        "evaporation":    [None] * n,
        "sunshine_hours": [None] * n,
        "wind_gust_speed": ws_kmh,
        "wind_gust_dir":  [_degrees_to_compass(v) for v in col("WD10M")],
        "weather_code":   [None] * n,
        "temp_9am":       [None] * n,
        "humidity_9am":   rh,
        "wind_speed_9am": [None] * n,
        "wind_dir_9am":   [None] * n,
        "pressure_9am":   ps_hpa,
        "cloud_9am":      [None] * n,
        "temp_3pm":       [None] * n,
        "humidity_3pm":   rh,
        "wind_speed_3pm": [None] * n,
        "wind_dir_3pm":   [None] * n,
        "pressure_3pm":   ps_hpa,
        "cloud_3pm":      [None] * n,
    })

    df["rain_today"] = (df["rainfall"].fillna(0) > 1.0).astype(int)
    return df


def fetch_today() -> pd.DataFrame:
    """Fetch yesterday's confirmed data for all cities (daily incremental)."""
    from datetime import date, timedelta
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    return fetch_all_cities(yesterday, yesterday, delay_seconds=0.3)
