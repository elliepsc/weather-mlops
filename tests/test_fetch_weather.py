from __future__ import annotations

import pytest
import pandas as pd

from pipeline import fetch_weather as fw


def test_fetch_city_maps_extended_open_meteo_fields(monkeypatch):
    monkeypatch.setitem(
        fw.LOCATIONS,
        "TestCity",
        {"lat": -33.0, "lon": 151.0, "timezone": "Australia/Sydney", "state": "NSW"},
    )

    hourly_template = [float(i) for i in range(24)]

    def fake_call_api(url: str, params: dict, retries: int = 3):
        assert "rain_sum" in params["daily"]
        assert "precipitation_hours" in params["daily"]
        assert "shortwave_radiation_sum" in params["daily"]
        assert "dew_point_2m" in params["hourly"]
        assert "surface_pressure" in params["hourly"]
        assert "vapour_pressure_deficit" in params["hourly"]
        assert "windspeed_100m" in params["hourly"]
        return {
            "daily": {
                "time": ["2026-04-20"],
                "temperature_2m_min": [12.0],
                "temperature_2m_max": [24.0],
                "precipitation_sum": [5.0],
                "rain_sum": [4.0],
                "precipitation_hours": [3.0],
                "et0_fao_evapotranspiration": [4.5],
                "sunshine_duration": [28800],
                "shortwave_radiation_sum": [18.5],
                "windgusts_10m_max": [30.0],
                "winddirection_10m_dominant": [45.0],
                "weathercode": [61],
            },
            "hourly": {
                "temperature_2m": hourly_template,
                "relative_humidity_2m": hourly_template,
                "dew_point_2m": [value + 100 for value in hourly_template],
                "windspeed_10m": hourly_template,
                "windspeed_100m": [value + 5 for value in hourly_template],
                "winddirection_10m": [90.0] * 24,
                "vapour_pressure_deficit": [value * 0.1 for value in hourly_template],
                "pressure_msl": [1000.0 + value for value in hourly_template],
                "surface_pressure": [900.0 + value for value in hourly_template],
                "cloudcover": [50.0] * 24,
            },
        }

    monkeypatch.setattr(fw, "_call_api", fake_call_api)

    result = fw.fetch_city("TestCity", "2026-04-20", "2026-04-20")

    assert isinstance(result, pd.DataFrame)
    row = result.iloc[0]
    # existing fields
    assert row["rain_sum"] == 4.0
    assert row["precipitation_hours"] == 3.0
    assert row["dew_point_9am"] == 109.0
    assert row["dew_point_3pm"] == 115.0
    assert row["surface_pressure_9am"] == 909.0
    assert row["surface_pressure_3pm"] == 915.0
    # new fields
    assert row["shortwave_radiation_sum"] == 18.5
    assert row["vpd_9am"] == pytest.approx(0.9)
    assert row["vpd_3pm"] == pytest.approx(1.5)
    assert row["wind_speed_100m_9am"] == 14.0
    assert row["wind_speed_100m_3pm"] == 20.0
