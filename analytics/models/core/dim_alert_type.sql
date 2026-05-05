-- Static alert-type dimension used by mart_weather_risk_alerts.

SELECT *
FROM (
    VALUES
        ('Rain risk',     'Rain',        'Risk based on rain_tomorrow_proba'),
        ('Storm risk',    'Storm',       'Risk based on storm_probability'),
        ('Heatwave risk', 'Temperature', 'Risk based on heatwave_risk'),
        ('Frost risk',    'Temperature', 'Risk based on frost_risk'),
        ('Low comfort',   'Comfort',     'Risk based on low comfort_score')
) AS t(alert_type, alert_category, alert_description)
