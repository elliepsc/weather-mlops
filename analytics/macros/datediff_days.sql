{% macro datediff_days(end_date, start_date) %}
    {{ adapter.dispatch('datediff_days', 'weather_mlops_analytics')(end_date, start_date) }}
{% endmacro %}

{% macro duckdb__datediff_days(end_date, start_date) %}
    CAST({{ end_date }} - {{ start_date }} AS INTEGER)
{% endmacro %}

{% macro bigquery__datediff_days(end_date, start_date) %}
    DATE_DIFF({{ end_date }}, {{ start_date }}, DAY)
{% endmacro %}

{% macro default__datediff_days(end_date, start_date) %}
    DATEDIFF('day', {{ start_date }}, {{ end_date }})
{% endmacro %}
