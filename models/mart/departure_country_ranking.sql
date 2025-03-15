{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['DEPARTURE_COUNTRY_NAME', 'FLIGHT_DATE', 'cn']
) }}

SELECT
    DEPARTURE_COUNTRY_NAME,
    FLIGHT_DATE,
    COUNT(*) AS cn,
    CURRENT_TIMESTAMP() AS last_updated
FROM
    {{ ref('dwh_flights') }}
GROUP BY
    DEPARTURE_COUNTRY_NAME,
    FLIGHT_DATE