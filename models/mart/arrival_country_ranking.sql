{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ARRIVAL_COUNTRY_NAME', 'FLIGHT_DATE', 'cn']
) }}

SELECT
    ARRIVAL_COUNTRY_NAME,
    FLIGHT_DATE,
    COUNT(*) AS cn,
    CURRENT_TIMESTAMP() AS last_updated
FROM
    {{ ref('dwh_flights') }}
GROUP BY
    ARRIVAL_COUNTRY_NAME,
    FLIGHT_DATE
