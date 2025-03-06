
{{ config(
    materialized='incremental',
    unique_key=['ARRIVAL_COUNTRY_NAME', 'FLIGHT_DATE', 'FLIGHT_NUMBER'],
    incremental_strategy='merge'
) }}

SELECT
    ARRIVAL_COUNTRY_NAME,
    FLIGHT_NUMBER,
    FLIGHT_DATE,
    COUNT(*) AS cn,
    CURRENT_TIMESTAMP() as last_updated
FROM
    {{ ref('dwh_flights') }}

{% if is_incremental() %}
WHERE last_updated > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}

GROUP BY
    ARRIVAL_COUNTRY_NAME,
    FLIGHT_DATE,
    FLIGHT_NUMBER
ORDER BY
    cn DESC