
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT
    ARRIVAL_COUNTRY_NAME,
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
ORDER BY
    cn DESC