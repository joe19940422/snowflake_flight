{{ config(materialized='table') }}

select
ARRIVAL_COUNTRY_NAME,
FLIGHT_DATE,
count(*) as cn
from {{ ref('dwh_flights') }}
group by ARRIVAL_COUNTRY_NAME, FLIGHT_DATE order by cn desc