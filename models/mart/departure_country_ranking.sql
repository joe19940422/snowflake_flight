{{ config(materialized='table') }}

select 
departure_country_name, 
FLIGHT_DATE, 
count(*) as cn 
from {{ ref('dwh_flights') }}
group by departure_country_name, FLIGHT_DATE order by cn desc 