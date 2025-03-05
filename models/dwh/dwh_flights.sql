-- models/dwh/dwh_flights.sql

{{
    config(
        materialized='table',
        schema='dwh'
    )
}}

with dwh_flights as (

    select
        f.flight_date,
        f.flight_status,
        f.departure_airport,
        f.departure_delay,
        f.departure_icao,
        f.departure_iata,
        i.iso_country as departure_country,
        f.arrival_airport,
        f.arrival_delay,
        f.arrival_icao,
        f.arrival_iata,
        i1.iso_country as arrival_country,
        f.airline_name

    from {{ ref('ods_flights') }} as f
    left join {{ ref('icata') }} as i
    on f.departure_iata = i.iata_code

    left join {{ ref('icata') }} as i1
    on f.arrival_iata = i1.iata_code

) 
select d.*, 
c.country_name as departure_country_name,
c2.country_name as arrival_country_name
from dwh_flights d 
left join  {{ ref('country') }} as c
on d.departure_country = c.iso_country 

left join {{ ref('country') }} as c2
on d.arrival_country = c2.iso_country

