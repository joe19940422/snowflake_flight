CREATE DATABASE flight;

CREATE SCHEMA ods;

CREATE OR REPLACE TABLE ods.flights_data_raw (
    source VARCHAR,
    data VARCHAR,
    load_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
);
ALTER SESSION SET TIMEZONE = 'Europe/Amsterdam';

select * from flight.ods.flights_data_raw

SELECT
    PARSE_JSON(data):flight_date::STRING AS flight_date,
    PARSE_JSON(data):flight_status::STRING AS flight_status,
    PARSE_JSON(data):departure:airport::STRING AS departure_airport,
    PARSE_JSON(data):departure:delay::STRING AS departure_delay,
    PARSE_JSON(data):departure:icao::STRING AS departure_icao,
    PARSE_JSON(data):arrival:airport::STRING AS arrival_airport,
    PARSE_JSON(data):arrival:delay::STRING AS arrival_delay,
    PARSE_JSON(data):arrival:icao::STRING AS arrival_icao,
    PARSE_JSON(data):airline:name::STRING AS airline_name,
    CURRENT_TIMESTAMP() as load_timestamp
FROM
    ods.flights_data_raw;