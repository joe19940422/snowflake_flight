{{ config(schema='ods') }}

SELECT 
    PARSE_JSON(data):flight_date::STRING AS flight_date,
    PARSE_JSON(data):flight_status::STRING AS flight_status,
    PARSE_JSON(data):departure:airport::STRING AS departure_airport,
    PARSE_JSON(data):departure:delay::STRING AS departure_delay,
    PARSE_JSON(data):departure:icao::STRING AS departure_icao,
    PARSE_JSON(data):departure:iata::STRING AS departure_iata,
    PARSE_JSON(data):arrival:airport::STRING AS arrival_airport,
    PARSE_JSON(data):arrival:delay::STRING AS arrival_delay,
    PARSE_JSON(data):arrival:icao::STRING AS arrival_icao,
    PARSE_JSON(data):arrival:iata::STRING AS arrival_iata,
    PARSE_JSON(data):airline:name::STRING AS airline_name,
    PARSE_JSON(data):flight:number::STRING AS flight_number,
    CURRENT_TIMESTAMP() as load_timestamp
    
FROM {{ source('flight_ods', 'flights_data_raw') }}   --flight_ods.flights_data_raw;
