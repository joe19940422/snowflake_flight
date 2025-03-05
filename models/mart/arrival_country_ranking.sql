select 
ARRIVAL_COUNTRY_NAME, 
FLIGHT_DATE, 
count(*) as cn 
from flight.flight_dwh.dwh_flights 
group by ARRIVAL_COUNTRY_NAME, FLIGHT_DATE order by cn desc 