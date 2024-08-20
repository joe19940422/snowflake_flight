import requests
import snowflake.connector
import json
from datetime import datetime

#Step 1: Fetch the JSON data from the API
api_url = 'https://api.aviationstack.com/v1/flights'
params = {
    'access_key': '6d9483427721b5e480d5e20e17ab05dc',
    'flight_status': 'active',
    'limit': 100
}

response = requests.get(api_url, params=params)


if response.status_code == 200:
    flights_data = response.json().get('data', [])
  # Extract the 'data' part of the JSON
    print(flights_data)
else:
    raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Step 2: Connect to Snowflake
conn = snowflake.connector.connect(
    user='PENGFEIQIAO',
    password='19940422QPf@',
    account='fh67946.eu-west-3.aws',
    database='flight',
    schema='ods'
)
cur = conn.cursor()

# Step 3: Insert the JSON data into the ODS table
for flight in flights_data:
    flight_json_str = json.dumps(flight)  # Convert dict to JSON string
    print(flight_json_str) # {"flight_date": "2024-08-18", "flight_status": "active", "departure": {"airport": "Taipei Songshan (Sung Shan)", "timezone": "Asia/Taipei", "iata": "TSA", "icao": "RCSS", "terminal": "2", "gate": null, "delay": 9, "scheduled": "2024-08-18T18:30:00+00:00", "estimated": "2024-08-18T18:30:00+00:00", "actual": "2024-08-18T18:38:00+00:00", "estimated_runway": "2024-08-18T18:38:00+00:00", "actual_runway": "2024-08-18T18:38:00+00:00"}, "arrival": {"airport": "Makung", "timezone": "Asia/Taipei", "iata": "MZG", "icao": "RCQC", "terminal": "1", "gate": null, "baggage": null, "delay": null, "scheduled": "2024-08-18T19:20:00+00:00", "estimated": "2024-08-18T19:20:00+00:00", "actual": null, "estimated_runway": null, "actual_runway": null}, "airline": {"name": "EVA Air", "iata": "BR", "icao": "EVA"}, "flight": {"number": "8625", "iata": "BR8625", "icao": "EVA8625", "codeshared": null}, "aircraft": null, "live": null}


    try:
        cur.execute("""
            INSERT INTO ods.flights_data_raw (source, data, load_timestamp)
            VALUES (
            'AviationStack API', 
            %s,
            CONVERT_TIMEZONE('UTC', 'Europe/Paris', CURRENT_TIMESTAMP())
            )
        """, (flight_json_str,))
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error inserting data: {e}")

# Step 4: Commit the transaction and close the connection
conn.commit()
cur.close()
conn.close()

print("Data loaded into Snowflake ODS successfully.")

