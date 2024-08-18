import requests
import snowflake.connector
import json
from datetime import datetime

#Step 1: Fetch the JSON data from the API
api_url = 'https://api.aviationstack.com/v1/flights'
params = {
    'access_key': '**',
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
    password='**',
    account='*',
    database='flight',
    schema='ods'
)
cur = conn.cursor()

# Step 3: Insert the JSON data into the ODS table
for flight in flights_data:
    flight_json_str = json.dumps(flight)  # Convert dict to JSON string
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
