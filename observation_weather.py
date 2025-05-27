"""'''This is a python script to extract information from the realm5 sensors
Programmed by Deus F.K @May2025'''

#Headers
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

#loading env variables
api_key = os.getenv('realm_five_api_key')
dev_eui = os.getenv('weather_station_dev_eui')

#getting weather API endpoint
base_url = "https://app.realmfive.com/api/"
url = f"{base_url}v2/weather_stations/observations/{dev_eui}"

#defining API header
headers = {
    'Content-Type': 'application/json',
    'X-API-Key': api_key 
}

'''Making an API request'''
response = requests.get(url,headers=headers)
print(response)
if response.status_code == 200:
    print("HTTP request sucessfully...!!!")
    data = response.json()
    
    if data:
        with open("weather_data_json", "w") as file:
            json.dump(data, file, indent = 3)


#print(f"{api_key}, {dev_eui}")

"""

import json
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import os

#API credentials (hardcoded or replace with os.getenv for production)
api_key = "2S6C7zJEANXxuwMk9FrK7ifvqoiDm89p"
dev_eui = 26218796

#Get current UTC hour start and end
current_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
start_time = current_utc.isoformat().replace("+00:00", "Z")
end_time = (current_utc + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

print(f"🔍 Fetching data from {start_time} to {end_time}")

#Updated RealmFive endpoint using time filtering
url = f"https://app.realmfive.com/api/v2/weather_stations/observations/{dev_eui}"
params = {
    "occurred_after": start_time,
    "occurred_before": end_time
}
headers = {
    "Content-Type": "application/json",
    "X-API-Key": api_key
}

#Request data from API
try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"❌ Failed to fetch data: {e}")
    raise SystemExit

#No data?
if not data or len(data) == 0:
    print("⚠️ No data returned for the current hour.")
    raise SystemExit

#Aggregate data
aggregated = defaultdict(list)

for readings in data.values():
    for key, value in readings.items():
        aggregated[key].append(value)

#Keys to include
expected_keys = [
    "dew_point_c", "temperature_c", "sea_level_pressure_hPa",
    "wind_speed_kph", "wind_direction_degrees", "wind_gust_kph_max",
    "solar_radiation_watts_per_meter_squared", "humidity_percent",
    "pressure_hPa", "rainfall_in"
]

#Unit conversion
def convert_to_us_units(key, value):
    if value is None:
        return None
    if key in ["temperature_c", "dew_point_c"]:
        return round((value * 9/5) + 32, 1)  # °C → °F
    elif key == "rainfall_in":
        return round(value * 0.0393701, 2)   # mm → inches
    elif "pressure" in key:
        return round(value * 0.02953, 2)     # hPa → inHg
    elif key in ["wind_speed_kph", "wind_gust_kph_max"]:
        return round(value * 0.621371, 1)    # kph → mph
    elif key == "solar_radiation_watts_per_meter_squared":
        return round(value * 2589988.11, 1)  # W/m² → W/mi²
    else:
        return round(value, 1) if isinstance(value, float) else value

#Summarize into single hourly object
summary = {
    "timestamp": current_utc.isoformat().replace("+00:00", "Z")  #Global UTC timestamp
}

for key in expected_keys:
    values = aggregated.get(key, [])
    val = sum(values) if key == "rainfall_in" else (sum(values) / len(values)) if values else None
    summary[key] = convert_to_us_units(key, val)

# Save to file
file_name = f"weather_summary_{current_utc.strftime('%Y-%m-%dT%H')}.json"
with open(file_name, "w") as f:
    json.dump(summary, f, indent=2)

print(f"✅ Hourly weather summary saved to {file_name}")
