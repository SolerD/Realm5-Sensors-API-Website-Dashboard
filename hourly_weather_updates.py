#Libraries
import json
import requests
from datetime import datetime, timezone
from collections import defaultdict
import os
from dotenv import load_dotenv

load_dotenv()

#API credentials
api_key = os.getenv('realm_five_api_key')
dev_eui = os.getenv('weather_station_dev_eui')


#RealmFive API setup
url = f"https://app.realmfive.com/api/v2/device_readings/weather_station_readings/{dev_eui}"
headers = {
    "Content-Type": "application/json",
    "X-API-Key": api_key
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"❌ Failed to fetch data: {e}")
    raise SystemExit

#Get current hour (UTC)
current_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
current_hour_str = current_utc.isoformat(timespec='minutes')[:13]  #'YYYY-MM-DDTHH'

print(f"🔍 Filtering records for hour (UTC): {current_hour_str}")

#Filter 15-min records for current hour
current_hour_records = {}

for ts, readings in data.items():
    dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
    if dt.strftime("%Y-%m-%dT%H") == current_hour_str:
        current_hour_records[ts] = readings

if not current_hour_records:
    print("⚠️ No data available for the current hour.")
    raise SystemExit

#Aggregate readings
aggregated = defaultdict(list)

for readings in current_hour_records.values():
    for key, value in readings.items():
        aggregated[key].append(value)

#Fields to include
expected_keys = [
    "dew_point_c", "temperature_c", "sea_level_pressure_hPa",
    "wind_speed_kph", "wind_direction_degrees", "wind_gust_kph_max",
    "solar_radiation_watts_per_meter_squared", "humidity_percent",
    "pressure_hPa", "rainfall_in"
]

#✅Unit conversion function
def convert_to_us_units(key, value):
    if value is None:
        return None

    if key in ["temperature_c", "dew_point_c"]:
        return round((value * 9/5) + 32, 1)  # °C → °F

    elif key == "rainfall_in":
        return round(value * 0.1, 2)   # rounds → inches

    elif "pressure" in key:
        return round(value * 0.02953, 2)     # hPa → inHg

    elif key in ["wind_speed_kph", "wind_gust_kph_max"]:
        return round(value * 0.621371, 1)    # kph → mph

    elif key == "solar_radiation_watts_per_meter_squared":
        return round(value * 2589988.11, 1)  # W/m² → W/mi²

    else:
        return round(value, 1) if isinstance(value, float) else value

# ✅Output summary in UTC ISO 8601 format (Z suffix)
summary = {
    "timestamp": current_utc.isoformat().replace("+00:00", "Z")
}

"""for key in expected_keys:
    values = aggregated.get(key, [])
    val = sum(values) if key == "rainfall_in" else sum(values) / len(values) if values else None
    summary[key] = convert_to_us_units(key, val)"""

for key in expected_keys:
    values = aggregated.get(key, [])

    if not values:
        #summary[key] = None
        summary[key] = 0.0 if key == "rainfall_in" else None
        continue

    # Apply meteorology-based logic
    if key == "rainfall_in":
        val = sum(values)  # Total hourly rainfall
    elif key in ["wind_gust_kph_max", "solar_radiation_watts_per_meter_squared"]:
        val = max(values)  # Peak values
    else:
        val = sum(values) / len(values)  # Average everything else

    # Convert to US units
    #summary[key] = convert_to_us_units(key, val)
    # Mapping to simplified US-readable keys
    rename_keys = {
    "dew_point_c": "dew_point",
    "temperature_c": "temperature",
    "sea_level_pressure_hPa": "sea_level_pressure",
    "wind_speed_kph": "wind_speed",
    "wind_direction_degrees": "wind_direction",
    "wind_gust_kph_max": "wind_gust",
    "solar_radiation_watts_per_meter_squared": "solar_radiation",
    "humidity_percent": "humidity",
    "pressure_hPa": "pressure",
    "rainfall_in": "rainfall"
    }

    # Use renamed key in final JSON
    final_key = rename_keys.get(key, key)
    summary[final_key] = convert_to_us_units(key, val)


#Save output file
os.makedirs("data", exist_ok=True)
file_name = f"data/weather_summary_{current_utc.strftime('%Y-%m-%dT%H')}.json"
with open(file_name, "w") as f:
    json.dump(summary, f, indent=2)

print(f"✅ Hourly weather summary saved to {file_name}")
