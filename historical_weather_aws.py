"""import pandas as pd
import requests
from datetime import datetime, timedelta
from pytz import timezone
import os
import json
from dotenv import load_dotenv
import boto3

#load .env file
load_dotenv()


#API credentials
api_key = os.getenv('REALM5_API_KEY')
dev_eui = os.getenv('WEATHER_STATION_DEVICE')
access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_region = os.getenv('AWS_REGION')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
aws_bucket_name = os.getenv('AWS_BUCKET_NAME')

local_file_path = "data"
aws_s3_file_key = "dashboard/data"

#Client
s3 = boto3.client("s3",
aws_access_key_id = access_key_id,
aws_secret_access_key = aws_secret_key,
region_name = aws_region
)


#Set timezones
eastern = timezone("US/Eastern")
utc = timezone("UTC")

#Get today's start and end in US/Eastern
now_eastern = datetime.now(eastern)
today_start_eastern = eastern.localize(datetime(now_eastern.year, now_eastern.month, now_eastern.day, 0, 0, 0))
today_end_eastern = today_start_eastern + timedelta(days=1)

#Convert to UTC ISO strings for API
today_start_utc = today_start_eastern.astimezone(utc).isoformat().replace("+00:00", "Z")
today_end_utc = today_end_eastern.astimezone(utc).isoformat().replace("+00:00", "Z")

#API endpoint
url = f"https://app.realmfive.com/api/v2/weather_stations/observations/{dev_eui}"
params = {
    "occurred_after": today_start_utc,
    "occurred_before": today_end_utc
}
headers = {
    "Content-Type": "application/json",
    "X-API-Key": api_key
}

#Request todays data
try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    api_data = response.json()
except requests.exceptions.RequestException as e:
    raise SystemExit(f"❌ Failed to fetch today's data: {e}")

#API JSON to DataFrame
records = []
for ts, values in api_data.items():
    #ts_local = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(eastern)
    ts_cleaned = ts.replace("+0000", "+00:00")
    ts_local = datetime.fromisoformat(ts_cleaned).astimezone(eastern)
    row = {"timestamp": ts_local}
    row.update(values)
    if "rainfall" not in row:
        row["rainfall"] = None
    records.append(row)

if not records:
    raise SystemExit("⚠️ No new data for today from API.")

df_today = pd.DataFrame(records)

#output directory
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)
historical_file = os.path.join(output_dir, "full_weather_data.csv")

#Load or initialize historical data
if os.path.exists(historical_file):
    df_hist = pd.read_csv(historical_file, parse_dates=["timestamp"])
    df_combined = pd.concat([df_hist, df_today])
    df_combined.drop_duplicates(subset="timestamp", inplace=True)
else:
    df_combined = df_today

df_combined.sort_values("timestamp", inplace=True)
df_combined.to_csv(historical_file, index=False)

#Parse timestamps in local time
dt_series = pd.to_datetime(df_combined['timestamp'], utc=True).dt.tz_convert('US/Eastern')
df_combined['date'] = dt_series.dt.date
df_combined['year'] = dt_series.dt.year
df_combined['day_of_year'] = dt_series.dt.dayofyear

#=== Temperature Summary ===
hist_temp = df_combined.groupby('day_of_year')['temperature'].agg(['min', 'max', 'mean']).reset_index()
hist_temp.columns = ['day_of_year', 'hist_min_temp', 'hist_max_temp', 'hist_avg_temp']

cur_year = now_eastern.year
cur_year_df = df_combined[df_combined['year'] == cur_year]

cur_temp = cur_year_df.groupby('date')['temperature'].agg(['min', 'max', 'mean']).reset_index()
cur_temp.columns = ['Date', 'cur_min_temp', 'cur_max_temp', 'cur_avg_temp']
cur_temp['day_of_year'] = pd.to_datetime(cur_temp['Date']).dt.dayofyear

temp_summary = pd.merge(cur_temp, hist_temp, on='day_of_year', how='left')
temp_summary = temp_summary[[
    'Date', 'hist_min_temp', 'hist_max_temp', 'hist_avg_temp',
    'cur_min_temp', 'cur_max_temp', 'cur_avg_temp'
]]

#=== Rainfall Summary ===
if 'rainfall' in df_combined.columns:
    cur_rain = cur_year_df.groupby('date')['rainfall'].sum().reset_index()
    cur_rain.columns = ['Date', 'daily_rainfall']
    cur_rain['day_of_year'] = pd.to_datetime(cur_rain['Date']).dt.dayofyear

    hist_rain = df_combined.groupby('day_of_year')['rainfall'].mean().reset_index()
    hist_rain.columns = ['day_of_year', 'hist_avg_rainfall']

    rain_summary = pd.merge(cur_rain, hist_rain, on='day_of_year', how='left')
    rain_summary['running_total'] = rain_summary['daily_rainfall'].cumsum()
    rain_summary = rain_summary[['Date', 'hist_avg_rainfall', 'daily_rainfall', 'running_total']]
else:
    rain_summary = pd.DataFrame(columns=[
        'Date', 'hist_avg_rainfall', 'daily_rainfall', 'running_total'
    ])

#=== Saving CSV summaries ===
temp_summary.to_csv(os.path.join(output_dir, "temperature_summary.csv"), index=False)
rain_summary.to_csv(os.path.join(output_dir, "rainfall_summary.csv"), index=False)

#Upload CSVs to S3
try:
    s3.upload_file(
        Filename=os.path.join(output_dir, "temperature_summary.csv"),
        Bucket=aws_bucket_name,
        Key=f"{aws_s3_file_key}/temperature_summary.csv"
    )
    print(f"✅ Uploaded temperature_summary.csv to s3://{aws_bucket_name}/{aws_s3_file_key}/")

    s3.upload_file(
        Filename=os.path.join(output_dir, "rainfall_summary.csv"),
        Bucket=aws_bucket_name,
        Key=f"{aws_s3_file_key}/rainfall_summary.csv"
    )
    print(f"✅ Uploaded rainfall_summary.csv to s3://{aws_bucket_name}/{aws_s3_file_key}/")

except Exception as e:
    print(f"❌ Failed to upload to S3: {e}")
"""





from dotenv import load_dotenv
import pandas as pd
import requests
from datetime import datetime, timedelta
from pytz import timezone
import os
import json
import boto3

#Load environment variables
load_dotenv()

#Configuration
historical_file = "data/realmfive_weather_full_data.csv"
temperature_output_file = "data/temperature_summary.csv"
rainfall_output_file = "data/rainfall_summary.csv"
api_key = os.getenv('REALM5_API_KEY')
dev_eui = os.getenv('WEATHER_STATION_DEVICE')
access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_region = os.getenv('AWS_REGION')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
aws_bucket_name = os.getenv('AWS_BUCKET_NAME')

local_file_path = "data"
aws_s3_file_key = "dashboard/data"

#Client
s3 = boto3.client("s3",
aws_access_key_id = access_key_id,
aws_secret_access_key = aws_secret_key,
region_name = aws_region
)

#Timezone setup
eastern = timezone("US/Eastern")
utc = timezone("UTC")

#Load historical data
df_full = pd.read_csv(historical_file, parse_dates=["timestamp"])
df_full['timestamp'] = pd.to_datetime(df_full['timestamp'], utc=True).dt.tz_convert(eastern)
df_full['date'] = df_full['timestamp'].dt.date
df_full['year'] = df_full['timestamp'].dt.year
df_full['month'] = df_full['timestamp'].dt.month
df_full['day'] = df_full['timestamp'].dt.day

#Get current year and today
today = datetime.now(eastern).date()
current_year = today.year

#Group historical average by month/day (all years)
hist_temp = df_full.groupby(['month', 'day'])['temperature'].agg(['min', 'max', 'mean']).reset_index()
hist_temp.columns = ['month', 'day', 'hist_min_temp', 'hist_max_temp', 'hist_avg_temp']

hist_rain = df_full.groupby(['month', 'day'])['rainfall'].mean().reset_index()
hist_rain.columns = ['month', 'day', 'hist_avg_rainfall']

#Extract current-year data from historical CSV
df_current_year = df_full[df_full['year'] == current_year].copy()

''' --- Temperature for current year --- '''
cur_temp = df_current_year.groupby('date')['temperature'].agg(['min', 'max', 'mean']).reset_index()
cur_temp['cur_min_temp'] = (cur_temp['min'] * 9/5 + 32).round(2)
cur_temp['cur_max_temp'] = (cur_temp['max'] * 9/5 + 32).round(2)
cur_temp['cur_avg_temp'] = (cur_temp['mean'] * 9/5 + 32).round(2)
cur_temp['month'] = pd.to_datetime(cur_temp['date']).dt.month
cur_temp['day'] = pd.to_datetime(cur_temp['date']).dt.day

''' --- Rainfall for current year --- '''
cur_rain = df_current_year.groupby('date')['rainfall'].sum().reset_index()
cur_rain['daily_rainfall'] = (cur_rain['rainfall'] * 0.1).round(2)
cur_rain['month'] = pd.to_datetime(cur_rain['date']).dt.month
cur_rain['day'] = pd.to_datetime(cur_rain['date']).dt.day
cur_rain = cur_rain.sort_values('date')
cur_rain['running_total'] = cur_rain['daily_rainfall'].cumsum()

''' Full calendar for the current year '''
year_days = pd.date_range(start=datetime(current_year, 1, 1), end=datetime(current_year, 12, 31), freq='D')
calendar = pd.DataFrame({'Date': year_days})
calendar['month'] = calendar['Date'].dt.month
calendar['day'] = calendar['Date'].dt.day

#Merge all
temp_summary = calendar.merge(hist_temp, on=['month', 'day'], how='left')
temp_summary = temp_summary.merge(
    cur_temp[['month', 'day', 'cur_min_temp', 'cur_max_temp', 'cur_avg_temp']],
    on=['month', 'day'], how='left'
)
temp_summary = temp_summary[['Date', 'hist_min_temp', 'hist_max_temp', 'hist_avg_temp', 'cur_min_temp', 'cur_max_temp', 'cur_avg_temp']]

rain_summary = calendar.merge(hist_rain, on=['month', 'day'], how='left')
rain_summary = rain_summary.merge(
    cur_rain[['month', 'day', 'daily_rainfall', 'running_total']],
    on=['month', 'day'], how='left'
)
rain_summary = rain_summary[['Date', 'hist_avg_rainfall', 'daily_rainfall', 'running_total']]

#Output to files
output_dir = "data"
os.makedirs("data", exist_ok=True)
temp_summary.to_csv(temperature_output_file, index=False)
rain_summary.to_csv(rainfall_output_file, index=False)


#Upload CSVs to S3
try:
    s3.upload_file(
        Filename=os.path.join(output_dir, "temperature_summary.csv"),
        Bucket=aws_bucket_name,
        Key=f"{aws_s3_file_key}/temperature_summary.csv"
    )
    print(f"✅ Uploaded temperature_summary.csv to s3://{aws_bucket_name}/{aws_s3_file_key}/")

    s3.upload_file(
        Filename=os.path.join(output_dir, "rainfall_summary.csv"),
        Bucket=aws_bucket_name,
        Key=f"{aws_s3_file_key}/rainfall_summary.csv"
    )
    print(f"✅ Uploaded rainfall_summary.csv to s3://{aws_bucket_name}/{aws_s3_file_key}/")

except Exception as e:
    print(f"❌ Failed to upload to S3: {e}")