
#Libraries
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

#print(f"the api key is {api_key} and device eui is {dev_eui}")

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


'''A function block to fetch current temperature and rainfall data'''

def fetch_current_year_weather_data(dev_eui, api_key):
    base_url = f"https://app.realmfive.com/api/v2/weather_stations/observations/{dev_eui}"
    headers = {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "accept": "application/json"
    }

    start_date = datetime(datetime.now().year, 1, 1).astimezone(utc).isoformat()
    end_date = datetime.now(utc).isoformat()

    params = {
        "occurred_after": start_date,
        "occurred_before": end_date
    }

    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    records = []
    for ts, values in data.items():
        values["timestamp"] = ts
        #Fill missing rainfall with 0
        if "rainfall" not in values:
            values["rainfall"] = 0
        records.append(values)

    df = pd.DataFrame(records)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert(eastern)
        df['date'] = df['timestamp'].dt.date
        return df

    return pd.DataFrame()

''' Aggregate current temperature and rainfall data '''
def summarize_daily_weather(df):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    #Temperature summary
    temp_summary = df.groupby('date')['temperature'].agg(['min', 'max', 'mean']).reset_index()
    temp_summary.columns = ['date', 'cur_min_temp', 'cur_max_temp', 'cur_avg_temp']
    temp_summary[['cur_min_temp', 'cur_max_temp', 'cur_avg_temp']] = (
        temp_summary[['cur_min_temp', 'cur_max_temp', 'cur_avg_temp']] * 9/5 + 32
    ).round(2)

    #Rainfall summary
    rain_summary = df.groupby('date')['rainfall'].sum().reset_index()
    rain_summary['daily_rainfall'] = (rain_summary['rainfall'] * 0.1).round(2)
    rain_summary['running_total'] = rain_summary['daily_rainfall'].cumsum()

    return temp_summary, rain_summary

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
#print(f"{hist_temp}")
hist_temp[['min', 'max', 'mean']] = hist_temp[['min', 'max', 'mean']].apply(lambda x: (x * 9/5 + 32).round(2))
hist_temp.columns = ['month', 'day', 'hist_min_temp', 'hist_max_temp', 'hist_avg_temp']
#print(f"{hist_temp}")
hist_temp = hist_temp.drop_duplicates(subset=['month', 'day'])

hist_rain = df_full.groupby(['month', 'day'])['rainfall'].mean().reset_index()
#print(f"{hist_rain}")
hist_rain.columns = ['month', 'day', 'hist_avg_rainfall']
hist_rain = hist_rain.drop_duplicates(subset=['month', 'day'])

#Keep all data regardless of year for daily summaries
df_recent = df_full.copy()
#print(f" {df_current_year}")

#Fetch current live data
df_current = fetch_current_year_weather_data(dev_eui, api_key)

#Summarize it into daily current temperature and rainfall
cur_temp, cur_rain = summarize_daily_weather(df_current)

#Add month and day columns for merge
cur_temp['month'] = pd.to_datetime(cur_temp['date']).dt.month
cur_temp['day'] = pd.to_datetime(cur_temp['date']).dt.day

cur_rain['month'] = pd.to_datetime(cur_rain['date']).dt.month
cur_rain['day'] = pd.to_datetime(cur_rain['date']).dt.day

''' Full calendar for the current year '''
year_days = pd.date_range(start=datetime(current_year, 1, 1), end=datetime(current_year, 12, 31), freq='D')
calendar = pd.DataFrame({'Date': year_days})
calendar['month'] = calendar['Date'].dt.month
calendar['day'] = calendar['Date'].dt.day

#Merge temperature
temp_summary = calendar.merge(hist_temp, on=['month', 'day'], how='left')
temp_summary = temp_summary.merge(
    cur_temp[['month', 'day', 'cur_min_temp', 'cur_max_temp', 'cur_avg_temp']],
    on=['month', 'day'], how='left'
)
temp_summary = temp_summary[['Date', 'hist_min_temp', 'hist_max_temp', 'hist_avg_temp', 'cur_min_temp', 'cur_max_temp', 'cur_avg_temp']]

#Merge rainfall
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

