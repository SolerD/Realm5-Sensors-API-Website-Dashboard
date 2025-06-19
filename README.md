# REalm5 Weather API data for website dashboard

This script focuses on daily and historical weather data from Realm5 API for 4D-farm website dashboard statistics

## Task 
1-historical_weather_asw.py:' generatestwo CSV files one for tempearture and one for rainfall
2-hourly_weather_updates_aws.py: generates a JSON file with current temperature data
-data/': output of the two CSV files appended daily 
'-env': stores API key and weather device EUI (Not in Repo)

## Setup
1. Create the .env file
realmfive api key and dev eui

2. install dependencies, like requests and other python libraries see the 'requirements.txt'
pip install -r requirements.txt

3. Run the script:historical_weather_aws.py
4. Run the script:hourly_weather_updates.py

