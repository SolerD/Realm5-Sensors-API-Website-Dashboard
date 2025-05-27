# REalm5 Weather API data for website dashboard

This script focuses on daily and historical weather data from Realm5 API for 4D-farm website dashboard statistics

## Task 1
-historical weather.py:' generatestwo CSV files one for tempearture and one for rainfall
-data/': output of the two CSV files appended daily 
'-env': stores API key and weather device EUI (Not in Repo)

## Setup
1. Create the .env file
realmfive api key and dev eui

2. install dependencies, like requests and other python libraries see the 'requirements.txt'
pip install -r requirements.txt

3. Run the script:historical_weather.py

## Task 2
-observation_weather.py: Pulls weather data from the API then generates the historical data
-realm5test.py: API connection testing