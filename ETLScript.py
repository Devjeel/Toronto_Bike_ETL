#!/usr/bin/env python3
import datetime

import pandas as pd
import requests
import threading
import logging
import math

# Logging
logging.basicConfig(filename='ETL.log', level=logging.INFO, filemode='a',
                    format='%(asctime)s.%(msecs)03d : %(levelname)s - %(name)s - %(message)s')

# Lets make request and parse it to JSON
# If we have more URLs to parse we can use threading or Concurrent.ThreadExecutor
station_info_data = requests.get(
    url="https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information").json()

logging.info("Station Info Downloaded")

station_bike_data = requests.get(
    url="https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status").json()

logging.info("Station Bike Data Downloaded")

# Getting station data and running for loop to attach data
stations_list = station_info_data['data']['stations']
bike_list = station_bike_data['data']['stations']

logging.info("ETL Process Started")

for bike in bike_list:
    for station in stations_list:
        if bike['station_id'] == station['station_id']:
            # Update station data like Dictionary
            station.update(bike)

            # Check if Station is not in service "END_OF_LIFE"
            if station['status'] == "END_OF_LIFE":
                print("Deleting Out Of Service Station..")
                del station

# Normalizing and converting from JSON to DF
df = pd.json_normalize(stations_list)

# Epoch/Unix-time to DateTime
stamp = datetime.datetime.fromtimestamp(station_info_data['last_updated']).strftime('%Y-%m-%d %H:%M:%S')
df['Last Updated'] = stamp

logging.info(f"Exporting {len(df)} Rows to CSV")

# Let's export; We don't want index as a column
df.to_csv("Toronto_Bike_Stations.csv", index=False)

logging.info("ETL Done.")

# Challenge 3: Find nearest bike stations; Coords:(43.661896 / -79.396160)
def calculateDistance(x2,y2,x1=43.661896,y1=-79.396160):
    # Euclidean Distance
    return math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

d = dict()

for i in range(len(df)):
    lat, lon = float(df.iloc[i, df.columns.get_loc('lat')]), float(df.iloc[i, df.columns.get_loc('lon')])
    d[df.iloc[i, df.columns.get_loc('address')]] = calculateDistance(x2=lat, y2=lon)


# Get Top 3 closest distance
closest_stations = sorted(d, key=d.get)[:3]

# Rounding numbers to 4 for KMs
print(" Your Closest stations are below: \n\t Address \t\t KM")
print(f"1. {closest_stations[0]} - {round(d.get(closest_stations[0]), 4)}")
print(f"2. {closest_stations[1]} - {round(d.get(closest_stations[1]), 4)}")
print(f"3. {closest_stations[2]} - {round(d.get(closest_stations[2]), 4)}")
