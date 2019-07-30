from geopy.distance import geodesic
import numpy as np
import pandas as pd
import pyowm
import utils
import time
from urllib3.response import ReadTimeoutError

sensor_types = ["bme280", "bmp180", "dht22", "ds18b20", "hpm", "htu21d", "pms3003", "pms5003", "pms7003", "ppd42ns", "sds011"]
credentials_1 = pyowm.OWM('48dae982f9e685eee268e90dafba5041')
MAX_DISTANCE = 50

total_sensors = pd.DataFrame(columns=["sensorId", "location", "lat", "lon"])
for sensor_type in sensor_types:
    current_df = pd.read_csv(f"../../data/processed/statistics/{sensor_type}.csv", sep=";", names=["sensorId", "sensorType", "location", "lat", "lon", "minTimestamp", "maxTimestamp", "readingCount"])
    total_sensors = total_sensors.append(current_df[["sensorId", "sensorType", "location", "lat", "lon"]])

total_sensors.dropna(inplace=True)


def filter_sensors(sensor):
    sensor_tuple = (sensor["lat"], sensor["lon"])
    berlin_tuple = (52.520008,  13.404954)
    return geodesic(sensor_tuple, berlin_tuple).km < MAX_DISTANCE
total_sensors = total_sensors[total_sensors.apply(filter_sensors, axis=1)]

closest_weather_stations=[]
for index, sensor in total_sensors.iterrows():
    time.sleep(1)
    for attempt in range(10):
        try:
            stations = utils.get_closest_weather_station(sensor["lat"],sensor["lon"],credentials_1)
            break
        except Exception:
            print(f"ReadTimeoutError, retry #{attempt + 1}")
            time.sleep(30)
    if stations:
        location = stations[0].get_location()
        closest_weather_stations.append([sensor["sensorId"], sensor["sensorType"],sensor["lon"],sensor["lat"],sensor["location"],location.get_name(),location.get_lon(),location.get_lat()])

unique_closest_weather_stations=np.unique(np.asarray(closest_weather_stations),axis=0)

berlin_enrichable_sensors = pd.DataFrame(unique_closest_weather_stations, columns=["sensorId", "sensorType", "lon", "lat", "location", "stationName", "stationLon", "stationLat"])

weather = pd.read_csv("../../data/raw/weather_data.csv", sep=";")

unique_locations = weather["location"].unique()

def filter_for_weather_stations(sensor):
    return sensor["stationName"] in unique_locations


berlin_enrichable_sensors = berlin_enrichable_sensors[berlin_enrichable_sensors.apply(filter_for_weather_stations, axis=1)]
berlin_enrichable_sensors.to_csv("../../data/intermediate/berlin_enrichable_sensors.csv", sep=",", index=False)