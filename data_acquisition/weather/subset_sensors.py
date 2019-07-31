"""
    This script maps the sensors from a specific location given its coordinates constraint by a maximum radius defined by the user to their
    closest weather stations, it saves the final results to the relative path: ../../data/intermediate/berlin_enrichable_sensors.csv
"""

__author__ = 'Ricardo Salazar'

from geopy.distance import geodesic
import numpy as np
import pandas as pd
import pyowm
import time
from causal_analysis.utils import get_closest_weather_station
from urllib3.response import ReadTimeoutError


def create_sensors_dataframe():
    ''' creates a pandas dataframe containing all sensors in the processed data folder

    Returns: a pandas dataframe containing all the sensors in the processed data folder

    '''
    sensor_types = ["bme280", "bmp180", "dht22", "ds18b20", "hpm", "htu21d", "pms3003", "pms5003", "pms7003", "ppd42ns",
                    "sds011"]

    total_sensors = pd.DataFrame(columns=["sensorId", "location", "lat", "lon"])
    for sensor_type in sensor_types:
        current_df = pd.read_csv(f"../../data/processed/statistics/{sensor_type}.csv", sep=";",
                                 names=["sensorId", "sensorType", "location", "lat", "lon", "minTimestamp",
                                        "maxTimestamp", "readingCount"])
        total_sensors = total_sensors.append(current_df[["sensorId", "sensorType", "location", "lat", "lon"]])

    total_sensors.dropna(inplace=True)

    return total_sensors


def filter_sensors(sensor):
    ''' This function filters the sensors based on its distance to a given Berlin Mitte coordinates
    Args:
        sensor: a tuple containing a sensor latitude and longitude

    Returns: a boolean result, true if valid tuple false otherwise

    '''
    MAX_DISTANCE = 50
    sensor_tuple = (sensor["lat"], sensor["lon"])
    berlin_tuple = (52.520008, 13.404954)
    return geodesic(sensor_tuple, berlin_tuple).km < MAX_DISTANCE


def map_sensors(total_sensors):
    ''' Maps the sensors to their closest weather station

    Args:
        total_sensors: A pandas dataframe containing the sensors to be mapped

    Returns:  a list with the sensor geographical information and its closest weather station details

    '''
    credentials_1 = pyowm.OWM('48dae982f9e685eee268e90dafba5041')
    closest_weather_stations = []
    for index, sensor in total_sensors.iterrows():
        time.sleep(1)
        for attempt in range(10):
            try:
                stations = get_closest_weather_station(sensor["lat"], sensor["lon"], credentials_1)
                break
            except Exception:
                print(f"ReadTimeoutError, retry #{attempt + 1}")
                time.sleep(30)
        if stations:
            location = stations[0].get_location()
            closest_weather_stations.append(
                [sensor["sensorId"], sensor["sensorType"], sensor["lon"], sensor["lat"], sensor["location"],
                 location.get_name(), location.get_lon(), location.get_lat()])

    return closest_weather_stations


def filter_for_weather_stations(sensor):
    ''' filter out the sensors whose closest weather station is not in the list of unique weather locations

    Args:
        sensor: A valid sensor observation

    Returns:

    '''
    weather = pd.read_csv("../../data/raw/weather/weather_data.csv", sep=";")

    unique_locations = weather["location"].unique()

    return sensor["stationName"] in unique_locations


def create_enrichable_sensors_and_save_result(unique_closest_weather_stations):
    berlin_enrichable_sensors = pd.DataFrame(unique_closest_weather_stations,
                                             columns=["sensorId", "sensorType", "lon", "lat", "location", "stationName",
                                                      "stationLon", "stationLat"])

    weather = pd.read_csv("../../data/raw/weather/weather_data.csv", sep=";")

    unique_locations = weather["location"].unique()

    berlin_enrichable_sensors = berlin_enrichable_sensors[
        berlin_enrichable_sensors.apply(filter_for_weather_stations, axis=1)]
    berlin_enrichable_sensors.to_csv("../../data/intermediate/berlin_enrichable_sensors.csv", sep=",", index=False)


def subset_sensors():
    total_sensors = create_sensors_dataframe()
    total_sensors = total_sensors[total_sensors.apply(filter_sensors, axis=1)]
    closest_weather_stations = map_sensors(total_sensors)
    unique_closest_weather_stations = np.unique(np.asarray(closest_weather_stations), axis=0)
    create_enrichable_sensors_and_save_result(unique_closest_weather_stations)


if __name__ == '__main__':
    subset_sensors()
