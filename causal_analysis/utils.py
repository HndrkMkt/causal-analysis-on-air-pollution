""""
    This module contains a set of functions that support some of the scripts needed to subset and localize the sensors and the weather
    stations

"""

__author__ = 'Ricardo Salazar'

import pyowm
import forecastio
from datetime import datetime
from datetime import timedelta
import datetime as dt
import pandas as pd
import numpy as np
import glob
import pyarrow.parquet as pq


def get_closest_weather_station(latitude, longitude, credentials):
    ''' Finds the closest weather station for the requested coordinates
    Args:
        latitude: The latitude of interest
        longitude: The longitude of interest
        credentials: The pyowm secret key

    Returns: The closest weather station name and coordinates for the location

    '''
    return credentials.weather_around_coords(latitude, longitude, limit=1)


def get_weather_variables_for_locations(list_of_locations, start_date, days_back, credentials):
    ''' Fetchs historical weather data for a list of locations

    Args:
        list_of_locations: A list of weather stations with their coordinates
        start_date: The date from where start pulling data
        days_back: The amount of days to go back with the data
        credentials: A forecastio secret key

    Returns:

    '''
    start_date = dt.datetime.strptime(str(start_date + ' 23:00:0'), '%Y-%m-%d %H:%M:%S')
    data_block_list = []
    for d in range(days_back):
        for location in list_of_locations:
            forecast = forecastio.load_forecast(credentials, location[6], location[5],
                                                time=start_date - timedelta(days=d))
            data_block_list.append(
                [forecast.hourly(), start_date - timedelta(days=d), location[0], location[1], location[2]])

        tuples = []
    for data_block in data_block_list:
        for hourlyData in data_block[0].data:
            t = []
            t.extend([data_block[2],
                      data_block[3],
                      data_block[4],
                      hourlyData.time])
            try:
                t.extend([hourlyData.temperature])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.apparentTemperature])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.cloudCover])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.dewPoint])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.humidity])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.ozone])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.precipIntensity])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.precipProbability])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.precipType])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.pressure])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.uvIndex])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.visibility])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.windBearing])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.windGust])
            except:
                t.extend([None])
            try:
                t.extend([hourlyData.windSpeed])
            except:
                t.extend([None])

        tuples.append(t)
    return tuples


def percentile(n):
    '''Finds the value of the requested percentile in an array

    Args:
        n: the requested percentile

    Returns: The value of the requested percentile

    '''

    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


def make_agg_df(df):
    ''' agregates a dataframe containing sensor measures by creating new columns containing the min,percentile_percentile_25, \
        mean,percentile_75 and maximum values of it
    Args:
        df: a pandas dataframe with the sensor data

    Returns:

    '''
    df['date'] = pd.to_datetime(df.timestamp).dt.date
    df['hour'] = pd.to_datetime(df.timestamp).dt.hour

    agg_df = df.groupby(
        ['sensor_id', 'date', 'hour'], as_index=False
    ).agg(
        {
            # find the min, max of the P1 column
            'P1': [min, percentile(25), 'mean', percentile(75), max],
            # find the min, max of the P2 column
            'P2': [min, percentile(25), 'mean', percentile(75), max],
        }
    )

    return agg_df
