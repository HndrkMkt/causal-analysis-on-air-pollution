"""Data preparation for causal discovery.

This module contains functions to generate a TIGRAMITE dataframe from the intermediate data created by the data
processing component.
"""

__author__ = 'Ricardo Salazar'

import pandas as pd
from datetime import datetime
import numpy as np
import sys
from operator import itemgetter
from tigramite import data_processing as pp


def feature_list():
    ''' Creates a list with the final features included in the intermediate dataset

    Returns: A list containing the final features in the intermediate dataset

    '''
    return ["location",
            "timestamp",
            "lat",
            "lon",
            "dayOfYear",
            "minuteOfDay",
            "minuteOfYear",
            "dayOfWeek",
            "isWeekend",
            "pressure_1",
            "pressure_sealevel",
            "temperature",
            "humidity_sensor",
            "p1",
            "p2",
            "p0",
            "durP1",
            "ratioP1",
            "durP2",
            "ratioP2",
            "apparent_temperature",
            "cloud_cover",
            "dew_point",
            "humidity",
            "ozone",
            "precip_intensity",
            "precip_probability",
            "precip_type",
            "pressure",
            "uv_index",
            "visibility",
            "wind_bearing",
            "wind_gust",
            "wind_speed"]


def sensor_family():
    ''' returns a list with the features that belong to the sensor family

    Returns: returns a list with the features that belong to the sensor family

    '''
    return ["location",
            "timestamp",
            "lat",
            "lon",
            "pressure_1",
            "pressure_sealevel",
            "temperature",
            "humidity_sensor",
            "p1",
            "p2",
            "p0",
            "durP1",
            "ratioP1",
            "durP2",
            "ratioP2"]


def time_family():
    ''' returns a list with the features that belong to the time family

    Returns: returns a list with the features that belong to the time family

    '''
    return ["dayOfYear",
            "minuteOfDay",
            "minuteOfYear",
            "dayOfWeek",
            "isWeekend"]


def weather_family():
    ''' returns a list with the features that belong to the weather family

    Returns: returns a list with the features that belong to the weather family

    '''
    return ["apparent_temperature",
            "cloud_cover",
            "dew_point",
            "humidity",
            "ozone",
            "precip_intensity",
            "precip_probability",
            "precip_type",
            "pressure",
            "uv_index",
            "visibility",
            "wind_bearing",
            "wind_gust",
            "wind_speed"]

# Create lists of families
sensor_family = sensor_family()
time_family = time_family()
weather_family = weather_family()


def family_list():
    ''' returns a list of the families list

    Returns: returns a list of the families list

    '''
    return [sensor_family, time_family, weather_family]


def load_data(path):
    ''' This function loads the intermediate dataset from csv

    Args:
        path: The path where the intermidiate csv lies

    Returns: a pandas dataframe containing the original features of the csv file plus some new time features

    '''
    features = feature_list()
    features.remove("minuteOfYear")
    sensor_data = pd.read_csv(path, sep=";", names=features, true_values=["true"], false_values=["false"])

    sensor_data["timestamp"] = pd.to_datetime(sensor_data["timestamp"])
    sensor_data["minuteOfYear"] = sensor_data["dayOfYear"] * 60 + sensor_data["minuteOfDay"]
    sensor_data["isWeekend"] = sensor_data["isWeekend"].astype(int)

    sensor_data = sensor_data.sort_values(by=["location", "timestamp"])
    return sensor_data


def subset(data, by_family=[], by_columns=[], start_date='', end_date=''):
    ''' This function allows subsetting the intermidiate data set by either choosing the desired family of features or by column.
        Also, a time frame can be selected choosing a start and end dates

    Args:
        data: A pandas dataframe with the intermidiate data
        by_family: A list with the desired families to be included in the subsetting
        by_columns: A list with the desired columns to be included in the subsetting
        start_date: The start date of the time frame
        end_date: The end date of the time frame

    Returns: A subset of the original dataframe

    '''
    if by_family or by_columns:
        final_feature_list = ['timestamp']
        if by_family:
            for i in by_family:
                if i == 'sensor_family':
                    # for j in sensor_family:
                    final_feature_list.extend(sensor_family)
                elif i == 'time_family':
                    # for j in time_family:
                    final_feature_list.extend(time_family)
                elif i == 'weather_family':
                    # for j in weather_family:
                    final_feature_list.extend(weather_family)
                else:
                    print('Oops! are you sure all the families exist?, maybe check spelling')
                    raise Exception

        if by_columns:
            for i in by_columns:
                if i in feature_list():
                    final_feature_list.extend([i])
                else:
                    print('Oops! are you sure all the columns exist in the dataframe?, maybe check spelling')
                    raise Exception

        final_feature_list = list(dict.fromkeys(final_feature_list))
        sensor_data = data[final_feature_list]

    else:
        sensor_data = data

    if start_date != '' and end_date != '':

        s = datetime.strptime(start_date, '%Y-%m-%d')
        e = datetime.strptime(end_date, '%Y-%m-%d')

        sensor_data = sensor_data.loc[(sensor_data['timestamp'] < e) & (sensor_data['timestamp'] > s)]
    else:
        pass
    return sensor_data.drop_duplicates()


def localize(data, lat, lon, results=1):
    ''' This function localize a user-specified number of sensors around a set of coordinates by computing the euclidian distance

    Args:
        data: a pandas dataframe
        lat: latitude
        lon: longitude
        results: the desired number of sensors around the specified location

    Returns: a dataframe containing the observations that were included in the localization

    '''
    must = ['location', 'lat', 'lon']
    if all([i in list(data) for i in must]):
        locations_array = np.asarray(data[['location', 'lat', 'lon']].drop_duplicates())
        distances = []
        for i in locations_array:
            distances.append([i[0], np.sqrt((i[1] - lat) ** 2 + (i[2] - lon) ** 2)])
        distances = sorted(distances, key=itemgetter(1))
        slices = [i[0] for i in distances[0:results]]
        localized_data = data.loc[data['location'].isin(slices)]
    else:
        print('Oops! are you sure you included the <location>, <lat> and <lon> fields in the dataframe?'  )
        raise Exception

    return localized_data


def input_na(data, columns, method=None, value=None):
    ''' This function allows na imputation in a given pandas dataframe. User can either select a method from {‘backfill’, ‘bfill’, ‘pad’, ‘ffill’, None}
        or a specific value

    Args:
        data: A pandas dataframe
        columns: A list of columns to perform the na imputation
        method: A method from this list {‘backfill’, ‘bfill’, ‘pad’, ‘ffill’, None}
        value: An specific value to fill the na's

    Returns: A pandas dataframe with modified null values.

    '''
    must = ['location', 'timestamp']
    if all([i in list(data) for i in must]):
        x = data.sort_values(by=["location", "timestamp"])
        if 'precip_type' in columns and 'precip_type' in list(data):
            x['precip_type'].replace(np.nan, 'no precip', regex=True, inplace=True)
        else:
            pass

        if all([i in list(data) for i in columns]):
            for i in columns:
                if method != None:
                    x[i].fillna(method=method, inplace=True)
                elif value != None:
                    x[i].fillna(value=value, inplace=True)
        else:
            print('Oops! are you sure all the columns exist in the dataframe?, maybe check spelling')
            raise Exception

        no_nulls_list = []
        for j in list(data):
            if x[j].isnull().sum().sum() == 0:
                no_nulls_list.extend([j])

    # x=x.dropna()

    return x


def create_tigramite_dataframe(dataset, exclude):
    ''' Creates a TIGRAMITE datframe from a pandas dataframe

    Args:
        dataset: A pandas dataframe with a timestamp column in it an numeric measures
        exclude: A list of columns to be excluded from the TIGRAMITEs dataframe

    Returns: A TIGRAMITE dataframe

    '''
    must = ['timestamp']
    var_list = list(dataset)
    if all([i in var_list for i in exclude]):
        for i in exclude:
            var_list.remove(i)
    else:
        print('Oops! are you sure all the columns to exclude exist in the dataframe?, maybe check spelling')
        raise Exception

    data = dataset[var_list]

    if 'timestamp' in list(dataset):
        datatime = dataset["timestamp"]
    else:
        print('Oops! are you sure you included <timestamp> in the dataframe?, maybe check spelling')
        raise Exception

    dataframe = pp.DataFrame(data.values, datatime=datatime.values, var_names=var_list)
    return dataframe, var_list
