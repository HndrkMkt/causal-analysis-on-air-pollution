import pandas as pd
from datetime import datetime
import numpy as np
import sys
from operator import itemgetter
from tigramite import data_processing as pp


def feature_list():
    return ["location",
            "lat",
            "lon",
            "timestamp",
            "dayOfYear",
            "minuteOfDay",
            "minuteOfYear",
            "dayOfWeek",
            "isWeekend",
            "pressure_1",
            "altitude",
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
    return ["location", "lat", "lon", "altitude", "pressure_sealevel", "temperature",
            "humidity_1", "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2"]


def time_family():
    return ["timestamp", "dayOfYear", "minuteOfDay", "minuteOfYear", "dayOfWeek", "isWeekend"]


def weather_family():
    return ["apparent_temperature", "cloud_cover", "dew_point", "humidity", "ozone",
            "precip_intensity", "precip_probability", "precip_type", "pressure", "uv_index", "visibility",
            "wind_bearing", "wind_gust", "wind_speed"]


sensor_family = sensor_family()
time_family = time_family()
weather_family = weather_family()


def family_list():
    return [sensor_family, time_family, weather_family]


def load_data(path):
    features = feature_list()
    features.remove("minuteOfYear")
    sensor_data = pd.read_csv(path, sep=";", names=features, true_values=["true"], false_values=["false"])

    sensor_data["timestamp"] = pd.to_datetime(sensor_data["timestamp"])
    sensor_data["minuteOfYear"] = sensor_data["dayOfYear"] * 60 + sensor_data["minuteOfDay"]
    sensor_data["isWeekend"] = sensor_data["isWeekend"].astype(int)

    sensor_data = sensor_data.sort_values(by=["location", "timestamp"])
    return sensor_data


def subset(data, by_family=[], by_columns=[], start_date='', end_date=''):
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
                    #TODO: BETTER EXCEPTION HANDLING
                    raise Exception

        if by_columns:
            for i in by_columns:
                if i in feature_list():
                    final_feature_list.extend([i])
                else:
                    #TODO: BETTER EXCEPTION HANDLING
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
        #TODO: BETTER EXCEPTION HANDLING
        raise Exception

    return localized_data


def input_na(data, columns, method=None, value=None):
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
            #TODO: BETTER EXCEPTION HANDLING
            raise Exception

        no_nulls_list = []
        for j in list(data):
            if x[j].isnull().sum().sum() == 0:
                no_nulls_list.extend([j])

    # x=x.dropna()

    return x


def create_tigramite_dataframe(dataset, exclude):
    must = ['timestamp']
    var_list = list(dataset)
    if all([i in var_list for i in exclude]):
        for i in exclude:
            var_list.remove(i)
    else:
        #TODO: BETTER EXCEPTION HANDLING
        raise Exception

    data = dataset[var_list]

    if 'timestamp' in list(dataset):
        datatime = dataset["timestamp"]
    else:
        #TODO: BETTER EXCEPTION HANDLING
        raise Exception

    dataframe = pp.DataFrame(data.values, datatime=datatime.values, var_names=var_list)
    return dataframe, var_list
