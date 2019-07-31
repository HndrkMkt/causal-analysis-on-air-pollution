import unittest
import pandas as pd
import numpy as np
import datetime
import sys

sys.path.insert(1, '../')
from weather_api import load_csv, pyowm_key, get_weather_stations \
    , get_min_date, fetch_weather_data, forecastio_api_key, create_pandas_df, append_df


class WeatherTest(unittest.TestCase):
    def test_df_length(self):
        data_path = '../../../data/raw/weather/weather_data.csv'
        self.assertEqual(load_csv(data_path).shape[1], 19, "The number of columns of the historical csv should be 19")

    def test_shape_weather_stations(self):
        limit = 10
        latitude = 52.52
        longitude = 13.40
        expected = tuple([limit, 3])
        test = np.array_equal(expected, np.asarray(get_weather_stations(pyowm_key, latitude, longitude, limit)).shape)
        self.assertEqual(test, True, "Size of the list of tuples should match the lenght of limit and 3 columns")

    def test_format_min_date(self):
        data_path = '../../../data/raw/weather/weather_data.csv'
        data = load_csv(data_path)
        min_date = type(get_min_date(data))
        test = min_date is pd._libs.tslibs.timestamps.Timestamp
        self.assertEqual(test, True, "The class of the minimum date should be timestamp")

    def test_shape_weather_blocks(self):
        limit = 10
        latitude = 52.52
        longitude = 13.40
        start_date = datetime.datetime(2019, 5, 17)
        days_back = 2
        weather_stations_list = get_weather_stations(pyowm_key, latitude, longitude, limit)
        weather_data_blocks = fetch_weather_data(weather_stations_list, forecastio_api_key, start_date, days_back)
        expected = tuple([limit * days_back, 5])
        test = np.array_equal(expected, np.asarray(weather_data_blocks).shape)
        self.assertEqual(test, True,
                         "length of the list should match the product of the number of days back times the number of weather stations and 5 columns")

    def test_shape_new_df(self):
        limit = 10
        latitude = 52.52
        longitude = 13.40
        start_date = datetime.datetime(2019, 5, 17)
        days_back = 2
        weather_stations_list = get_weather_stations(pyowm_key, latitude, longitude, limit)
        weather_data_blocks = fetch_weather_data(weather_stations_list, forecastio_api_key, start_date, days_back)
        new_df = create_pandas_df(weather_data_blocks)
        test = new_df.shape[0] > 0 and new_df.shape[1] == 19
        self.assertEqual(test, True, "lenght of the new dataframe should be greater than 0 and have 19 columns")

    def test_shape_appended_df(self):
        data_path = '../../../data/raw/weather/weather_data.csv'
        old_df = load_csv(data_path)
        limit = 10
        latitude = 52.52
        longitude = 13.40
        start_date = datetime.datetime(2019, 5, 17)
        days_back = 2
        weather_stations_list = get_weather_stations(pyowm_key, latitude, longitude, limit)
        weather_data_blocks = fetch_weather_data(weather_stations_list, forecastio_api_key, start_date, days_back)
        new_df = create_pandas_df(weather_data_blocks)
        appended_df = append_df(old_df, new_df)
        test = appended_df.shape[0] > 0 and appended_df.shape[1] == 19
        self.assertEqual(test, True, "lenght of the appended dataframe should be greater than 0 and have 19 columns")


if __name__ == '__main__':
    unittest.main()
