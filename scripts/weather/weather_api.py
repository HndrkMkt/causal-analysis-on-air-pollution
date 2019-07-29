import pyowm
import pyowm
import forecastio
from datetime import datetime
from datetime import timedelta
import pandas as pd
import os.path
from os import path


days_back=118
latitude=52.52
longitude=13.40
forecastio_api_key = '500c5418e313c7fcb7ac6dc5ba73cdab'
pyowm_key = '48dae982f9e685eee268e90dafba5041'
data_path = '../../data/raw/weather/weather_data.csv'


def load_csv(data_path):
    '''Function to load an existing csv with historical weather data. If such file does not exist a new one is created in the specified path
    Args:
        data_path: The path where the file exists. If the file does not exist, then the path where the new csv file will be created

    Returns: If the historical csv file exists it returns a pandas dataframe with the weather fields and the corresponding values.
             If the historical csv does not exists it returns an empty pandas dataframe with the corresponding columns.

    '''
    if path.exists(data_path):
        old_data=pd.read_csv(data_path,sep=';')
    else:
        old_data=pd.DataFrame(columns = ['location' , 'longitude', 'latitude','time','temperature',
                                     'apparent_temperature',
                                     'cloud_cover',
                                     'dew_point',
                                     'humidity',
                                     'ozone',
                                     'precip_intensity',
                                     'precip_probability',
                                     'precip_type',
                                     'pressure',
                                     'uv_index',
                                     'visibility',
                                     'wind_bearing',
                                     'wind_gust',
                                     'wind_speed'])
        old_data.to_csv(path_or_buf=data_path, index=False, sep=';')

    old_data.set_index(['location','time'])
    old_data['time'] = pd.to_datetime(old_data['time'], format='%Y-%m-%d %H:%M:%S')

    return old_data

def get_weather_stations(pyowm_key,latitude,longitude,limit):
    ''' This function get the closest weather stations for an especified location, with the help of the pyowm wrapper.

    Args:
        pyowm_key: A secret key for the pyowm wrapper
        latitude: The latitud of the desired place to retrieve the closest weather stations
        longitude: The longitud of the desired place to retrieve the closest weather stations
        limit: The number of closest weather stations to include in the list

    Returns: A list of tuples composed by the name of the weather stations and its latitude and longitude

    '''

    owm = pyowm.OWM(pyowm_key)
    obs_list = owm.weather_around_coords(latitude,longitude,limit=limit)
    lon_lat_list=[]
    for i,location in enumerate(obs_list):
        l = obs_list[i].get_location()
        lon_lat_list.append([l.get_name(),l.get_lat(),l.get_lon()])

    return lon_lat_list

def get_min_date(old_data):
    ''' This function gets the oldest date in the historical data. If the historical data does not exist, the current date is returned.
    Args:
        old_data: A pandas dataframe containing the historical weather data

    Returns:

    '''
    if old_data['time'].min():
        min_date=old_data['time'].min()
    else:
        min_date = datetime.now()

    return min_date

def fetch_weather_data(weather_stations_list,forecastio_api_key,start_date,days_back):
    ''' This function fetchs weather data through a foracstio forecast object for the requested weather stations.
    Args:
        weather_stations_list: A list containing tuples composed by the weather station name, its latituded, its longitude
        forecastio_api_key: A secret key for the forecastio wrapper.
        start_date: The start date from where weather data will start being pulled
        days_back: The amount of days to look back to pull weather data

    Returns: A list of tuples composed by a forecastio forecast object, the date, the name of the weather station, the latitude and
            longitude of the weather station

    '''
    data_block_list=[]
    for d in range(days_back):
        for i,location in enumerate(weather_stations_list):
            forecast = forecastio.load_forecast(forecastio_api_key, weather_stations_list[i][1], weather_stations_list[i][2],time=start_date-timedelta(days=d))
            data_block_list.append([forecast.hourly(),start_date-timedelta(days=d),weather_stations_list[i][0],weather_stations_list[i][1],weather_stations_list[i][2]])

    return data_block_list

def create_pandas_df(data_block_list):
    ''' This function creates a pandas dataframe from the forecastio forecast objects for the different required weather stations
    Args:
        data_block_list: A list of tuples composed by a forecastio forecast object, the date, the name of the weather station, the latitude and
            longitude of the weather station

    Returns: A pandas dataframe containing the weather variables for the requested weather stations

    '''
    tuples=[]
    for i,data_block in enumerate(data_block_list):
        for j,hourlyData in enumerate(data_block_list[i][0].data):
            t=[]
            t.extend([data_block_list[i][2],
                           data_block_list[i][4],
                           data_block_list[i][3],
                           data_block_list[i][0].data[j].time])
            try:
                t.extend([data_block_list[i][0].data[j].temperature])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].apparentTemperature])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].cloudCover])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].dewPoint])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].humidity])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].ozone])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].precipIntensity])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].precipProbability])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].precipType])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].pressure])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].uvIndex])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].visibility])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].windBearing])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].windGust])
            except:
                t.extend([None])
            try:
                t.extend([data_block_list[i][0].data[j].windSpeed])
            except:
                t.extend([None])
            
            tuples.append(t)
                   
    df=pd.DataFrame(tuples,columns = ['location' , 'longitude', 'latitude','time','temperature',
                                     'apparent_temperature',
                                     'cloud_cover',
                                     'dew_point',
                                     'humidity',
                                     'ozone',
                                     'precip_intensity',
                                     'precip_probability',
                                     'precip_type',
                                     'pressure',
                                     'uv_index',
                                     'visibility',
                                     'wind_bearing',
                                     'wind_gust',
                                     'wind_speed'])

    df.set_index(['location','time'])

    return df

def append_df(old_data,new_data):
    ''' This function appends the new data to the old dataframe containing the historical weather measures. If there is not an historical
        dataframe then it creates a new one.
    Args:
        old_data: The old pandas dataframe containing the historical weather measures
        new_data: The new pandas dataframe containing the new weather measures

    Returns: A pandas dataframe containing the union of the old and new dataframes

    '''
    weather_data=old_data.append(new_data,ignore_index=True)
    weather_data.set_index(['location','time'])
    weather_data.sort_values(by=['location','time']).drop_duplicates(subset=['location', 'time'],inplace=True)

    return weather_data

def save_df(df,data_path):
    ''' This function saves a pandas dataframe into a specific path
    Args:
        df: A pandas dataframe
        data_path: A path where to save the dataframe

    '''
    df.to_csv(path_or_buf=data_path,index=False,sep=';')

old_data = load_csv(data_path)
weather_list = get_weather_stations(pyowm_key,latitude,longitude,limit=5)
start_date = get_min_date(old_data)
weather_data = fetch_weather_data(weather_list,forecastio_api_key,start_date,days_back)
pandas_df = create_pandas_df(weather_data)
new_df = append_df(old_data,pandas_df)

save_df(new_df,data_path)

print('success!')
