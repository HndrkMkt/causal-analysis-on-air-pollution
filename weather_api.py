import pyowm
import pyowm
import forecastio
from datetime import datetime
from datetime import timedelta
import pandas as pd

#Days we want to go back with the data
days_back=118

#Load the file that contains historical hourly weather data for 5 stations in Berlin
old_data=pd.read_csv('~/weather_data.csv')
old_data.set_index(['location','time'])
old_data['time'] = pd.to_datetime(weather_data['time'], format='%Y-%m-%d %H:%M:%S')

#Get the minimum date in the file
min_date=old_data['time'].min()
start_date=min_date

#Credetianls
forecastio_api_key = "677e17ccb348e07d52486ae3857a2d84"
owm = pyowm.OWM('48dae982f9e685eee268e90dafba5041') 

#Getting the 5 closer points to Berlin Mitte
obs_list = owm.weather_around_coords(52.52,13.40,limit=5)

#Create iterable that contains name longitude and latitude for the 5 locations
lon_lat_list=[]
for i,location in enumerate(obs_list):
    l = obs_list[i].get_location()
    lon_lat_list.append([l.get_name(),l.get_lon(),l.get_lat()])

#Pulls the data and create list of data_block_list.Each data block contains daily,hourly data points with the weather features as attributes 
data_block_list=[]
for d in range(days_back):
    for i,location in enumerate(lon_lat_list):
        forecast = forecastio.load_forecast(forecastio_api_key, lon_lat_list[i][2], lon_lat_list[i][1],time=start_date-timedelta(days=d))
        data_block_list.append([forecast.hourly(),start_date-timedelta(days=d),lon_lat_list[i][0],lon_lat_list[i][1],lon_lat_list[i][2]])

#Get the variables of interest for each hour in the time frame. Exceptions need to be handled due to missing observations for some points in time
tuples=[]
for i,data_block in enumerate(data_block_list):
    for j,hourlyData in enumerate(data_block_list[i][0].data):
        t=[]
        t.extend([data_block_list[i][2],
                       data_block_list[i][3],
                       data_block_list[i][4],
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

#Formatting the dataframe                    
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

#Appending new tuples to historical file
weather_data=old_data.append(df,ignore_index=True)
weather_data.set_index(['location','time'])

#Droping duplicates if any
weather_data=weather_data.drop_duplicates(keep=False,inplace=True)

#Saving csv with old and new tuples
weather_data.to_csv(path_or_buf='~/weather_data.csv',index=False)

print('success!')