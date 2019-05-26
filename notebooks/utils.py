import pyowm
import forecastio
from datetime import datetime
from datetime import timedelta
import datetime as dt
import pandas as pd
import numpy as np
import glob
import pyarrow.parquet as pq

def get_closest_weather_station(latitude,longitude,credentials): 
	return credentials.weather_around_coords(latitude,longitude,limit=1)

def get_weather_variables_for_locations(list_of_locations,start_date,days_back,credentials):
	start_date=dt.datetime.strptime(str(start_date+' 23:00:0'), '%Y-%m-%d %H:%M:%S')
	data_block_list=[]
	for d in range(days_back):
	    for location in list_of_locations:
	        forecast = forecastio.load_forecast(credentials, location[6], location[5],time=start_date-timedelta(days=d))
	        data_block_list.append([forecast.hourly(),start_date-timedelta(days=d),location[0],location[1],location[2]])

	#Get the variables of interest for each hour in the time frame. Exceptions need to be handled due to missing observations for some points in time
	tuples=[]
	for data_block in data_block_list:
	    for hourlyData in data_block[0].data:
	        t=[]
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

def fetch_sensor_data(list_of_sensors,sensors_of_interest,start_date,end_date):
    sensors=list_of_sensors[np.where(np.in1d(list_of_sensors[:,3],sensors_of_interest))][:,0]
    sd=dt.datetime.strptime(str(start_date+' 01:00:0'), '%Y-%m-%d %H:%M:%S')
    ed=dt.datetime.strptime(str(end_date+' 23:00:0'), '%Y-%m-%d %H:%M:%S')
    now = dt.datetime.now()
    
    parquet_files=[]
    berlin_data = []
    for y in range(sd.year,ed.year+1):
        if ed.year-sd.year==0:
            if y==now.year:
                for m in range(sd.month,ed.month+1):
                    if m<10:
                        file=glob.glob(f'../data/raw/parquet/{y}-0{m}/sds011/*.parquet')
                        file=file[0]
                        parquet_files=pq.ParquetFile(file)
                        for i in range(parquet_files.num_row_groups):
                            current_data = parquet_files.read_row_group(i).to_pandas()
                            berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
                    else:
                        file=glob.glob(f'../data/raw/parquet/{y}-{m}/sds011/*.parquet')
                        file=file[0]
                        parquet_files=pq.ParquetFile(file)
                        for i in range(parquet_files.num_row_groups):
                            current_data = parquet_files.read_row_group(i).to_pandas()
                            berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
            else:
                for m in range(sd.month,ed.month+1):
                    if m<10:
                        file=glob.glob(f'../data/raw/parquet/{y}-0{m}/sds011/*.parquet')
                        file=file[0]
                        parquet_files=pq.ParquetFile(file)
                        for i in range(parquet_files.num_row_groups):
                            current_data = parquet_files.read_row_group(i).to_pandas()
                            berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
                    else:
                        file=glob.glob(f'../data/raw/parquet/{y}-{m}/sds011/*.parquet')
                        file=file[0]
                        parquet_files=pq.ParquetFile(file)
                        for i in range(parquet_files.num_row_groups):
                            current_data = parquet_files.read_row_group(i).to_pandas()
                            berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
        elif y==sd.year:
            for m in range(sd.month,13):
                if m<10:
                    file=glob.glob(f'../data/raw/parquet/{y}-0{m}/sds011/*.parquet')
                    file=file[0]
                    parquet_files=pq.ParquetFile(file)
                    for i in range(parquet_files.num_row_groups):
                        current_data = parquet_files.read_row_group(i).to_pandas()
                        berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
                else:
                    file=glob.glob(f'../data/raw/parquet/{y}-{m}/sds011/*.parquet')
                    file=file[0]
                    parquet_files=pq.ParquetFile(file)
                    for i in range(parquet_files.num_row_groups):
                        current_data = parquet_files.read_row_group(i).to_pandas()
                        berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
                
        elif y!=ed.year:
            for m in range(1,13):
                if m<10:
                    file=glob.glob(f'../data/raw/parquet/{y}-0{m}/sds011/*.parquet')
                    file=file[0]
                    parquet_files=pq.ParquetFile(file)
                    for i in range(parquet_files.num_row_groups):
                        current_data = parquet_files.read_row_group(i).to_pandas()
                        berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
                else:
                    file=glob.glob(f'../data/raw/parquet/{y}-{m}/sds011/*.parquet')
                    file=file[0]
                    parquet_files=pq.ParquetFile(file)
                    for i in range(parquet_files.num_row_groups):
                        current_data = parquet_files.read_row_group(i).to_pandas()
                        berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
        elif y==ed.year:
            for m in range(1,ed.month+1):
                if m<10:
                    file=glob.glob(f'../data/raw/parquet/{y}-0{m}/sds011/*.parquet')
                    file=file[0]
                    parquet_files=pq.ParquetFile(file)
                    for i in range(parquet_files.num_row_groups):
                        current_data = parquet_files.read_row_group(i).to_pandas()
                        berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
                else:
                    file=glob.glob(f'../data/raw/parquet/{y}-{m}/sds011/*.parquet')
                    file=file[0]
                    parquet_files=pq.ParquetFile(file)
                    for i in range(parquet_files.num_row_groups):
                        current_data = parquet_files.read_row_group(i).to_pandas()
                        berlin_data.extend([current_data[current_data['sensor_id'].isin(sensors)]])
        
    berlin_data=pd.concat(berlin_data)
                
    return berlin_data

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def make_agg_df(df):
    df['date']=pd.to_datetime(df.timestamp).dt.date
    df['hour']=pd.to_datetime(df.timestamp).dt.hour
    
    agg_df=df.groupby(
    ['sensor_id', 'date','hour'],as_index=False
    ).agg(
        {
            # find the min, max of the P1 column
            'P1': [min,percentile(25),'mean',percentile(75), max],
             # find the min, max of the P2 column
            'P2': [min,percentile(25),'mean',percentile(75), max],
        }
    )
    
    return agg_df
