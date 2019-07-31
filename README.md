#  Causal Analysis Pipeline for Air Pollution Data

## Virtual environment
Create a new virtual environment with:
```
python -m venv env
```

Activate virtual environment:
```
source env/bin/activate
```

Install required packages:
```
pip install --upgrade pip
pip install -r requirements.txt
```

Install causal analysis package to the virtual environment:
```
pip install -e .
```

## Additional requirements
We assume a running Flink cluster to submit the workflows to. To fully use the TIGRAMITE package, please follow
the installation instructions for its requirements at [https://github.com/jakobrunge/tigramite](https://github.com/jakobrunge/tigramite).

## Downloading data
The sensor data that the project uses by default is taken from the monthly `.zip` files at [http://archive.luftdaten.info/](http://archive.luftdaten.info/).
To download these files and process them run the following steps:
1. Navigate to the project root
2. Run the script to download monthly `.zip` files for the desired time range
    ```
    $ data_acquisition/luftdaten/load_csv_per_month.sh 2019-01-01 2019-05-01
    ```
3. Change the compression to `.gzip` due to limitations of Apache Flink in handling compressed files:
    ```
    $ data_acquisition/luftdaten/zip2gzip_monthly_files.sh 2019-01-01 2019-05-01
    ```

There is also several other scripts for downloading data from the project root:
 - Download individual csv files:
    ```
    $ data_acquisition/luftdaten/load_csv.sh 2019-01-01 2019-05-01
    ```
- Download monthly parquet files:
    ```
    $ data_acquisition/luftdaten/load_parquet.sh 2019-01-01 2019-05-01
 
    ```

4. Download weather data:
    Due to daily limit API calls constraints, in order to get the weather data used in the project, the following script must be run in 7 different days:
    ```
    $ cd <project_root>/data_acquisition/weather
    $ python weather_api.py
    ```

## Jupyter Lab
Create kernel to use virtual environment:
```
$ ipython kernel install --user --name=causal-air-pollution
```

Run:
```
$ jupyter lab
```

# Running the pipelines
To run the pipeline on the sensor data that you downloaded, execute the following steps:
1. Navigate to `<project_root>/data_processing` and package the application:
    ```
    mvn clean package
    ```
2. Navigate to the `<project_root>` and calculate sensor statistics:
    ```
    $ flink run -c de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows.SensorStatistics data_processing/target/air-pollution-data-processing-1.0-SNAPSHOT.jar --data_dir <project_root>/data
    ```
3. Calculate sensor matchings:
    ```
    $ cd <project_root>/data_acquisition/weather
    $ python subset_sensors.py
    ```
    Walk through `ExtractSensorsForWeatherStations.ipynb`.
4. Filter raw sensor to the sensors for which we have weather data:
    ```
    $ flink run -c de.tuberlin.dima.bdapro.dataIntegration.sensor.workflows.SensorFiltering data_processing/target/air-pollution-data-processing-1.0-SNAPSHOT.jar --data_dir <project_root>/data
    ```
5. Join all the datasets together and create output data for causal analysis:
    ```
    $ flink run -c de.tuberlin.dima.bdapro.advancedProcessing.FeatureTableCombination data_processing/target/air-pollution-data-processing-1.0-SNAPSHOT.jar --data_dir <project_root>/data
    ```
6. Run causal analysis pipeline with activated virtual environment and store standard output to output.log:
    ```
    $ cd <project_root>/notebooks
    $ python causal_discovery.py > output.log
    ```