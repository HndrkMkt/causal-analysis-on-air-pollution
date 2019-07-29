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

Install causal analysis package:
```
pip install -e .
```

## Downloading data
The sensor data that the project uses by default is taken from the monthly `.zip` files on [http://archive.luftdaten.info/](http://archive.luftdaten.info/).
To download these files and process them run the following steps:
1. Navigate to the project root
2. Run the script to download monthly `.zip` files for the desired time range
    ```
    $ scripts/luftdaten/load_csv_per_month.sh 2019-01-01 2019-05-01
    ```
3. Change the compression to `.gzip` due to limitations of Apache Flink in handling compressed files:
    ```
    $ scripts/luftdaten/zip2gzip_monthly_files.sh 2019-01-01 2019-05-01
    ```

There is also several other scripts for downloading data from the project root:
 - Download individual csv files:
    ```
    $ scripts/luftdaten/load_csv.sh 2019-01-01 2019-05-01
    ```
- Download monthly parquet files:
    ```
    $ scripts/luftdaten/load_parquet.sh 2019-01-01 2019-05-01
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
1. Navigate to `<project_root>/air_pollution` and package the application:
    ```
    mvn clean package
    ```
2. Navigate to the `<project_root>` and calculate sensor statistics:
    ```
    $ flink run -c de.tuberlin.dima.bdapro.jobs.SensorStatistics air-pollution/target/air-pollution-1.0-SNAPSHOT.jar --data_dir <project_root>/data
    ```
3. Download weather data:
    Due to daily limit API calls constraints, in order to get the weather data used in the project, the following script must be run in 7 different days:
    ```
    $ cd <project_root>/scripts/weather
    $ python weather_api.py
    ```
4. Calculate sensor matchings:
    ```
    $ cd <project_root>/notebooks
    $ jupyter lab
    ```
    Walk through `ExtractSensorsForWeatherStations.ipynb`.
5. Filter raw sensor to the sensors for which we have weather data:
    ```
    $ flink run -c de.tuberlin.dima.bdapro.jobs.Filtering air-pollution/target/air-pollution-1.0-SNAPSHOT.jar --data_dir <project_root>/data
    ```
6. Join all the datasets together and create output data for causal analysis:
    ```
    $ flink run -c de.tuberlin.dima.bdapro.jobs.Joining air-pollution/target/air-pollution-1.0-SNAPSHOT.jar --data_dir <project_root>/data
    ```
7. Run causal analysis pipeline with activated virtual environment and store standard output to output.log:
    ```
    $ cd <project_root>/notebooks
    $ python causal_discovery.py > output.log
    ```