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

## Downloading data
Navigate to the project root and run
 - to download individual csv files
```
scripts/luftdaten/load_csv.sh 2019-01-01 2019-05-01
```
- to download monthly parquet files
```
scripts/luftdaten/load_parquet.sh 2019-01-01 2019-05-01
```

## Jupyter Lab
Create kernel to use virtual environment:
```
ipython kernel install --user --name=causal-air-pollution
```

Run:
```
jupyter lab
```