import pandas as pd
from tigramite import data_processing as pp


def load_data():
    sensor_data = pd.read_csv("../data/processed/causalDiscoveryData.csv", sep=";", names=[
        "location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "pressure",
        "altitude", "pressure_sealevel", "temperature",
        "humidity_sensor", "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2",
        "apparent_temperature", "cloud_cover", "dew_point", "humidity_weather", "ozone", "precip_intensity",
        "precip_probability",
        "precip_type", "pressure", "uv_index", "visibility", "wind_bearing", "wind_gust", "wind_speed"],
                              true_values=["true"], false_values=["false"])

    sensor_data.loc[:, "timestamp"] = pd.to_datetime(sensor_data["timestamp"])
    sensor_data.loc[:, "isWeekend"] = sensor_data["isWeekend"].astype(int)

    sensor_data = sensor_data.sort_values(by=["location", "timestamp"])
    return sensor_data


def extract_dataset():
    sensor_data = load_data()
    sensor_data = sensor_data.loc[:,
                  ["location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend",
                   "temperature",
                   "humidity_sensor", "p1", "p2", "apparent_temperature", "cloud_cover", "dew_point",
                   "humidity_weather", "precip_intensity", "precip_probability",
                   "visibility", "wind_bearing", "wind_gust", "wind_speed"]]
    sensor716 = sensor_data.loc[sensor_data["location"] == 716]
    sensor716["minuteOfYear"] = sensor716["dayOfYear"] * 60 + sensor716["minuteOfDay"]
    sensor716.loc[sensor716["wind_gust"].isna(), "wind_gust"] = sensor716[sensor716["wind_gust"].isna()]["wind_speed"]
    sensor716.loc[sensor716["wind_bearing"].isna(), "wind_bearing"] = 0.
    sensor716.loc[sensor716["cloud_cover"].isna(), "cloud_cover"] = 0.
    sensor716.loc[sensor716["precip_intensity"].isna(), "precip_intensity"] = 0.
    sensor716.loc[sensor716["precip_probability"].isna(), "precip_probability"] = 0.
    sensor716 = sensor716.fillna(-999)
    return sensor716


def create_tigramite_dataframe(var_names):
    dataset = extract_dataset()
    data = dataset.loc[:, var_names]
    datatime = dataset.loc[:, "timestamp"]

    dataframe = pp.DataFrame(data.values, datatime=datatime.values, var_names=var_names, missing_flag=-999)
    return dataframe


def generate_dataframe(var_names):
    return create_tigramite_dataframe(var_names)
