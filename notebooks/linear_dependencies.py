import pandas as pd
from tigramite import data_processing as pp
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb, RCOT

def load_data():
    sensor_data = pd.read_csv("../data/processed/causalDiscoveryData.csv", sep=";", names=[
        "location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "pressure", "altitude", "pressure_sealevel", "temperature",
        "humidity_sensor", "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2",
        "apparent_temperature", "cloud_cover", "dew_point", "humidity_weather", "ozone", "precip_intensity", "precip_probability",
        "precip_type", "pressure", "uv_index", "visibility", "wind_bearing", "wind_gust", "wind_speed"], true_values=["true"], false_values=["false"])

    sensor_data.loc[:, "timestamp"] = pd.to_datetime(sensor_data["timestamp"])
    sensor_data.loc[:, "isWeekend"] = sensor_data["isWeekend"].astype(int)

    sensor_data = sensor_data.sort_values(by=["location", "timestamp"])
    return sensor_data

def extract_dataset(sensor_data):
    sensor_data = sensor_data.loc[:, ["location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "temperature",
                               "humidity_sensor", "p1", "p2", "apparent_temperature", "cloud_cover", "dew_point", "humidity_weather", "precip_intensity", "precip_probability",
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

# def var_names():
#     return ["dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "temperature",
##            "humidity_sensor", "p1", "p2", "apparent_temperature", "cloud_cover", "dew_point", "humidity_weather", "precip_intensity", "precip_probability",
  ##          "visibility", "wind_bearing", "wind_gust", "wind_speed"]

var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature", "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]

def create_tigramite_dataframe(dataset):
    data = dataset.loc[:, var_names]
    datatime = dataset.loc[:, "timestamp"]

    dataframe = pp.DataFrame(data.values, datatime = datatime.values, var_names=var_names, missing_flag=-999)
    return dataframe

dataframe = create_tigramite_dataframe(extract_dataset(load_data()))
parcorr = ParCorr(significance='analytic')
# cmi_knn = CMIknn(significance='shuffle_test', knn=0.1, shuffle_neighbors=5)
# pcmci = PCMCI(
#     dataframe=dataframe,
#     cond_ind_test=parcorr,
#     verbosity=1)

rcot = RCOT(significance="analytic", num_f=500)
gpdc = GPDC(significance='analytic', gp_params=None)
# gpdc.generate_and_save_nulldists(sample_sizes=range(495, 501),
#     null_dist_filename='dc_nulldists.npz')
# gpdc.null_dist_filename ='dc_nulldists.npz'

print(f"Variable names: {var_names}")

pcmci= PCMCI(
    dataframe=dataframe,
    cond_ind_test=rcot,
    verbosity=1)

# correlations = pcmci.get_lagged_dependencies(tau_max=2)

# lag_func_matrix = tp.plot_lagfuncs(val_matrix=correlations, setup_args={'var_names':var_names,
#                                                                         'x_base':5, 'y_base':.5})
import time
start = time.time()
results = pcmci.run_pcmci(tau_min=0, tau_max=1, pc_alpha=0.001, fdr_method='fdr_bh')
end = time.time()

print(f"Execution time: {round(end - start, 2)} seconds")

import pickle
with open('results.pickle', 'wb') as f:
    # Pickle the 'data' dictionary using the highest protocol available.
    pickle.dump(results, f, pickle.HIGHEST_PROTOCOL)

q_matrix = pcmci.get_corrected_pvalues(p_matrix=results['p_matrix'], fdr_method='fdr_bh')
pcmci.print_significant_links(
    p_matrix=results['p_matrix'],
    q_matrix=q_matrix,
    val_matrix=results['val_matrix'],
    alpha_level=0.01)

link_matrix = pcmci.return_significant_parents(pq_matrix=q_matrix,
                                               val_matrix=results['val_matrix'], alpha_level=0.01)['link_matrix']

tp.plot_graph(
    val_matrix=results['val_matrix'],
    link_matrix=link_matrix,
    var_names=var_names,
    link_colorbar_label='cross-MCI',
    node_colorbar_label='auto-MCI',
    figsize=(20, 20),
    save_name="graph"
)

# Plot time series graph
tp.plot_time_series_graph(
    val_matrix=results['val_matrix'],
    link_matrix=link_matrix,
    var_names=var_names,
    link_colorbar_label='MCI',
    figsize=(20, 20),
    save_name="time_series_graph"
)
