import pandas as pd
from tigramite import data_processing as pp
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb

var_names = ["dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "temperature", "humidity", "p1", "p2"]


def create_dataframe(var_names):
    sensor_data = pd.read_csv("../data/processed/output.csv", sep=";", names=[
        "location", "lat", "lon", "timestamp", "dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "pressure",
        "altitude", "pressure_sealevel", "temperature",
        "humidity", "p1", "p2", "p0", "durP1", "ratioP1", "durP2", "ratioP2"], true_values=["true"],
                              false_values=["false"])

    sensor_data["timestamp"] = pd.to_datetime(sensor_data["timestamp"])
    sensor_data["isWeekend"] = sensor_data["isWeekend"].astype(int)

    sensor_data = sensor_data.sort_values(by=["location", "timestamp"])
    filtered_sensor_data = sensor_data[
        ["dayOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "temperature", "humidity", "p1", "p2"] + ["location",
                                                                                                         "timestamp"]]
    filtered_sensor_data = filtered_sensor_data.dropna()

    localized_data = filtered_sensor_data[filtered_sensor_data["location"] == 716]
    localized_data = localized_data.head(2000)
    data = localized_data[var_names]
    datatime = localized_data["timestamp"]

    dataframe = pp.DataFrame(data.values, datatime=datatime.values, var_names=var_names)
    return dataframe


dataframe = create_dataframe(var_names)
# parcorr = ParCorr(significance='analytic')
cmi_knn = CMIknn(significance='shuffle_test', knn=0.1, shuffle_neighbors=5)
pcmci = PCMCI(
    dataframe=dataframe,
    cond_ind_test=cmi_knn,
    verbosity=2)

# correlations = pcmci.get_lagged_dependencies(tau_max=20)

# lag_func_matrix = tp.plot_lagfuncs(val_matrix=correlations, setup_args={'var_names':var_names,
#                                                                         'x_base':5, 'y_base':.5})
results = pcmci.run_pcmci(tau_max=2, pc_alpha=0.05)

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
