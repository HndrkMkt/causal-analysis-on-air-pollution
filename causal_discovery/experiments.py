from causal_discovery.discovery import generate_dataframe, select_links, test_alphas

from tigramite.independence_tests import ParCorr, RCOT
from tigramite.pcmci import PCMCI
from tigramite import plotting as tp

def lagged_dependencies():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    tau_min = 0
    tau_max = 24
    dataframe, var_list = generate_dataframe(var_names)
    print(f"Variable names: {var_names}")
    ci_test = ParCorr(significance='analytic')
    pcmci= PCMCI(
        dataframe=dataframe,
        cond_ind_test=ci_test,
        verbosity=1)
    correlations = pcmci.get_lagged_dependencies(tau_min=tau_min, tau_max=tau_max)
    lag_func_matrix = tp.plot_lagfuncs(val_matrix=correlations, setup_args={'var_names':var_names, 'x_base':5, 'y_base':.5})
    print(lag_func_matrix)

def linear_model():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    tau_min = 0
    tau_max = 24
    dataframe, var_list = generate_dataframe(var_names)
    print(f"Variable names: {var_names}")
    ci_test = ParCorr(significance='analytic')
    alphas = [3 ** -n for n in range(1, 9)]
    test_alphas(dataframe, ci_test, alphas, var_names, tau_min=tau_min, tau_max=tau_max)


def rcot_hyperparameter_tuning():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    tau_min = 0
    tau_max = 1
    dataframe, var_list = generate_dataframe(var_names, start_index=0, end_index=2000)
    print(f"Variable names: {var_names}")
    num_fs = [2 ** n for n in range(1, 11)]
    for num_f in num_fs:
        ci_test = RCOT(significance='analytic', num_f=num_f)
        test_alphas(dataframe, ci_test, [0.05, 0.1, 0.2], var_names, tau_min=tau_min, tau_max=tau_max)


def rcot_causal_model():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    tau_min = 0
    tau_max = 24
    dataframe, var_list = generate_dataframe(var_names, start_index=0, end_index=2000)
    print(f"Variable names: {var_names}")
    num_fs = [2 ** 9, 2 ** 10]
    for num_f in num_fs:
        ci_test = RCOT(significance='analytic', num_f=num_f)
        test_alphas(dataframe, ci_test, [0.05, 0.1, 0.2], var_names, tau_min=tau_min, tau_max=tau_max)


def prior_knowledge():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    tau_min = 0
    tau_max = 24

    dataframe, var_list = generate_dataframe(var_names, start_index=0, end_index=2000)
    print(f"Variable names: {var_names}")
    num_fs = [2 ** 9, 2 ** 10]
    for num_f in num_fs:
        ci_test = RCOT(significance='analytic', num_f=num_f)
        test_alphas(dataframe, ci_test, [0.05, 0.1, 0.2], var_names, tau_min=tau_min, tau_max=tau_max,
                    selected_links=select_links(var_names, tau_min, tau_max))


if __name__ == "__main__":
    prior_knowledge()
