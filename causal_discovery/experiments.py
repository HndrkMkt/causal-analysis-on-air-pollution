from causal_discovery.discovery import generate_dataframe, select_links, test_alphas

from tigramite.independence_tests import ParCorr, RCOT
from tigramite.pcmci import PCMCI
from tigramite import plotting as tp


def time_lagged_correlation():
    """Runs the time-lagged correlation analysis experiment.

    This function alculates the time-lagged correlation between the variables for lags of up to 48 hours and plots the
    result as a scatterplot matrix.
    """
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    tau_min = 0
    tau_max = 48
    dataframe, var_list = generate_dataframe(var_names)
    print(f"Variable names: {var_names}")
    ci_test = ParCorr(significance='analytic')
    pcmci = PCMCI(
        dataframe=dataframe,
        cond_ind_test=ci_test,
        verbosity=1)
    correlations = pcmci.get_lagged_dependencies(tau_min=tau_min, tau_max=tau_max)
    lag_func_matrix = tp.plot_lagfuncs(
        name="lagged_dependencies.png",
        val_matrix=correlations,
        setup_args={'var_names': var_names,
                    'figsize': (50, 25),
                    'label_fontsize': 12,
                    'label_space_top': 0.025,
                    'label_space_left': 0.05,
                    'lag_units': 'hours',
                    'x_base': 6,
                    'y_base': .5})
    print(lag_func_matrix)


def linear_model():
    """Runs the linear causal model experiment mentioned in the report.

    This function creates causal linear models for a variety of different alphas and plots the results as
    network and timeseries graphs. The maximum lag is 24.
    """
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
    """Runs the experiment for tuning the hyperparameters of the non-linear causal model using RCOT.

    This function creates causal models using the RCOT test for a variety of different alphas and numbers of random
    Fourier transformations (num_f). The results are plotted as network and timeseries graphs. The maximum lag is 1.
    """
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
    """Runs the second experiment for creating non-linear causal model using RCOT.

    This function creates causal models using the RCOT test for a variety of alphas and small range of random Fourier
    transformations (num_f). The results are plotted as network and timeseries graphs. The maximum lag is 24.
    """
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
    """Runs the experiment for incorporating prior knowledge into the non-linear causal model using RCOT.

    This function creates causal models using the RCOT test for a variety of alphas and small range of random Fourier
    transformations (num_f). It further limits the solution space by limiting the selected_links used in the PCMCI
    algorithm, which effectively enforced independencies in the result. The results are plotted as network and
    timeseries graphs. The maximum lag is 1.
    """
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
    linear_model()
