from causal_analysis.discovery import generate_dataframe

from tigramite.independence_tests import ParCorr
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
        name="experiments/causal_discovery/images/time_lagged_correlation.png",
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


if __name__ == "__main__":
    time_lagged_correlation()
