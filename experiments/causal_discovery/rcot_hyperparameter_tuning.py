from causal_analysis.discovery import generate_dataframe, test_alphas

from tigramite.independence_tests import RCOT


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


if __name__ == "__main__":
    rcot_hyperparameter_tuning()
