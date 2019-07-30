from causal_analysis.discovery import generate_dataframe, test_alphas

from tigramite.independence_tests import ParCorr


def linear_causal_model():
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


if __name__ == "__main__":
    linear_causal_model()
