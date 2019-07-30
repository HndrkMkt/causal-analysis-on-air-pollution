from causal_discovery.discovery import generate_dataframe, generate_links_from_prior_knowledge, test_alphas

from tigramite.independence_tests import RCOT


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
                    selected_links=generate_links_from_prior_knowledge(var_names, tau_min, tau_max))


if __name__ == "__main__":
    prior_knowledge()
