"""Helper functions for running causal discovery experiments.
"""

__author__ = 'Hendrik Makait'

from causal_analysis.data_preparation import load_data, subset, localize, input_na, create_tigramite_dataframe
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import RCOT

import time


def generate_dataframe(var_names, start_index=None, end_index=None):
    """Generates a TIGRAMITE dataframe from the intermediate data.

    This function loads the intermediate data from disk, preprocesses it and subsets it to use only a single sensor
     location. It then creates a TIGRAMITE dataframe from the resulting data that includes the given columns. If either
    a start or an end index are given, it uses the subset of the preprocessed data defined by the given range.

    Args:
        var_names: The names of the variables to use in the dataframe.
        start_index: The start index of the row range to use.
        end_index: The end index of the row range to use (excluded).

    Returns:

    """
    data = load_data('data/processed/causalDiscoveryData.csv')
    data = localize(data, 52.496, 13.338, 1)
    data = input_na(data, columns=["temperature", "humidity", "p1", "p2", "apparent_temperature", "cloud_cover",
                                   "dew_point", "humidity", "visibility", "wind_bearing", "wind_gust", "wind_speed",
                                   "uv_index"], method='ffill')
    data = input_na(data, columns=["precip_intensity", "precip_probability"], value=0)
    data = subset(data, by_columns=['timestamp', 'location'] + var_names)
    data = input_na(data, columns=var_names, value=-999)
    if start_index:
        if end_index:
            data = data.iloc[start_index:end_index]
        else:
            data = data.iloc[start_index:]
    elif end_index:
        data = data.iloc[:end_index]
    dataframe = create_tigramite_dataframe(data, exclude=['timestamp', 'location'])
    return dataframe


def test_alphas(dataframe, cond_ind_test, alphas, var_names, tau_min=0, tau_max=1, selected_links=None):
    """Executes the PCMCI algorithm over a list of different alphas and plots the results.

    Args:
        dataframe: The TIGRAMITE dataframe to use.
        cond_ind_test: The conditional independence test to use.
        alphas: The list of individual alphas.
        var_names: The names of the variables contained in the dataframe.
        tau_min: The minimum lag.
        tau_max: The maximum lag.
        selected_links: Dictionalry specifying whether only selected links should be tested.
    """
    pcmci = PCMCI(
        dataframe=dataframe,
        cond_ind_test=cond_ind_test,
        verbosity=1)
    for pc_alpha in alphas:
        run_experiment(pcmci, cond_ind_test, pc_alpha, tau_min, tau_max, var_names, selected_links)


def run_experiment(pcmci, cond_ind_test, pc_alpha, tau_min, tau_max, var_names, selected_links):
    """Runs the PCMCI algorithm with the specified input and generates plots for the results.

    Args:
        pcmci: The PCMCI object to use.
        cond_ind_test: The conditional independence test used.
        pc_alpha: Alpha threshold for significance of links.
        tau_min: The minimum lag.
        tau_max: The maximum lag.
        var_names: The names of the variables in the data.
        selected_links: The links to investigate in the model. Investigates all links if none given.
    """
    start = time.time()
    if selected_links:
        lagged_links = {key: [tpl for tpl in val if tpl[1] < 0] for key, val in selected_links.items()}
        all_parents = pcmci.run_pc_stable(
            tau_min=tau_min,
            tau_max=tau_max,
            pc_alpha=pc_alpha,
            selected_links=lagged_links
        )

        results = pcmci.run_mci(
            selected_links=selected_links,
            tau_min=tau_min,
            tau_max=tau_max,
            parents=all_parents,
        )
    else:
        results = pcmci.run_pcmci(
            tau_min=tau_min,
            tau_max=tau_max,
            pc_alpha=pc_alpha,
            fdr_method='fdr_bh',
            selected_links=selected_links)

    end = time.time()

    class_str = ""
    if isinstance(cond_ind_test, RCOT):
        class_str = f"{cond_ind_test.measure}(num_f={cond_ind_test.num_f})"
    else:
        class_str = cond_ind_test.measure
    print(f"Test: {class_str}\nAlpha: {pc_alpha}\n"
          f"Min. Tau: {tau_min}\nMax. Tau: {tau_max}\n"
          f"Execution Time: {round(end - start, 2)} seconds\n")

    plot_results(pcmci, results, cond_ind_test, pc_alpha, tau_min, tau_max, var_names)


def plot_results(pcmci, results, cond_ind_test, pc_alpha, tau_min, tau_max, var_names):
    """Generates a network and a timeseries plot for the given results of the PCMCI algorithm and saves them as files.

    Args:
        pcmci: The PCMCI object used.
        results: The results of the PCMCI algorithm.
        cond_ind_test: The conditional independence test used.
        pc_alpha: Alpha threshold for significance of links.
        tau_min: The minimum lag.
        tau_max: The maximum lag.
        var_names: The names of the variables in the data.
    """
    base_path = "experiments/causal_discovery/results/"
    q_matrix = pcmci.get_corrected_pvalues(p_matrix=results['p_matrix'], fdr_method='fdr_bh')

    link_matrix = pcmci.return_significant_parents(
        pq_matrix=q_matrix,
        val_matrix=results['val_matrix'],
        alpha_level=0.01)['link_matrix']
    class_prefix = ""
    if isinstance(cond_ind_test, RCOT):
        class_prefix = f"{cond_ind_test.measure}_num_f_{cond_ind_test.num_f}"
    else:
        class_prefix = cond_ind_test.measure
    file_name_prefix = f"{class_prefix}_alpha_{pc_alpha:.5f}_tau_{tau_min}to{tau_max}"

    tp.plot_graph(
        val_matrix=results['val_matrix'],
        link_matrix=link_matrix,
        var_names=var_names,
        link_colorbar_label='cross-MCI',
        node_colorbar_label='auto-MCI',
        figsize=(20, 20),
        save_name= base_path + file_name_prefix + "_graph.png"
    )

    tp.plot_time_series_graph(
        val_matrix=results['val_matrix'],
        link_matrix=link_matrix,
        var_names=var_names,
        link_colorbar_label='MCI',
        figsize=(20, 20),
        save_name= base_path + file_name_prefix + "_time_series_graph.png"
    )


def generate_links_from_prior_knowledge(var_names, tau_min, tau_max):
    """Generates the link dictionary from prior knowledge.

    This function creates the dictionary of the possible dependencies in the causal model used by the PCMCI algorithm
    and only includes those links where dependencies may exist according to our domain knowledge. Specifically, it
    removes all incoming links into time-based variables except for those specifying their functional dependencies.
    Time dimensions also can only influence other variables with a lag of 0.

    Args:
        var_names: The names of the variables used in the data.
        tau_min: The minimum lag.
        tau_max: The maximum lag.

    Returns: Dictionary specifying the selected links for the PCMCI algorithm.

    """
    if tau_min > 0:
        raise ValueError(f"tau_min must be 0 to incorporate prior knowledge, is {tau_min}")
    if tau_max < 1:
        raise ValueError(f"tau_min must be at least 1 to incorporate prior knowledge, is {tau_max}")

    name_indices = {name: index for index, name in enumerate(var_names)}
    selected_links = {}
    prior_knowledge = {
        "dayOfYear": [("minuteOfYear", 0)],
        "minuteOfYear": [("minuteOfYear", -1)],
        "minuteOfDay": [("minuteOfYear", 0)],
        "dayOfWeek": [("minuteOfDay", 0), ("dayOfWeek", -1)],
        "isWeekend": [("dayOfWeek", 0)]
    }

    for key, deps in prior_knowledge.items():
        index = name_indices.get(key, None)
        if index is None:
            continue
        links = selected_links.get(index)
        if not links:
            links = []
            selected_links[index] = links
        for dep in deps:
            dep_index = name_indices.get(dep[0], None)
            if dep_index is None:
                raise ValueError(f"{dep[0]} must be in var_names to incorporate prior knowledge for {key}.")
            links.append((dep_index, dep[1]))

    for i, var_name in enumerate(var_names):
        if var_name not in prior_knowledge:
            for j in range(len(var_names)):
                for lag in range(tau_min, tau_max + 1):
                    link = (j, -lag)
                    link_list = selected_links.get(i)
                    if not link_list:
                        link_list = []
                    selected_links[i] = link_list
                    link_list.append(link)

    return selected_links
