from processing import generate_dataframe
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb, RCOT

import time


def run_experiments(dataframe, cond_ind_test):
    tau_max = 24
    tau_min = 0

    pcmci = PCMCI(
        dataframe=dataframe,
        cond_ind_test=cond_ind_test,
        verbosity=1)
    for pc_alpha in [3 ** -n for n in range(1, 9)]:
        run_experiment(pcmci, cond_ind_test, pc_alpha, tau_min, tau_max)


def run_experiment(pcmci, cond_ind_test, pc_alpha, tau_min, tau_max):
    start = time.time()
    results = pcmci.run_pcmci(
        tau_min=tau_min,
        tau_max=tau_max,
        pc_alpha=pc_alpha,
        fdr_method='fdr_bh')

    end = time.time()

    print(f"Test: {cond_ind_test.measure}\nAlpha: {pc_alpha}\n"
          f"Min. Tau: {tau_min}\nMax. Tau: {tau_max}\n"
          f"Execution Time: {round(end - start, 2)} seconds\n")

    plot_results(pcmci, results, cond_ind_test, pc_alpha, tau_min, tau_max)


def plot_results(pcmci, results, cond_ind_test, pc_alpha, tau_min, tau_max):
    q_matrix = pcmci.get_corrected_pvalues(p_matrix=results['p_matrix'], fdr_method='fdr_bh')

    link_matrix = pcmci.return_significant_parents(
        pq_matrix=q_matrix,
        val_matrix=results['val_matrix'],
        alpha_level=0.01)['link_matrix']
    file_name_prefix = f"images/{cond_ind_test.measure}_alpha_{pc_alpha:.5f}_tau_{tau_min}to{tau_max}"

    tp.plot_graph(
        val_matrix=results['val_matrix'],
        link_matrix=link_matrix,
        var_names=var_names,
        link_colorbar_label='cross-MCI',
        node_colorbar_label='auto-MCI',
        figsize=(20, 20),
        save_name=file_name_prefix + "_graph.png"
    )

    tp.plot_time_series_graph(
        val_matrix=results['val_matrix'],
        link_matrix=link_matrix,
        var_names=var_names,
        link_colorbar_label='MCI',
        figsize=(20, 20),
        save_name=file_name_prefix + "_time_series_graph.png"
    )


var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
             "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]

dataframe = generate_dataframe(var_names)

print(f"Variable names: {var_names}")
parcorr = ParCorr(significance='analytic')
run_experiments(dataframe, parcorr)
