from processing import generate_dataframe
from tigramite import plotting as tp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb, RCOT

import time


def test_alphas(dataframe, cond_ind_test, alphas, var_names):
    tau_max = 24
    tau_min = 0

    pcmci = PCMCI(
        dataframe=dataframe,
        cond_ind_test=cond_ind_test,
        verbosity=1)
    for pc_alpha in alphas:
        run_experiment(pcmci, cond_ind_test, pc_alpha, tau_min, tau_max, var_names)


def run_experiment(pcmci, cond_ind_test, pc_alpha, tau_min, tau_max, var_names):
    start = time.time()
    results = pcmci.run_pcmci(
        tau_min=tau_min,
        tau_max=tau_max,
        pc_alpha=pc_alpha,
        fdr_method='fdr_bh')

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
    file_name_prefix = f"images/{class_prefix}_alpha_{pc_alpha:.5f}_tau_{tau_min}to{tau_max}"

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


def linear_dependencies_experiment():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    dataframe = generate_dataframe(var_names)
    print(f"Variable names: {var_names}")
    parcorr = ParCorr(significance='analytic')
    alphas = [3 ** -n for n in range(1, 9)]
    test_alphas(dataframe, parcorr, alphas, var_names)


def rcot_hyperparam_experiment():
    var_names = ["dayOfYear", "minuteOfYear", "minuteOfDay", "dayOfWeek", "isWeekend", "humidity_sensor", "temperature",
                 "precip_intensity", "cloud_cover", "p1", "p2", "dew_point", "wind_speed"]
    dataframe = generate_dataframe(var_names)
    print(f"Variable names: {var_names}")
    num_fs = [2 ** n + 1 for n in range(1, 10)]
    for num_f in num_fs:
        rcot = RCOT(significance='analytic', num_f=num_f)
        test_alphas(dataframe, rcot, [0.0125, 0.025, 0.05, 0.1, 0.2], var_names)


if __name__ == "__main__":
    rcot_hyperparam_experiment()
