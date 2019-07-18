import pandas as pd
import time
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb, RCOT
from tigramite.pcmci import PCMCI
import random
from causal_analysis.data_preparation import load_data, subset, localize, input_na, create_tigramite_dataframe

pd.set_option('mode.chained_assignment', None)
path = '../../data/processed/causalDiscoveryData.csv'

my_dataset = load_data(path)

print('data loaded')

my_subset = subset(data=my_dataset, by_family=['sensor_family', 'time_family', 'weather_family'], start_date='2017-1-1',
                   end_date='2019-6-8')

print('data subset created')

my_localized_subset = localize(my_subset, lat=52.517, lon=13.425, results=1)

print('localized data created')

# pd.set_option('mode.chained_assignment', None)
my_procesed_dataset = input_na(my_localized_subset, columns=['location',
                                                             'lat',
                                                             'lon',
                                                             'timestamp',
                                                             'dayOfYear',
                                                             'minuteOfDay',
                                                             'dayOfWeek',
                                                             'isWeekend',
                                                             'altitude',
                                                             'pressure_sealevel',
                                                             'temperature',
                                                             'humidity_1',
                                                             'p1',
                                                             'p2',
                                                             'p0',
                                                             'durP1',
                                                             'ratioP1',
                                                             'durP2',
                                                             'ratioP2',
                                                             'apparent_temperature',
                                                             'cloud_cover',
                                                             'dew_point',
                                                             'humidity',
                                                             'ozone',
                                                             'precip_intensity',
                                                             'precip_probability',
                                                             'precip_type',
                                                             'pressure',
                                                             'uv_index',
                                                             'visibility',
                                                             'wind_bearing',
                                                             'wind_gust',
                                                             'wind_speed'], method='bfill')

my_procesed_dataset = input_na(my_procesed_dataset, columns=['location',
                                                             'lat',
                                                             'lon',
                                                             'timestamp',
                                                             'dayOfYear',
                                                             'minuteOfDay',
                                                             'dayOfWeek',
                                                             'isWeekend',
                                                             'altitude',
                                                             'pressure_sealevel',
                                                             'temperature',
                                                             'humidity_1',
                                                             'p1',
                                                             'p2',
                                                             'p0',
                                                             'durP1',
                                                             'ratioP1',
                                                             'durP2',
                                                             'ratioP2',
                                                             'apparent_temperature',
                                                             'cloud_cover',
                                                             'dew_point',
                                                             'humidity',
                                                             'ozone',
                                                             'precip_intensity',
                                                             'precip_probability',
                                                             'precip_type',
                                                             'pressure',
                                                             'uv_index',
                                                             'visibility',
                                                             'wind_bearing',
                                                             'wind_gust',
                                                             'wind_speed'], value=0)

my_procesed_dataset.dropna(inplace=True)

print('data na handled')

not_include = ['location', 'lat', 'lon', 'precip_type', 'altitude',
               'pressure_sealevel',
               'temperature',
               'humidity_1', 'p0',
               'durP1',
               'ratioP1',
               'durP2',
               'ratioP2', 'ozone', 'pressure']

my_variables = my_procesed_dataset.iloc[:, ~my_procesed_dataset.columns.isin(not_include)]

for i in list(my_variables):
    if my_variables[i].dtype == 'int64':
        my_variables[i] = my_variables[i].astype('float64')


def generate_DF(complexity=[5], instances=1000, sample_sizes=[1000]):
    tigramite_dataframes = []

    for c in complexity:
        for x in sample_sizes:
            stop = False
            count = 0
            while not stop:
                a = random.sample(range(my_variables.shape[1]), c)
                if all(a) != list(my_variables).index('timestamp'):
                    count += 1
                    a.extend([list(my_variables).index('timestamp')])
                    l = list(my_variables.iloc[:, a])
                    dataframe, var_names = create_tigramite_dataframe(my_variables.iloc[0:x + 1, a],
                                                                      exclude=["timestamp"])
                    tigramite_dataframes.append([c, x, dataframe, l])
                    if count == instances:
                        stop = True
                    # print(str(l) + str(dataframe.values.shape))
                else:
                    pass
    return tigramite_dataframes


def test(dataframes, max_lags=[4], alpha=[None], tests=['RCOT'], limit=1, num_f=[2]):
    test_results = []
    random.shuffle(dataframes)
    total = limit * len(max_lags) * len(alpha) * len(tests)
    data_frame_iter = iter(dataframes)

    tests_to_evaluate = []
    for f in num_f:
        rcot = RCOT(num_f=f)
        tests_to_evaluate.append(['RCOT_' + str(f), rcot])

    unique_complexities = list(set(l[1] for l in dataframes))
    counts = {}
    for i in unique_complexities:
        counts[i] = 0

    for test in tests_to_evaluate:
        stop = False
        for l in max_lags:
            for a in alpha:
                while not stop:
                    try:
                        i = random.sample(dataframes, 1)[0]
                        if counts[i[1]] < limit:
                            print('evaluating: ' + str(i[3]))
                            start = time.time()
                            pcmci = PCMCI(
                                dataframe=i[2],
                                cond_ind_test=test[1],
                                verbosity=0)
                            # correlations = pcmci.get_lagged_dependencies(tau_max=20)
                            pcmci.verbosity = 1
                            results = pcmci.run_pcmci(tau_max=l, pc_alpha=a)
                            time_lapse = round(time.time() - start, 2)

                            q_matrix = pcmci.get_corrected_pvalues(p_matrix=results['p_matrix'], fdr_method='fdr_bh')
                            valid_parents = list(pcmci.return_significant_parents(pq_matrix=q_matrix,
                                                                                  val_matrix=results['val_matrix'],
                                                                                  alpha_level=a)['parents'].values())

                            flat_list = []
                            for sublist in valid_parents:
                                for item in sublist:
                                    flat_list.append(item)

                            valid_links = len(flat_list)

                            test_results.append([i[3], i[0], i[1], l, test[0], a, valid_links, time_lapse])

                            results_df = pd.DataFrame(test_results,
                                                      columns=['representation', 'complexity', 'sample_size', 'max_lag',
                                                               'test', 'alpha', 'valid_links_at_alpha',
                                                               'learning_time'])
                            results_df.to_csv(
                                '../Results/results_fourier.csv',
                                index=False)

                            counts[i[1]] += 1
                            if all(value == limit for value in counts.values()):
                                stop = True

                    except:
                        print('Hoopla!')
                        pass

                for i in unique_complexities:
                    counts[i] = 0


networks = generate_DF(complexity=[10],sample_sizes=[1000])

print(str(len(networks)) + ' dataframes created ')

test(dataframes=networks,max_lags=[4],alpha=[0.05],tests=['RCOT'],limit = 1,num_f=[10,100,200,500,1000,2000,5000])

#test(dataframes=networks,max_lags=[4],alpha=[0.05],tests=['ParCorr','RCOT'],limit = 1)


# test(dataframes=networks,max_lags=[4],alpha=[0.05],tests=['ParCorr','RCOT'],limit = 1)
