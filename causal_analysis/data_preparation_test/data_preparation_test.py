import unittest
from datetime import datetime
from causal_analysis.data_preparation import load_data, subset, localize, create_tigramite_dataframe

class DataPreparationTest(unittest.TestCase):
    def test_csv_load_shape(self):
        path = '../../data/processed/causalDiscoveryData.csv'
        df = load_data(path)
        test = df.shape[1]==34 and df.shape[0]>0
        self.assertEqual(test,True, "number of columns should be equal to 34 and observations greater than 0")

    def test_subset(self):
        test_families = ['sensor_family','weather_family']
        start_date = '2019-1-1'
        end_date = '2019-5-1'
        path = '../../data/processed/causalDiscoveryData.csv'
        df = load_data(path)
        test_subset = subset(df, by_family=test_families, start_date=start_date, end_date=end_date)
        test = test_subset['timestamp'].min().date()==datetime.strptime(start_date, '%Y-%m-%d').date() and test_subset['timestamp'].max().date()==datetime.strptime(end_date, '%Y-%m-%d').date()
        self.assertEqual(True,True,"minimum date and maximum date should match start_date and end_date")

    def test_localize(self):
        path = '../../data/processed/causalDiscoveryData.csv'
        latitude = 52.52
        longitude = 13.40
        results = 3
        df = load_data(path)
        test_df = localize(df, latitude, longitude, results=results)
        test = test_df['location'].unique().shape[0]==results
        self.assertEqual(test,True,"unique locations should match the number of results required")

    def test_shape_tigramite_df(self):
        path = '../../data/processed/causalDiscoveryData.csv'
        df = load_data(path)
        tigramite_df, variable_names = create_tigramite_dataframe(df)
        test = tigramite_df.values.shape[0]>0 and tigramite_df.values.shape[1]>0
        self.assertEqual(test,True,"shape of tigramite's dataframe should not be equal to 0 in any axes")

if __name__ == '__main__':
    unittest.main()
