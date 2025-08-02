from script.data_preprocessor import  CSVTimeSeriesPreprocessor

csv_preprocessor = CSVTimeSeriesPreprocessor("../data/generated_data/synthetic_timeseries.csv")

csv_preprocessor.load_data()
csv_preprocessor.normalize_data()
csv_preprocessor.handle_missing_values(dropna=True)
csv_preprocessor.trend_analysis()