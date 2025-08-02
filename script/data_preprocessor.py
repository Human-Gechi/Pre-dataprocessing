from logs.custom_logger import logger
import pandas as pd
from abc import ABC, abstractmethod

class TimeSeriesPreprocessor(ABC):
    """
    Abstract base class for time series data preprocessing.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df = None

    @abstractmethod
    def load_data(self) -> pd.DataFrame:
        """
        Loads the time series data into a pandas dataframe
        :return: Preprocessed DataFrame.
        """
        pass

    @abstractmethod
    def normalize_data(self) -> pd.DataFrame:
        """
        Normalizes the time series data to a custom uniform interval.
        :return: Preprocessed DataFrame.
        :return:
        """

    @abstractmethod
    def trend_analysis(self) -> pd.DataFrame:
        """ Analyzes the data and get data statistics"""
        pass
    
    @abstractmethod
    def handle_missing_values(self, target_col=None, dropna=False, axis=0, constant=None, strategy="mean"):
       """Custom missing value handler"""
    pass

class CSVTimeSeriesPreprocessor(TimeSeriesPreprocessor):
    """Subclass to handle CSV files """
    def load_data(self) -> pd.DataFrame:
        logger.debug(f"Loading the dataset from{self.filepath}")
        self.df = pd.read_csv(self.filepath, index_col=0, parse_dates=True)
        return self.df

    def normalize_data(self) -> pd.DataFrame:
        logger.debug("Normalizing Data to DAILY")
        self.df = self.df.asfreq("D")
        self.df = self.df.interpolate(method="time")
        return self.df

    def trend_analysis(self) :
        trend = self.df.diff().mean()
        print("<------------ Trend Direction ------------->")
        print(trend)
        print("<------------ Data Statistics ------------->")
        print(self.df.describe(include="all"))
    
    def handle_missing_values(self, target_col=None, dropna=False, axis=0, constant=None, strategy="mean") -> pd.DataFrame:
        logger.debug("Handling missing values")
        if dropna:
            self.df.dropna(axis=axis, inplace=True)
        elif strategy == "mean":
            self.df[target_col].fillna(self.df[target_col].mean(), inplace=True)
        elif strategy == "median":
            self.df[target_col].fillna(self.df[target_col].median(), inplace=True)
        elif strategy == "max":
            self.df[target_col].fillna(self.df[target_col].max(), inplace=True)
        elif strategy == "constant" and constant is not None:
            self.df[target_col].fillna(constant, inplace=True)
        logger.info("Missing values handled!")
        return self.df


class ExcelTimeSeriesPreprocessor(TimeSeriesPreprocessor):
    """ Subclass to handle Excel files """
    def load_data(self) -> pd.DataFrame:
        logger.debug(f"Loading the dataset from{self.filepath}")
        self.df = pd.read_excel(self.filepath, index_col=0, parse_dates=True)
        return self.df

    def normalize_data(self) -> pd.DataFrame:
        logger.debug("Normalizing Data to DAILY")
        self.df = self.df.asfreq("D")
        self.df = self.df.interpolate(method="linear")
        return  self.df

    def trend_analysis(self):
        trend = self.df.diff().mean()
        print("<------------ Trend Direction ------------->")
        print(trend)
        print("<------------ Data Statistics ------------->")
        print(self.df.describe(include="all"))
    
    def handle_missing_values(self, target_col=None, dropna=False, axis=0, constant=None, strategy="mean"):
        logger.debug("Handling missing values")
        if dropna:
            self.df.dropna(axis=axis, inplace=True)
        elif strategy == "mean":
            self.df[target_col].fillna(self.df[target_col].mean(), inplace=True)
        elif strategy == "median":
            self.df[target_col].fillna(self.df[target_col].median(), inplace=True)
        elif strategy == "max":
            self.df[target_col].fillna(self.df[target_col].max(), inplace=True)
        elif strategy == "constant" and constant is not None:
            self.df[target_col].fillna(constant, inplace=True)
        logger.info("Missing Values handled!")


class JSONTimeSeriesPreprocessor(TimeSeriesPreprocessor):
    """ Subclass to handle JSON files"""
    def load_data(self) -> pd.DataFrame:
        logger.debug(f"Loading data from {self.filepath}")
        self.df = pd.read_json(self.filepath)
        self.df.set_index(pd.to_datetime(self.df.iloc[:, 0]), inplace=True)
        self.df.drop(columns=self.df.columns[0], inplace=True)
        return self.df

    def normalize_data(self) -> pd.DataFrame:
        logger.debug("Normalizing data to DAILY")
        self.df = self.df.asfreq("D")
        self.df = self.df.interpolate(method="spline", order=2)
        return self.df

    def trend_analysis(self):
        trend = self.df.diff().mean()
        print("<------------ Trend Direction ------------->")
        print(trend)
        print("<------------ Data Statistics ------------->")
        print(self.df.describe(include="all"))
    
    def handle_missing_values(self, target_col=None, dropna=False, axis=0, constant=None, strategy="mean"):
        logger.debug("Handling missing values")
        if dropna:
            self.df.dropna(axis=axis, inplace=True)
        elif strategy == "mean":
            self.df[target_col].fillna(self.df[target_col].mean(), inplace=True)
        elif strategy == "median":
            self.df[target_col].fillna(self.df[target_col].median(), inplace=True)
        elif strategy == "max":
            self.df[target_col].fillna(self.df[target_col].max(), inplace=True)
        elif strategy == "constant" and constant is not None:
            self.df[target_col].fillna(constant, inplace=True)
        logger.info("Missing Values handled!")

class ParquetTimeSeriesPreprocessor(TimeSeriesPreprocessor):
    """Subclass to handle Parquet files"""
    def load_data(self) -> pd.DataFrame:
        logger.debug(f"Loading data from {self.filepath}")
        self.df = pd.read_parquet(self.filepath)
        self.df.set_index(pd.to_datetime(self.df.iloc[:, 0]), inplace=True)
        self.df.drop(columns=self.df.columns[0], inplace=True)
        return self.df

    def normalize_data(self) -> pd.DataFrame:
        logger.debug("Normalizing Data to DAILY")
        self.df = self.df.asfreq("D")
        self.df = self.df.interpolate(method="polynomial", order=2)
        return self.df

    def trend_analysis(self):
        trend = self.df.diff().mean()
        print("<------------ Trend Direction ------------->")
        print(trend)
        print("<------------ Data Statistics ------------->")
        print(self.df.describe(include="all"))
    
    def handle_missing_values(self, target_col=None, dropna=False, axis=0, constant=None, strategy="mean"):
        logger.debug("Handling Missing Values")
        if dropna:
            self.df.dropna(axis=axis, inplace=True)
        elif strategy == "mean":
            self.df[target_col].fillna(self.df[target_col].mean(), inplace=True)
        elif strategy == "median":
            self.df[target_col].fillna(self.df[target_col].median(), inplace=True)
        elif strategy == "max":
            self.df[target_col].fillna(self.df[target_col].max(), inplace=True)
        elif strategy == "constant" and constant is not None:
            self.df[target_col].fillna(constant, inplace=True)
        logger.info("Missing Values handled!")
