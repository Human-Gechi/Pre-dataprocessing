from fileinput import filename
import sys
import os
import pandas as pd

# Ensure the logs directory is in the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logs.custom_logger import logger

#Defining the DataPreprocessor class
# This class handles the loading, cleaning, and preprocessing of data files.
class DataPreprocessor:
    def __init__(self, data):
        self.original_filename = data if isinstance(data, str) else None
        self.data = data
        if isinstance(data, str):
            if data.endswith('.csv'):
                self.data = pd.read_csv(data)
            elif data.endswith('.json'):
                self.data = pd.read_json(data, orient='records', lines=True)
            elif data.endswith('.xlsx'):
                self.data = pd.read_excel(data)
            elif data.endswith('.parquet'):
                self.data = pd.read_parquet(data)
            else:
                raise ValueError("Unsupported file format")
        logger.info("Data loaded successfully")
        if self.data is None or not isinstance(self.data, pd.DataFrame):
            raise ValueError("Data is not properly loaded.")

    def clean_data(self, filename=None):
        if self.data.empty:
            print("DataFrame is empty. Nothing to clean.")
            return self.data

        # Find columns with null values
        null_columns = self.data.columns[self.data.isnull().any()].tolist()
        if not null_columns:
            print("No columns contain null values.")
            return self.data

        print(f"Columns with null values: {null_columns}")
        column = input("Enter the column name to clean (or press Enter to clean all): ").strip()
        if column not in null_columns:
            print(f"Column '{column}' either does not exist or has no null values.")
            return self.data

        while True:
            print('What do you want to do with null values in this column?'
                '\n1. Drop rows with null values'
                '\n2. Fill null values with mean'
                '\n3. Fill null values with median'
                '\n4. Fill null values with mode'
                '\n5. Fill null values with a specific value'
                '\n6. Fill with 0'
                '\n7. Fill with pd.NA'
                '\n8. Fill with pd.NaT for datetime columns')
            choice = input("Enter your choice (1-8): ").strip()

            try:
                if choice == '1':
                    self.data = self.data.dropna(subset=[column])
                    break
                elif choice == '2':
                    self.data[column] = self.data[column].fillna(self.data[column].mean(numeric_only=True))
                    break
                elif choice == '3':
                    self.data[column] = self.data[column].fillna(self.data[column].median(numeric_only=True))
                    break
                elif choice == '4':
                    self.data[column] = self.data[column].fillna(self.data[column].mode().iloc[0])
                    break
                elif choice == '5':
                    value = input('Enter the value to fill null column with: ').strip()
                    self.data[column] = self.data[column].fillna(value)
                    break
                elif choice == '6':
                    self.data[column] = self.data[column].fillna(0)
                    break
                elif choice == '7':
                    self.data[column] = self.data[column].fillna(pd.NA)
                    break
                elif choice == '8':
                    self.data[column] = self.data[column].fillna(pd.NaT)
                    break
                else:
                    print('Invalid choice. Try again.')
            except Exception as e:
                print(f"Error during fill: {e}")
                continue

        if filename:
            self.data.to_csv(filename, index=False)
            logger.info(f"Data cleaned successfully and saved to {filename}.")
        return self.data
    def remove_duplicates(self,filename):
        before = len(self.data)
        self.data = self.data.drop_duplicates(keep='first', ignore_index=True)

        after = len(self.data)
        print(f"Removed {before - after} duplicate rows.")
        logger.info(f"Removed {before - after} duplicate rows.")
        if self.data.empty:
            print("No data left after removing duplicates.")
            return
        if filename:
            self.data.to_csv(filename, index=False)
        logger.info(f"Duplicated data has been removed successfully and saved to {filename}.")

    def convert_data_types(self,filename,columns=None):
        self.data.info()
        if columns is None:
            columns = self.data.columns
        while True:
            column = input("Enter the column name to convert (or 'exit' to stop): ").strip()
            if column == 'exit':
                break
            if column not in self.data.columns:
                print("Invalid column name.")
                continue
            print(f"\nWhat do you want to convert '{column}' to?")
            print("1. int\n2. float\n3. str\n4. datetime")
            choice = input("Enter your choice (1-4): ").strip()

            try:
                if choice == '1':
                    self.data[column] = pd.to_numeric(self.data[column], errors='coerce').round().astype('Int64')
                    self.data[column] = self.data[column].astype('Int64')
                elif choice == '2':
                    self.data[column] = pd.to_numeric(self.data[column], errors='coerce')
                elif choice == '3':
                    self.data[column] = self.data[column].astype(str)
                elif choice == '4':
                    self.data[column] = pd.to_datetime(self.data[column], errors='coerce')
                else:
                    print("Invalid choice.")
                    continue
                print(f"'{column}' has been converted successfully.")
            except Exception as e:
                print(f"Conversion failed: {e}")
        if filename:
            self.data.to_csv(filename, index=False)
            logger.info(f"Data types has been converted successfully and saved to {filename}.")
        return self.data

    def summary_stat(self):
        if self.data is None or not isinstance(self.data, pd.DataFrame):
            raise ValueError("Data is not properly loaded.")
        summary = self.data.describe(exclude='string')
        summary.to_csv('summary_statistics.csv') #saving summary statistics csv file
        logger.info('Summary statistics generated and saved to summary_statistics.csv.')
        logger.info('Summary statistics CSV file created successfully.')
        return summary

    def resampling(self, column, freq,filename):
        if column not in self.data.columns:
            raise ValueError("Invalid column name.")
        self.data[column] = pd.to_datetime(self.data[column], errors='coerce')
        resampled = self.data.set_index(column).resample(freq).asfreq()
        if filename:
            resampled.to_csv(filename, index=False)
        logger.info(f"Resampled data saved to {filename}.")
def main():
    filename = input("Enter the filename of the dataset: ").strip()
    preprocessor = DataPreprocessor(filename)
    while True:
        print("\nData Preprocessing Menu:")
        print("1. Clean Data")
        print("2. Remove Duplicates")
        print("3. Convert Data Types")
        print("4. Summary Statistics")
        print("5. Resampling")
        print("6. Save Cleaned Data")
        print("7. Exit")

        choice = input("Enter your choice (1-7): ").strip()

        if choice == '1':
            preprocessor.clean_data(filename)
            print("Data cleaned successfully.")
        elif choice == '2':
            preprocessor.remove_duplicates(filename)
        elif choice == '3':
            preprocessor.convert_data_types(filename)
        elif choice == '4':
            preprocessor.summary_stat()
            print("Summary statistics generated and saved to summary_statistics.csv.")
        elif choice == '5':
            column = input("Enter the column to resample: ").strip()
            freq = input('Enter frequency (e.g., D, W, M): ').strip()
            result = preprocessor.resampling(column, freq)
            print(result.head())
        elif choice == '6':
            save_filename = input("Enter filename to save cleaned data: ").strip()
            preprocessor.save_cleaned_data(save_filename)
        elif choice == '7':
            break
        else:
            print("Invalid choice. Please try again.")
if __name__ == "__main__":
    main()