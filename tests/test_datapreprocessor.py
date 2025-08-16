import pytest
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from humangechi.data_preprocessor import DataPreprocessor

@pytest.fixture
def sample_df():
    df = pd.DataFrame({
        'name': ['Alice','Alice',None,"David"],
        "age" :[30,30,12,None],
        'city' : ['Arizona','Arizona',None,"Taraba"]
    })
    return df
def test_remove_duplicate(tmp_path, sample_df):
    preprocessor = DataPreprocessor(sample_df)

    #pytest's tmp_path fixture for a temporary file
    filename = tmp_path / "cleaned.csv"
    preprocessor.remove_duplicates(filename)
    #Expected Dataframe
    expected = sample_df.drop_duplicates(keep='first', ignore_index=True)
    assert preprocessor.data.equals(expected)
    # Check file was actually written
    saved_df = pd.read_csv(filename)
    pd.testing.assert_frame_equal(saved_df, expected)

def test_convert_data_types(sample_df):
    processor = DataPreprocessor(sample_df)
    # Convert 'age' column to string
    processor.data['age'] = processor.data['age'].astype(float)
    processor.convert_data_types(filename=None, columns=['age'])
    assert processor.data['age'].dtype == 'float64' or processor.data['age'].dtype == 'Int64'
def test_summary_stat_invalid_df(sample_df):
    preprocessor = DataPreprocessor(sample_df)
    preprocessor.summary_stat()
    expected = sample_df.describe(exclude='string')
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(expected,sample_df)
def test_clean_data(tmp_path, sample_df):
    preprocessor = DataPreprocessor(sample_df)
    filename = tmp_path / "cleaned.csv"
    preprocessor.clean_data(filename)
    expected = sample_df
    expected['age'] = expected['age'].fillna(sample_df['age'].mean())
    assert preprocessor.data.equals(expected)
    saved_df = pd.read_csv(filename)
    pd.testing.assert_frame_equal(saved_df, expected)