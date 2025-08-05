import pytest
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from humangechi.data_preprocessor import DataPreprocessor
from logs.custom_logger import logger
