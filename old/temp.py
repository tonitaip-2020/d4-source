import re
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import logging
# logging.basicConfig(level=logging.DEBUG)

import datasets
from datasets import load_dataset
# >>> datasets.__version__
# '2.18.0'

import csv
from collections import defaultdict
import pandas as pd

a = pd.read_parquet('data/train-00000-of-01126.parquet', engine='pyarrow')
print(a)
