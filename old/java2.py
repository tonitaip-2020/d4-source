import re
import os
os.environ["HF_DATASETS_OFFLINE"] = "0"

import logging
#logging.basicConfig(level=logging.DEBUG)

import datasets
from datasets import load_dataset
# >>> datasets.__version__
# '2.18.0'

import csv
from collections import defaultdict

# debugging:
datasets.utils.logging.set_verbosity_info()

# https://huggingface.co/datasets/codeparrot/github-code

#ds = load_dataset("codeparrot/github-code", streaming=True, split="train", trust_remote_code=True, languages=["SQL"])
#ds.save_to_disk("my-arrow-datasets")

ds = load_dataset("codeparrot/github-code", streaming=True, split="train", filter_languages=True, languages=["SQL"])
print(next(iter(ds)))
print("###################################################")
print(next(iter(ds)))
