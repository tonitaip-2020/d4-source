import re
import datetime
import time
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# Define regex patterns for different querying methods
patterns = {
    'RawSQL': r"(mysql)|(sqlite)|(import pg from \'pg\')",
    'Sequelize': r"sequelize",
    'Knex': r"knex",
    'TypeORM': r"typeorm",
    'Bookshelf': r"bookshelf",
    'Objection.js': r"objection",
    'Waterline': r"waterline",
    'Massive.js': r"massive",
    'Redis': r"(redis)|(ioredis)",
    'MongoDB': r"(mongodb)|(mongoose)",
    'Cassandra': r"cassandra-driver",
    'Neo4J': r"neo4j-driver",
    'OrientDB': r"orientjs",
}

# File path for CSV output
csv_file_path = 'javascript.csv'

# Initialize CSV file and write the header if the file doesn't exist
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

# Dictionary to store aggregated information per repository
repo_info = {}
count = 0
job_start_time = datetime.datetime.now()
loop_start_time = datetime.datetime.now()

# Function to write repositories with non-zero values to the CSV file
def write_all_repo_data():
    with open(csv_file_path, mode='a', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        for repo_name, info in repo_info.items():
            if any(info[method] > 0 for method in patterns.keys()):
                writer.writerow({
                    'repo_name': repo_name,
                    'num_files': info['num_files'],
                    'languages': ', '.join(info['languages']),
                    **{method: info[method] for method in patterns.keys()}
                })

# Offline, works after generation:
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["JavaScript"], num_proc=24)

# Process each data object in the stream
for obj in data_stream:
    repo_name = obj['repo_name']
    language = obj['language']
    code = obj['code']
    path = obj['path']
    
    # Initialize the repository entry if not already present
    if repo_name not in repo_info:
        repo_info[repo_name] = {
            'num_files': 0,
            'languages': set(),
            'RawSQL': 0,
            'Sequelize': 0,
            'Knex': 0,
            'TypeORM': 0,
            'Bookshelf': 0,
            'Objection.js': 0,
            'Waterline': 0,
            'Massive.js': 0,
            'Redis': 0,
            'MongoDB': 0,
            'Cassandra': 0,
            'Neo4J': 0,
            'OrientDB': 0,
        }
    
    # Update the repository information
    repo_info[repo_name]['num_files'] += 1
    repo_info[repo_name]['languages'].add(language)
    
    # Count occurrences of each querying method
    for method, pattern in patterns.items():
        repo_info[repo_name][method] += len(re.findall(pattern, code))
    
    # Periodically write to CSV and clear repo_info to manage memory usage
    if len(repo_info) >= 1000:
        write_all_repo_data()
        repo_info = {}

    count += 1
    if count % 10000 == 0:
        total = 11839883
        left = f"{total - count:,}"
        per = round(100 - count / total * 100, 2)
        loop_end_time = datetime.datetime.now()
        loop_duration = loop_end_time - loop_start_time
        formatted_duration = "{:.2f}".format(loop_duration.total_seconds())
        print(f"STAT: {left} files left ({per}%). Running for {datetime.datetime.now() - job_start_time}.", end=" ")
        print(f"This loop took {formatted_duration} seconds.")
        loop_start_time = datetime.datetime.now()
            
# Write remaining data to CSV file
write_all_repo_data()

print("END: Data has been written to", csv_file_path, "incrementally.")
