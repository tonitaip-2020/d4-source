import re
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import logging
# logging.basicConfig(level=logging.DEBUG)

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# generating examples takes as much time as there are files in the Language
# 1000 items = 10 sec.
# offline, works after generation:
data_stream = load_dataset("codeparrot/github-code", data_files="data/train-0*-of-01126.parquet", split='train', filter_languages=True, languages=["JavaScript"], num_proc=12)

# Define regex patterns for different querying methods
# Define regex patterns for different querying methods and DBMSs in JavaScript
patterns = {
    'Sequelize': r"require\s*\(\s*['\"]sequelize['\"]\s*\)",
    'Knex': r"require\s*\(\s*['\"]knex['\"]\s*\)",
    'TypeORM': r"require\s*\(\s*['\"]typeorm['\"]\s*\)",
    'Bookshelf': r"require\s*\(\s*['\"]bookshelf['\"]\s*\)",
    'Objection.js': r"require\s*\(\s*['\"]objection['\"]\s*\)",
    'Waterline': r"require\s*\(\s*['\"]waterline['\"]\s*\)",
    'Massive.js': r"require\s*\(\s*['\"]massive['\"]\s*\)",
    'node-postgres': r"require\s*\(\s*['\"]pg['\"]\s*\)",
    'mysql': r"require\s*\(\s*['\"]mysql['\"]\s*\)|require\s*\(\s*['\"]mysql2['\"]\s*\)",
    'sqlite': r"require\s*\(\s*['\"]sqlite3['\"]\s*\)|require\s*\(\s*['\"]sqlite['\"]\s*\)",
    'Redis': r"require\s*\(\s*['\"]redis['\"]\s*\)|require\s*\(\s*['\"]ioredis['\"]\s*\)",
    'MongoDB': r"require\s*\(\s*['\"]mongodb['\"]\s*\)|require\s*\(\s*['\"]mongoose['\"]\s*\)",
    'Cassandra': r"require\s*\(\s*['\"]cassandra-driver['\"]\s*\)",
    'Neo4J': r"require\s*\(\s*['\"]neo4j-driver['\"]\s*\)",
    'OrientDB': r"require\s*\(\s*['\"]orientjs['\"]\s*\)",
}

# File path for CSV output
csv_file_path = 'javascript.csv'

# Write header to CSV file
with open(csv_file_path, mode='w', newline='') as csv_file:
    fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

# Dictionary to store aggregated information per repository
repo_info = {}
count = 0

# Initialize CSV file and write the header if the file doesn't exist
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

# Function to write all repository data to the CSV file
def write_all_repo_data():
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for repo_name, info in repo_info.items():
            writer.writerow({
                'repo_name': repo_name,
                'num_files': info['num_files'],
                'languages': ', '.join(info['languages']),
                **{method: info[method] for method in patterns.keys()}
            })

# Process each data object in the stream
for obj in data_stream:
    repo_name = obj['repo_name']
    language = obj['language']
    code = obj['code']
    path = obj['path']
    
    # Initialize the repository entry if not already present
    if repo_name not in repo_info:
        # Initialize repository information dictionary
        repo_info[repo_name] = {
            'num_files': 0,
            'languages': set(),
            'Sequelize': 0,
            'Knex': 0,
            'TypeORM': 0,
            'Bookshelf': 0,
            'Objection.js': 0,
            'Waterline': 0,
            'Massive.js': 0,
            'node-postgres': 0,
            'mysql': 0,
            'sqlite': 0,
            'redis': 0,
            'mongodb': 0,
            'Cassandra': 0,
            'Neo4J': 0,
            'OrientDB': 0,
        })
    
    # Update the repository information
    repo_info[repo_name]['num_files'] += 1
    repo_info[repo_name]['languages'].add(language)
    
    # Count occurrences of each querying method
    for method, pattern in patterns.items():
        repo_info[repo_name][method] += len(re.findall(pattern, code))
    
    # Write all repository information to the CSV file after each update
    write_all_repo_data()
    
    # TODO: remove these
    count += 1
    if count % 10000 == 0:
      left = 11839883 - count
      per = 100 - count / 11839883 * 100
      print("STAT: There are about ", left, " files left (", per, ").")
      print(count, ": Wrote to .csv file. Latest from ", repo_name, ".")
      
    next(iter(data_stream))

print("END: Data has been written to ", csv_file_path, " incrementally.")
