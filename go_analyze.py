import re
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import logging
# logging.basicConfig(level=logging.DEBUG)

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# offline, works after generation:
data_stream = load_dataset("codeparrot/github-code", data_files="data/train-0*-of-01126.parquet", split='train', filter_languages=True, languages=["Go"], num_proc=12)

# Define regex patterns for different querying methods
patterns = {
    'Raw SQL': r'import\s+\(\s*"database/sql"\s*\)|import\s+"database/sql"|import\s+"go\-pg";',
    'GORM/GORM2': r'import\s+\(\s*"github\.com/jinzhu/gorm"\s*\)|import\s+"github\.com/jinzhu/gorm"|import\s+"gorm\.io";',
    'xorm': r'import\s+\(\s*"github\.com/go-xorm/xorm"\s*\)|import\s+"github\.com/go-xorm/xorm";',
    'upper': r'upper\.io',
    'qbs' : r'vattle\/qbs',
    'sqlx': r'import\s+\(\s*"github\.com/jmoiron/sqlx"\s*\)|import\s+"github\.com/jmoiron/sqlx";',
    'pgx': r'jackkc\/pgx',
    'dbr': r'gocraft\/dbr',
    'gorp': r'gopkg\.in',
    'entgo': r'import\s+\(\s*"entgo\.io/ent"\s*\)|import\s+"entgo\.io/ent";',
    'Redis': r'import\s+\(\s*"github\.com/go-redis/redis/v8"\s*\)|import\s+"github\.com/go-redis/redis/v8";',
    'MongoDB': r'import\s+\(\s*"go\.mongodb\.org/mongo-driver/mongo"\s*\)|import\s+"go\.mongodb\.org/mongo-driver/mongo";',
    'Cassandra': r'import\s+\(\s*"github\.com/gocql/gocql"\s*\)|import\s+"github\.com/gocql/gocql";',
    'Neo4J': r'import\s+\(\s*"github\.com/neo4j/neo4j-go-driver/v4"\s*\)|import\s+"github\.com/neo4j/neo4j-go-driver/v4";',
    'OrientDB': r'import\s+\(\s*"github\.com/istreamdata/orientgo"\s*\)|import\s+"github\.com/istreamdata/orientgo";',
}

# File path for CSV output
csv_file_path = 'go.csv'

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
        repo_info[repo_name] = {
            'num_files': 0,
            'languages': set(),
            'Raw SQL': 0,
            'GORM/GORM2': 0,
            'xorm': 0,
            'upper': 0,
            'qbs': 0,
            'sqlx': 0,
            'pgx': 0,
            'dbr': 0,
            'gorp': 0,
            'entgo': 0,
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
    
    # Write all repository information to the CSV file after each update
    write_all_repo_data()
    
    # TODO: remove these
    count += 1
    if count % 10000 == 0:
      left = 2265436 - count
      per = 100 - count / 2265436 * 100
      print("STAT: There are about ", left, " files left (", per, ").")
      print(count, ": Wrote to .csv file. Latest from ", repo_name, ".")
      
    next(iter(data_stream))

print("END: Data has been written to ", csv_file_path, " incrementally.")
