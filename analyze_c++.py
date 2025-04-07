import re
import datetime
import time
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# C++: #include <x> / #include "x"
# Define regex patterns for different querying methods
patterns = {
    # Raw SQL: MySQL, MariaDB, PostgreSQL, SQLite, SQL Server, Oracle, Firebird
    # ODBC
    "RawSQL": r'#include\s*[<\"]\s*mysql|#include\s*[<\"]\s*pqxx|#include\s*[<\"]\s*pg_query|#include\s*[<\"]\s*mariadb|#include\s*[<\"]\s*sqlite|#include\s*[<\"]\s*oci|#include\s*[<\"]\s*sybfront|#include\s*[<\"]\s*sydb|#include\s*[<\"]\s*ibase|#include\s*[<\"]\s*oci|#include\s*[<\"]\s*odbc',

    # NoSQL Databases
    "Redis": r'#include\s*[<\"]\s*hiredis',
    "MongoDB": r'#include\s*[<\"]\s*mongocxx',
    "Cassandra": r'#include\s*[<\"]\s*cassandra',
    "Neo4j": r'#include\s*[<\"]\s*neo4j-client',
    "memcached": r'#include\s*[<\"]\s*memcached',

    # Embedded Databases
    "Berkeley DB": r'#include\s*[<\"]\s*db_cxx',
    "Kyoto Cabinet": r'#include\s*[<\"]\s*kcdb',
    "Tokyo Cabinet": r'#include\s*[<\"]\s*tcutil',
    "UnQLite": r'#include\s*[<\"]\s*unqlite',
    "GDBM": r'#include\s*[<\"]\s*gdbm',
    "RocksDB": r'#include\s*[<\"]\s*rocksdb',
    "LevelDB": r'#include\s*[<\"]\s*leveldb|#include\s*[<\"]\s*LevelDB',
    "LMDB": r'#include\s*[<\"]\s*lmdb',
    "FoundationDB": r'#include\s*[<\"]\s*fdb_c',
    "HyperLevelDB": r'#include\s*[<\"]\s*hyperleveldb',

    # ORM Libraries
    "TinyORM": r'#include\s*[<\"]\s*TinyOrm',
    "ODB": r'#include\s*[<\"]\s*odb/database',
    "sqlpp11": r'#include\s*[<\"]\s*sqlpp11'

    # Message Queues and Caching
    # Out of scope for d4
    #"RabbitMQ": r'#include\s*[<\"]\s*amqb',
    #"ZeroMQ": r'#include\s*[<\"]\s*zmq',
    #"Kafka": r'#include\s*[<\"]\s*rdkafka'
}

# File path for CSV output
csv_file_path = 'c++.csv'

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

repo_info = {}
count = 0
job_start_time = datetime.datetime.now()
loop_start_time = datetime.datetime.now()

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

# Offline, works after generation
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["C++"], num_proc=24)

for obj in data_stream:
    repo_name = obj['repo_name']
    language = obj['language']
    code = obj['code']
    path = obj['path']

    if repo_name not in repo_info:
        repo_info[repo_name] = {'num_files': 0, 'languages': set(), **{method: 0 for method in patterns.keys()}}

    # Update the repository information
    repo_info[repo_name]['num_files'] += 1
    repo_info[repo_name]['languages'].add(language)

    for method, pattern in patterns.items():
        matches = len(re.findall(pattern, code))
        repo_info[repo_name][method] += matches

    if len(repo_info) >= 1000:
        write_all_repo_data()
        repo_info = {}

    count += 1
    if count % 10000 == 0:
        total = 7380520 # C++
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
