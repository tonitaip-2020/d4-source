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
    # RawSQL: SQLite, MySQL/MariaDB, PostgreSQL/CockroachDB, Oracle, Firebird, SQL Server
    # ODBC
    'RawSQL': r'#include\s*[<"](sqlite3\.h|mysql/mysql\.h|libpq-fe\.h|oci\.h|ibase\.h|sybfront\.h|sybdb\.h)[">]|#include\s*[<"](sql\.h|sqlext\.h)[">]',

    # NoSQL databases
    'Redis': r'#include\s*[<"]hiredis/hiredis\.h[">]',
    'MongoDB': r'#include\s*[<"]mongoc\.h[">]',
    'Cassandra': r'#include\s*[<"]cassandra\.h[">]',
    'Neo4J': r'#include\s*[<"]neo4j-client\.h[">]',
    'Memcached': r'#include\s*[<"]libmemcached/memcached\.h[">]',

    # Embedded databases
    'RocksDB': r'#include\s*[<"]rocksdb/c\.h[">]',
    'LevelDB': r'#include\s*[<"]leveldb/c\.h[">]',
    'LMDB': r'#include\s*[<"]lmdb\.h[">]',
    'FoundationDB': r'#include\s*[<"]fdb_c\.h[">]',
    'HyperLevelDB': r'#include\s*[<"]hyperleveldb/db\.h[">]',
    'BerkeleyDB': r'#include\s*[<"]db\.h[">]',
    'GDBM': r'#include\s*[<"]gdbm\.h[">]',

    'KyotoCabinet': r'#include\s*[<"]kcdb\.h[">]',
    'TokyoCabinet': r'#include\s*[<"]tcutil\.h[">]',
    'UnQLite': r'#include\s*[<"]unqlite\.h[">]'

    # Other methods, out of scope for d4
    #'RabbitMQ': r'#include\s*[<"]amqp\.h[">]',
    #'ZeroMQ': r'#include\s*[<"]zmq\.h[">]',
    #'Kafka': r'#include\s*[<"]librdkafka/rdkafka\.h[">]',
    #'HDF5': r'#include\s*[<"]hdf5\.h[">]',
    #'CERN_ROOT': r'#include\s*[<"]TFile\.h[">]',
    # 'Parquet': r'#include\s*[<"]parquet/api/reader\.h[">]', # not a DB
    #'TiDB': r'#include\s*[<"]mysql\.h[">]' # MySQL-like
}

csv_file_path = 'c.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["C"], num_proc=24)

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
        total = 14143113 # C
        left = f"{total - count:,}"
        per = round(100 - count / total * 100, 2)
        loop_end_time = datetime.datetime.now()
        loop_duration = loop_end_time - loop_start_time
        formatted_duration = "{:.2f}".format(loop_duration.total_seconds())
        print(f"STAT: {left} files left ({per}%). Running for {datetime.datetime.now() - job_start_time}.", end=" ")
        print(f"This loop took {formatted_duration} seconds.")
        loop_start_time = datetime.datetime.now()

# Write the remaining data to CSV file
write_all_repo_data()

print("END: Data has been written incrementally and analysis saved.")
