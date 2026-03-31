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
    # Raw SQL: SQLite, PostgreSQL, Oracle, DB2, SQL Server/Sybase (=tiny_tds) Firebird (=fb)
    # ODBC
    "RawSQL": r'\b(?:require|gem)\s*[("\']+(?:sqlite3|pg|mysql2|oci8|tiny_tds|ibm_db|fb|odbc)[)"\']|\bPG::Connection\b',
    
    "Ruby on Rails": r'\bRails\b|(?:require|gem)\s*[("\']+rails[)"\']',
    "ActiveRecord": r'\bActiveRecord\b|(?:require|gem)\s*[("\']+active_record[)"\']',
    "Sequel": r'\bSequel\b|(?:require|gem)\s*[("\']+sequel[)"\']',
    "ROM.rb": r'\bROM\b|(?:require|gem)\s*[("\']+(?:rom|rom-sql)[)"\']',
    "DataMapper": r'\bDataMapper\b|(?:require|gem)\s*[("\']+data_mapper[)"\']',
    "Hanami::Model": r'\bHanami::Model\b|(?:require|gem)\s*[("\']+hanami/model[)"\']',
    "Trailblazer::Reform": r'\bReform\b|(?:require|gem)\s*[("\']+reform[)"\']',
    "Arel": r'\bArel\b|(?:require|gem)\s*[("\']+arel[)"\']',
    
    "MongoDB": r'\bMongo(?:id|::Client)?\b|(?:require|gem)\s*[("\']+(?:mongo|mongoid)[)"\']',
    "CouchDB": r'(?:require|gem)\s*[("\']+couchrest[)"\']',
    "Redis": r'\bRedis\b|(?:require|gem)\s*[("\']+(?:redis|redic|ohm|moneta)[)"\']',
    "Cassandra": r'\bCassandra\b|(?:require|gem)\s*[("\']+cassandra[)"\']',
    "HBase": r'(?:require|gem)\s*[("\']+hbase[)"\']',
    "Neo4j": r'\bNeo4j\b|(?:require|gem)\s*[("\']+neo4j[)"\']',
    "Gremlin.rb": r'(?:require|gem)\s*[("\']+gremlin[)"\']',
    
    "InfluxDB": r'\bInfluxDB\b|(?:require|gem)\s*[("\']+influxdb[)"\']',
    "LevelDB": r'\bLevelDB\b|(?:require|gem)\s*[("\']+leveldb[)"\']',
    "LMDB": r'\bLMDB\b|(?:require|gem)\s*[("\']+lmdb[)"\']',
    "GDBM": r'\bGDBM\b|(?:require|gem)\s*[("\']+gdbm[)"\']',
    "TDB": r'(?:require|gem)\s*[("\']+tdb[)"\']',
    "PStore": r'\bPStore\b|(?:require|gem)\s*[("\']+pstore[)"\']',
}

# File path for CSV output
csv_file_path = "ruby.csv"

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", newline="") as csv_file:
        fieldnames = ["repo_name", "num_files", "languages"] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

repo_info = {}
count = 0
job_start_time = datetime.datetime.now()
loop_start_time = datetime.datetime.now()

def write_all_repo_data():
    with open(csv_file_path, mode="a", newline="") as csv_file:
        fieldnames = ["repo_name", "num_files", "languages"] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        for repo_name, info in repo_info.items():
            if any(info[method] > 0 for method in patterns.keys()):
                writer.writerow({
                    "repo_name": repo_name,
                    "num_files": info["num_files"],
                    "languages": ", ".join(info["languages"]),
                    **{method: info[method] for method in patterns.keys()}
                })

# Offline, works after generation
data_stream = load_dataset("codeparrot/github-code", data_files={"train": "data/train-0*-of-01126.parquet"}, split="train", filter_languages=True, languages=["Ruby"], num_proc=24)

for obj in data_stream:
    repo_name = obj["repo_name"]
    language = obj["language"]
    code = obj["code"]
    path = obj["path"]

    if repo_name not in repo_info:
        repo_info[repo_name] = {"num_files": 0, "languages": set(), **{method: 0 for method in patterns.keys()}}

    # Update the repository information
    repo_info[repo_name]["num_files"] += 1
    repo_info[repo_name]["languages"].add(language)

    for method, pattern in patterns.items():
        matches = len(re.findall(pattern, code))
        repo_info[repo_name][method] += matches

    if len(repo_info) >= 1000:
        write_all_repo_data()
        repo_info = {}
    count += 1
    if count % 10000 == 0:
        total = 4473331 # Ruby
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
