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
    "RawSQL": r"'SQL'|'sqlite3'|'pg'|'pg::Connection'|'mysql2'|'oci8'|'tiny_tds'|'ibm_db'|'fb'|'odbc'",

    # ORMs and data abstraction layers
    "Ruby on Rails": r"'Rails'",
    "ActiveRecord": r"'active_record'",
    "Sequel": r"'sequel'",
    "ROM.rb": r"'rom'|'rom-sql'",
    "DataMapper": r"''data_mapper'",
    "Hanami::Model": r"'hanami/model'",
    "Trailblazer::Reform": r"'reform'",
    "Arel": r"'arel'",

    # NoSQL databases
    "MongoDB": r"'mongo'|'Mongo::Client'|'mongoid'",
    "CouchDB": r"'couchrest'",
    "Redis": r"'redis'|'redic'|'ohm'|'moneta'",
    "Cassandra": r"'cassandra'", # and ScyllaDB
    "HBase": r"'hbase'",
    "Neo4j": r"'neo4j'",
    "Gremlin.rb": r"'gremlin'",
    "InfluxDB": r"'influxdb'",

    # Embedded databases
    "LevelDB": r"'leveldb'",
    "LMDB": r"'lmdb'",
    "GDBM": r"'gdbm'",
    "TDB": r"'tdb'",
    "PStore": r"'pstore'",

    # Other methods
    # Out of scope for d4
    #"Google Firestore": r"firebase",
    #"Google Bigtable": r"google.cloud.bigtable",
    #"Amazon DynamoDB": r"aws-sdk-dynamodb",
    #"Microsoft Azure CosmosDB": r"azure.cosmos",
    #"CockroachDB": r"pg",
    #"TiDB": r"mysql2",
    #"RabbitMQ": r"bunny",
    #"Apache Kafka": r"kafka",
    #"ActiveMQ": r"stomp",
    #"ZeroMQ": r"ffi-rzmq",
    #"NATS": r"nats",
    #"Elasticsearch": r"elasticsearch",
    #"Apache Solr": r"rsolr",
    #"Google BigQuery": r"google/cloud/bigquery",
    #"Que": r"que",
    #"Sidekiq": r"sidekiq",
    #"Resque": r"resque"
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
