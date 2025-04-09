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
    # Raw SQL: MySQL, PostgreSQL/TimescaleDB/CockroachDB, SQLite, SQL Server, Oracle, MariaDB, DB2
    'RawSQL': r"'mysql'|'mysql2'|'pg'|'pg-promise'|'sqlite.*?'|'better-sqlite.*?'|'oracledb'|'mariadb'|'ibm_db'",

    # ORMs and SQL abstraction layers
    'Sequelize': r"'sequelize'",
    'TypeORM': r"'typeorm'",
    'Objection.js': r"'objection'",
    'Knex.js': r"'knex'",
    'Prisma': r"'@prisma/client'",
    'Bookshelf.js': r"'bookshelf'",
    'Waterline': r"'waterline'",
    'Massive.js': r"'massive'",
    'Kysely': r"'kysely'",

    # NoSQL Databases
    'MongoDB': r"'mongodb'|'mongoose'",
    'CouchDB': r"'nano'",
    'PouchDB': r"'pouchdb'",
    'Redis': r"'redis'|'ioredis'",
    'Keyv': r"'keyv'",
    'Cassandra': r"'cassandra-driver'",
    'ScyllaDB': r"'cassandra-driver'",
    'HBase': r"'hbase'",
    'Neo4J': r"'neo4j-driver'",
    'Gremlin-JS': r"'gremlin'",

    # Time-Series Databases
    'InfluxDB': r"'@influxdata/influxdb-client'",

    # Embedded & File-Based Databases
    'NeDB': r"'nedb'",
    'LowDB': r"'lowdb'",
    'LokiJS': r"'lokijs'",
    'TaffyDB': r"'taffy'",

    # Other Data Access Libraries & Abstraction Layers
    'DataLoader': r"'dataloader'",
    'GraphQL.js': r"'graphql'",
    'Apollo Client': r"'@apollo/client'",
    'Hasura': r"'hasura'",

    # Cloud & Distributed Databases
    # Out of scope for d4
    #'Google Firestore': r"'firebase-admin'",
    #'Google Bigtable': r"'@google-cloud/bigtable'",
    #'Amazon DynamoDB': r"'aws-sdk'",
    #'Azure CosmosDB': r"'@azure/cosmos'",
    #'CockroachDB': r"'pg'",
    #'TiDB': r"'mysql2'",

    # Message Queues & Caching Systems
    # Out of scope for d4
    #'RabbitMQ': r"'amqplib'",
    #'Apache Kafka': r"'kafkajs'",
    #'ZeroMQ': r"'zeromq'",
    #'NATS': r"'nats'",

    # Specialized Data APIs & Graph Query Engines
    # Out of scope for d4
    #'Apache Solr': r"'solr-client'",
    #'Elasticsearch': r"'@elastic/elasticsearch'",
    #'Google BigQuery': r"'@google-cloud/bigquery'",

}

# File path for CSV output
csv_file_path = 'javascript.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["JavaScript"], num_proc=24)

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
        total = 11839883 # JavaScript
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
