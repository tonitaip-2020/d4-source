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
    # Raw SQL: SQLite, MySQL/MariaDB, PostgreSQL, SQL Server, Oracle, DB2, Firebird
    # ODBC
    'RawSQL': r'(database/sql)|(go\-pg)|mattn/go-sqlite3|go-sql-driver/mysql|jackc/pgx|lib/pq|microsoft/go-mssqldb|go-sql-driver/mysql|godror/godror|ibmdb/go_ibm_db|nakagami/firebirdsql|alexbrainman/odbc',

    # ORMs & SQL abstraction layers
    'GORM': r'gorm.io/gorm',
    'Ent': r'entgo.io/ent',
    'XORM': r'xorm.io/xorm',
    'SQLBoiler': r'volatiletech/sqlboiler',
    'gorm-gen': r'taichi-maker/gorm-gen',
    'Bun': r'uptrace/bun',
    'go-pg': r'go-pg/pg',
    'gorm-adapter': r'casbin/gorm-adapter',

    # NoSQL databases
    'MongoDB (go-mongo-driver)': r'go.mongodb.org/mongo-driver|gopkg.in/mgo',
    'CouchDB': r'go-kivik/kivik',
    'Redis': r'redis/go-redis|gomodule/redigo',
    'Olric': r'olric-io/olric',
    'Cassandra': r'gocql/gocql',
    'HBase': r'tsuna/gohbase',
    'Neo4j': r'neo4j-go-driver',
    'Gremlin-Go': r'apache/tinkerpop/gremlin-go',
    'InfluxDB': r'influxdata/influxdb-client-go',
    'VictoriaMetrics': r'VictoriaMetrics/metrics',

    # Embedded databases
    'BadgerDB': r'dgraph-io/badger',
    'BoltDB': r'etcd-io/bbolt',
    'LevelDB': r'syndtr/goleveldb',
    'PebbleDB': r'cockroachdb/pebble',
    'LMDB': r'bmatsuo/lmdb-go',

    # Other data access libraries & abstraction layers
    'sqlx': r'jmoiron/sqlx',
    'gorm-dialects': r'gorm.io/driver',
    'squirrel': r'Masterminds/squirrel',
    'upper.io': r'upper.io/db.v3'

    # Cloud & Distributed Databases
    # Out of scope for d4
    #'Google Firestore': r'cloud.google.com/go/firestore',
    #'Google Bigtable': r'cloud.google.com/go/bigtable',
    #'Amazon DynamoDB': r'aws-sdk-go-v2/service/dynamodb',
    #'Microsoft Azure CosmosDB': r'Azure/azure-sdk-for-go/sdk/data/azcosmos',
    #'CockroachDB': r'pgx|gorm',  # Compatible with PostgreSQL ORMs
    #'TiDB': r'go-sql-driver/mysql',  # Compatible with MySQL driver

    # Message Queues & Caching Systems
    # Out of scope for d4
    #'RabbitMQ': r'streadway/amqp',
    #'Apache Kafka': r'Shopify/sarama',
    #'Confluent Kafka': r'confluentinc/confluent-kafka-go',
    #'NATS': r'nats-io/nats.go',
    #'ZeroMQ': r'pebbe/zmq4',

    # Specialized Data APIs & Search Engines
    #'Elasticsearch': r'olivere/elastic',
    #'Bleve': r'blevesearch/bleve',
    #'Google BigQuery': r'cloud.google.com/go/bigquery'
}

# File path for CSV output
csv_file_path = 'go.csv'

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
# "GO" must be in all caps!
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["GO"], num_proc=24)

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
        total = 2265436 # GO files
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
