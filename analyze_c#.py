import re
import datetime
import time
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# C#: using x / using y.x
# Define regex patterns for different querying methods
patterns = {
    # Raw SQL:
    # ODBC
    # SQL Server, MySQL, PostgreSQL, Oracle, Firebird, DB2, Sybase, Access, SQLite
    "RawSQL": r"using\s+System\.Data\.Odbc|using\s+System\.Data\.SqlClient|using\s+Microsoft\.Data\.SqlClient|using\s+MySql\.Data\.MySqlClient|using\s+Npgsql|using\s+Oracle\.ManagedDataAccess\.Client|using\s+FirebirdSql\.Data\.FirebirdClient|using\s+IBM\.Data\.DB2|using\s+Sybase\.Data\.AseClient|using\s+System\.Data\.OleDb|using\s+System\.Data\.SQLite",

    # ORMs & SQL abstraction layers
    "OLE DB": r"using\s+System\.Data\.OleDb",
    "Entity Framework": r"using\s+Microsoft\.EntityFrameworkCore|using\s+System\.Data\.Entity", # Core & EF6
    "Dapper": r"using\s+Dapper",
    "NHibernate": r"using\s+NHibernate",
    "LINQ": r"using\s+System\.Data\.Linq|using\s+LinqToDB|linq|Linq", # LINQ to SQL, Linq2db, Linq general
    "ServiceStack OrmLite": r"using\s+ServiceStack\.OrmLite",
    "Telerik Data Access": r"using\s+Telerik\.OpenAccess",
    "RepoDb": r"using\s+RepoDb",
    "SqlKata": r"using\s+SqlKata\.Execution",
    "PetaPoco": r"using\s+PetaPoco",
    "Insight.Database": r"using\s+Insight\.Database",

    # NoSQL databases
    "MongoDB": r"using\s+MongoDB\.Driver",
    "Redis": r"using\s+StackExchange\.Redis",
    "Couchbase": r"using\s+Couchbase",
    "RavenDB": r"using\s+Raven\.Client\.Documents",
    "Cassandra": r"using\s+Cassandra",
    "DynamoDB": r"using\s+Amazon\.DynamoDBv2",
    "CosmosDB": r"using\s+Microsoft\.Azure\.Cosmos",
    "CouchDB": r"using\s+MyCouch",
    "LiteDB": r"using\s+LiteDB",
    "OrientDB": r"using\s+Orient\.Client",
    "Neo4j": r"using\s+Neo4jClient",
    "Memcached": r"using\s+Enyim\.Caching",

    # Embedded databases
    "RocksDB.NET": r"using\s+RocksDbSharp",
    "LMDB.NET": r"using\s+LightningDB",

    # Specialized Data APIs & Graph Query Engines
    "InfluxDB": r"using\s+InfluxDB\.Client",
    "TimescaleDB": r"using\s+Npgsql",
    "Gremlin.NET": r"using\s+Gremlin\.Net\.Driver",

    # Other Data Access Libraries & Abstraction Layers
    "FluentNHibernate": r"using\s+FluentNHibernate",
    "NPoco": r"using\s+NPoco",
    "DbUp": r"using\s+DbUp",
    "MassTransit": r"using\s+MassTransit"

    # Cloud & Distributed Databases
    # Out of scope for d4
    #"Azure Table Storage": r"using\s+Microsoft\.Azure\.Cosmos\.Table",
    #"Google Firestore": r"using\s+Google\.Cloud\.Firestore",
    #"Google BigQuery": r"using\s+Google\.Cloud\.BigQuery\.V2",
    #"ElasticSearch": r"using\s+Nest",
    #"Apache Ignite.NET": r"using\s+Apache\.Ignite\.Core",

    # Message Queues & Caching Systems
    # Out of scope for d4
    #"RabbitMQ": r"using\s+RabbitMQ\.Client",
    #"Apache Kafka": r"using\s+KafkaNet",
    #"ZeroMQ": r"using\s+NetMQ",
    #"NATS": r"using\s+NATS\.Client",
}

# File path for CSV output
csv_file_path = 'c#.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["C#"], num_proc=24)

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
        total = 6811652 # C#
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
