import re
import datetime
import time
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# PHP: recuire 'x' / require_once 'x' / include 'x' / include_once 'x'
# PHP: regex word boundaries \b make the analysis ~20x slower
# Define regex patterns for different querying methods
patterns = {
    # Raw SQL queries
    # MySQL/MariaDB, PostgreSQL/CockroachDB, SQLite, SQL Server, Oracle, DB2, Firebird
    # ODBC
    # PDO (data access layer, NOT database abstraction)
    'RawSQL': r'mysql_query|mysqli|pg_query|pgsql|sqlite|sqlsrv|oci8|ibm_db2|interbase|odbc|PDO',

    # ORMs and SQL abstraction layers
    'Doctrine ORM': r'Doctrine\\ORM',
    'Eloquent (Illuminate)': r'Illuminate\\Database',
    'Propel ORM': r'Propel',
    'Cycle ORM': r'cycle\\orm',
    'RedBeanPHP': r'redbeanphp\\rb',
    'Idiorm': r'j4mie\\idiorm',
    'Paris': r'j4mie\\paris',
    'Medoo': r'catfan\\medoo',
    'Atlas ORM': r'atlas\\orm',
    'DBAL': r'doctrine\\dbal',
    'AURA SQL': r'aura\\sql',
    'CakePHP ORM': r'cakephp\\orm',
    'FuelPHP ORM': r'fuelphp\\orm',
    'Zend DB': r'laminas\\laminas-db',

    # NoSQL databases
    'MongoDB': r'mongodb\\mongodb',
    'CouchDB': r'couchdb',
    'Redis (phpredis)': r'redis|predis',
    'Memcache': r'memcache', # and memcached
    'Cassandra': r'datastax\\php-driver',
    'ScyllaDB': r'datastax\\php-driver|cassandra',
    'Neo4J': r'graphaware\\neo4j-php-client|Neo4j',
    'Gremlin-PHP': r'pomm-project\\foundation',
    'InfluxDB': r'influxdb\\influxdb-php',

    # Embedded databases
    'Berkeley DB': r'DB4PHP',
    'LevelDB': r'LevelDB',
    'LMDB': r'LMDB',
    'GDBM': r'GDBM',

    # XML & Web Services
    'SOAP': r'SoapClient',
    'XML-RPC': r'xmlrpc_encode_request|xmlrpc_server_call_method',
    'GraphQL': r'GraphQL\\Query|GraphQL\\Mutation',

    # Cloud & distributed databases
    # Out of scope for d4
    #'Google Firestore': r'google\\cloud-firestore',
    #'Google Bigtable': r'google\\cloud-bigtable',
    #'Amazon DynamoDB': r'aws\\aws-sdk-php',
    #'Microsoft Azure CosmosDB': r'microsoft\\azure-sdk-for-php',
    #'TiDB': r'PDO|mysqli', # provides MySQL-like access

    # Message queues & caching systems
    # Out of scope for d4
    #'RabbitMQ': r'php-amqplib\\php-amqplib',
    #'Apache Kafka': r'arnaud-lb\\php-rdkafka',
    #'ActiveMQ': r'fusesource\\stomp-php',
    #'ZeroMQ': r'zeromq\\php-zmq',
    #'NATS': r'nats-io\\nats.php',

    # Specialized Data APIs & Search Engines
    # Out of scope for d4
    #'Elasticsearch': r'elasticsearch\\elasticsearch',
    #'Solarium (Solr)': r'solarium\\solarium',
    #'Google BigQuery': r'google\\cloud-bigquery',
}


# File path for CSV output
csv_file_path = 'php.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["PHP"], num_proc=24)

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
        total = 11177610 # PHP
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
