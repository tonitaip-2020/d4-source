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
    # Raw SQL: SQLite, MySQL, PostgreSQL, Oracle, SQL Server, MariaDB, Firebird, DB2, Sybase, H2
    # JDBC, ODBC
    'RawSQL': r'org\.sqlite\.JDBC|org\.h2\.Driver|org\.hsqldb\.jdbc\.JDBCDriver|com\.sybase\.jdbc4\.jdbc\.SybDriver|com\.ibm\.db2\.jcc\.DB2Driver|org\.mariadb\.jdbc\.Driver|org\.mariadb\.jdbc\.Driver|com\.microsoft\.sqlserver\.jdbc\.SQLServerDriver|com\.mysql\.cj\.jdbc\.Driver|org\.postgresql\.Driver|oracle\.jdbc\.OracleDriver|java\.sql\..*|sun\.jdbc\.odbc\..*',

    # Object-Relational Mappers (ORMs) & SQL Abstraction Layers
    'Hibernate': r'org\.hibernate\.SessionFactory',
    'Jakarta Persistence API (JPA)': r'javax\.persistence\..*',
    'Spring Data JPA': r'org\.springframework\.data\.jpa\.repository\.JpaRepository',
    'EclipseLink': r'org\.eclipse\.persistence\.jpa\.JpaEntityManager',
    'TopLink': r'oracle\.toplink\.essentials\..*',
    'OpenJPA': r'org\.apache\.openjpa\.persistence\..*',
    'JOOQ': r'org\.jooq\.DSLContext',
    'QueryDSL': r'com\.querydsl\.jpa\.impl\.JPAQueryFactory',
    'MyBatis': r'org\.apache\.ibatis\.session\.SqlSession',
    'Apache Cayenne': r'org\.apache\.cayenne\.ObjectContext',
    'JDBI': r'org\.jdbi\.v3\.core\.Jdbi',
    'Ebean ORM': r'io\.ebean\..*',

    # NoSQL databases
    'MongoDB': r'com\.mongodb\.client\.MongoClient',
    'CouchDB': r'org\.ektorp\.CouchDbConnector',
    'Couchbase': r'com\.couchbase\.client\.java\.Cluster',
    'RavenDB': r'net\.ravendb\.client\.documents\.DocumentStore',
    'Redis': r'redis\.clients\.jedis\.Jedis|org\.redisson\.api\.RedissonClient'',
    'Apache Ignite': r'org\.apache\.ignite\.Ignite',
    'Hazelcast': r'com\.hazelcast\.core\.HazelcastInstance',
    'Ehcache': r'net\.sf\.ehcache\.CacheManager',
    'Memcached': r'net\.spy\.memcached\.MemcachedClient',
    'Apache Cassandra': r'com\.datastax\.oss\.driver\.api\.core\.CqlSession',
    'HBase': r'org\.apache\.hadoop\.hbase\.client\.Connection',
    'ScyllaDB': r'com\.datastax\.oss\.driver\.api\.core\.CqlSession',
    'Neo4j': r'org\.neo4j\.driver\.Driver',
    'TinkerPop Gremlin': r'org\.apache\.tinkerpop\.gremlin\.driver\.Cluster',
    'ArangoDB': r'com\.arangodb\.ArangoDB',
    'InfluxDB': r'org\.influxdb\.InfluxDB',
    'TimescaleDB': r'org\.postgresql\.Driver',

    # Embedded & File-Based Databases
    'Berkeley DB': r'com\.sleepycat\.je\.Database',
    'LevelDB': r'org\.iq80\.leveldb\.DB',
    'LMDB': r'org\.lmdbjava\.Env',

    # Other methods
    'Derby (JavaDB)': r'org\.apache\.derby\.jdbc\.EmbeddedDriver',
    'Apache Phoenix': r'org\.apache\.phoenix\.jdbc\.PhoenixDriver',
    'JTA': r'javax\.transaction\..*',
    'Spring Data JDBC': r'org\.springframework\.data\.jdbc\.repository\.config\.EnableJdbcRepositories',
    'Apache Commons DbUtils': r'org\.apache\.commons\.dbutils\.QueryRunner',

    # Cloud & Distributed Databases
    # Out of scope for d4
    #'Google Firestore': r'com\.google\.cloud\.firestore\.Firestore',
    #'Google Bigtable': r'com\.google\.cloud\.bigtable\.hbase\.BigtableConfiguration',
    #'Amazon DynamoDB': r'software\.amazon\.awssdk\.services\.dynamodb\.DynamoDbClient',
    #'Microsoft Azure CosmosDB': r'com\.azure\.cosmos\.CosmosClient',
    #'CockroachDB': r'org\.postgresql\.Driver',
    #'TiDB': r'com\.mysql\.cj\.jdbc\.Driver',

    # Message Queues & Caching Systems
    # Out of scope for d4
    #'RabbitMQ': r'com\.rabbitmq\.client\.ConnectionFactory',
    #'Apache Kafka': r'org\.apache\.kafka\.clients\.producer\.KafkaProducer',
    #'ActiveMQ': r'org\.apache\.activemq\.ActiveMQConnectionFactory',
    #'ZeroMQ': r'org\.zeromq\.ZMQ',
    #'NATS': r'io\.nats\.client\.Connection',

    # Specialized Data APIs & Graph Query Engines
    # Out of scope for d4
    #'Apache Solr': r'org\.apache\.solr\.client\.solrj\.SolrClient',
    #'Elasticsearch': r'org\.elasticsearch\.client\.RestHighLevelClient',
    #'Google BigQuery': r'com\.google\.cloud\.bigquery\.BigQuery',

}

# File path for CSV output
csv_file_path = 'java.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["Java"], num_proc=24)

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
        total = 19548190 # Java
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
