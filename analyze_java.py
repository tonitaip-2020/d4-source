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
    'Raw SQL': r'import\s+java\.sql\.(Connection|DriverManager|Statement|PreparedStatement|ResultSet)',
    'Hibernate': r'import\s+org\.hibernate\.(Session|SessionFactory|query\.Query|cfg\.Configuration|Criteria)',
    'JPA': r'import\s+javax\.persistence\.(EntityManager|PersistenceContext|Query|TypedQuery|NamedQuery|Entity|Table|Id|Column);',
    'jOOQ': r'import\s+org\.jooq',
    'MyBatis': r'import\s+org\.apache\.ibatis\.(annotations\.Select|annotations\.Insert|annotations\.Update|annotations\.Delete);',
    'Spring Data JPA': r'import\s+org\.springframework\.data\.jpa\.(repository\.JpaRepository|repository\.Query);',
    'QueryDSL': r'import\s+com\.querydsl\.jpa\.impl\.JPAQueryFactory;',
    'JdbcTemplate': r'import\s+org\.springframework\.jdbc\.core\.JdbcTemplate;',
    'Ebean ORM': r'import\s+io\.ebean\.(Ebean|Query|Server);',
    'Criteria API (JPA)': r'import\s+javax\.persistence\.criteria\.(CriteriaBuilder|CriteriaQuery|Root);',
    'Spring JDBC Template': r'import\s+org\.springframework\.jdbc\.core\.JdbcTemplate;',
    'Apache Cayenne': r'import\s+org\.apache\.cayenne\.(ObjectSelect|SQLTemplate);',
    'OpenJPA': r'import\s+org\.apache\.openjpa\.(persistence\.EntityManager|persistence\.PersistenceContext|persistence\.Query|persistence\.TypedQuery|persistence\.NamedQuery);',
    'Redis': r'import\s+redis\.clients\.jedis\.(Jedis|JedisPool);|redis\.',
    'MongoDB': r'import\s+com\.mongodb\.(MongoClient|MongoDatabase|MongoCollection);|mongodb\.',
    'Cassandra': r'import\s+com\.datastax\.driver\.core|cassandra\.',
    'Neo4J': r'import\s+org\.neo4j\.driver|neo4j\.',
    'ObjectDB': r'import\s+com\.objectdb\.ojb',
    'EclipseLink': r'import\s+org\.eclipse\.persistence\.(sessions\.Session|queries\.Query|jpa\.EntityManager);',
    'Batoo JPA': r'import\s+org\.batoo\.jpa\.(EntityManager|Persistence|Query);',
    'Speedment': r'import\s+com\.speedment\.(Speedment|config\.db\.tables\.Table|runtime\.core\.Application);',
    'OrientDB': r'import\s+com\.orientechnologies\.orient\.(core\.db\.ODatabaseSession|core\.query\.OQuery);'
}

# File path for CSV output
csv_file_path = 'java.csv'

# Initialize CSV file and write the header if the file doesn't exist
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

# Dictionary to store aggregated information per repository
repo_info = {}
count = 0
job_start_time = datetime.datetime.now()
loop_start_time = datetime.datetime.now()

# Function to write repositories with non-zero values to the CSV file
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

# Offline, works after generation:
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["Java"], num_proc=24)

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
            'Hibernate': 0,
            'JPA': 0,
            'jOOQ': 0,
            'MyBatis': 0,
            'Spring Data JPA': 0,
            'QueryDSL': 0,
            'JdbcTemplate': 0,
            'Ebean ORM': 0,
            'Criteria API (JPA)': 0,
            'Spring JDBC Template': 0,
            'Apache Cayenne': 0,
            'OpenJPA': 0,
            'Redis': 0,
            'MongoDB': 0,
            'Cassandra': 0,
            'Neo4J': 0,
            'ObjectDB': 0,
            'EclipseLink': 0,
            'Batoo JPA': 0,
            'Speedment': 0,
            'OrientDB': 0,
        }
    
    # Update the repository information
    repo_info[repo_name]['num_files'] += 1
    repo_info[repo_name]['languages'].add(language)
    
    # Count occurrences of each querying method
    for method, pattern in patterns.items():
        repo_info[repo_name][method] += len(re.findall(pattern, code))
    
    # Periodically write to CSV and clear repo_info to manage memory usage
    if len(repo_info) >= 1000:
        write_all_repo_data()
        repo_info = {}

    count += 1
    if count % 10000 == 0:
        total = 19548190
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
