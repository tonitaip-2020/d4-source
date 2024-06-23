import re
import os
os.environ["HF_DATASETS_OFFLINE"] = "1"

import logging
# logging.basicConfig(level=logging.DEBUG)

import datasets
from datasets import load_dataset

import csv
from collections import defaultdict

# offline, works after generation:
data_stream = load_dataset("codeparrot/github-code", data_files="data/train-0*-of-01126.parquet", split='train', filter_languages=True, languages=["Java"], num_proc=12)

# Define regex patterns for different querying methods
patterns = {
    'Raw SQL': r'import\s+java\.sql\.(Connection|DriverManager|Statement|PreparedStatement|ResultSet);',
    'Hibernate': r'import\s+org\.hibernate\.(Session|SessionFactory|query\.Query|cfg\.Configuration|Criteria);',
    'JPA': r'import\s+javax\.persistence\.(EntityManager|PersistenceContext|Query|TypedQuery|NamedQuery|Entity|Table|Id|Column);',
    'jOOQ': r'import\s+org\.jooq\.(DSLContext|impl\.DSL|Record|Result);',
    'MyBatis': r'import\s+org\.apache\.ibatis\.(annotations\.Select|annotations\.Insert|annotations\.Update|annotations\.Delete);',
    'Spring Data JPA': r'import\s+org\.springframework\.data\.jpa\.(repository\.JpaRepository|repository\.Query);',
    'QueryDSL': r'import\s+com\.querydsl\.jpa\.impl\.JPAQueryFactory;',
    'JdbcTemplate': r'import\s+org\.springframework\.jdbc\.core\.JdbcTemplate;',
    'Ebean ORM': r'import\s+io\.ebean\.(Ebean|Query|Server);',
    'Criteria API (JPA)': r'import\s+javax\.persistence\.criteria\.(CriteriaBuilder|CriteriaQuery|Root);',
    'Spring JDBC Template': r'import\s+org\.springframework\.jdbc\.core\.JdbcTemplate;',
    'Apache Cayenne': r'import\s+org\.apache\.cayenne\.(ObjectSelect|SQLTemplate);',
    'OpenJPA': r'import\s+org\.apache\.openjpa\.(persistence\.EntityManager|persistence\.PersistenceContext|persistence\.Query|persistence\.TypedQuery|persistence\.NamedQuery);',
    'Redis': r'import\s+redis\.clients\.jedis\.(Jedis|JedisPool);',
    'MongoDB': r'import\s+com\.mongodb\.(MongoClient|MongoDatabase|MongoCollection);',
    'Cassandra': r'import\s+com\.datastax\.driver\.core\.(Cluster|Session);',
    'Neo4J': r'import\s+org\.neo4j\.driver\.(Driver|Session|Transaction);',
    'ObjectDB': r'import\s+com\.objectdb\.ojb\.(ObjectContainer|Query|Extent);',
    'EclipseLink': r'import\s+org\.eclipse\.persistence\.(sessions\.Session|queries\.Query|jpa\.EntityManager);',
    'Batoo JPA': r'import\s+org\.batoo\.jpa\.(EntityManager|Persistence|Query);',
    'Speedment': r'import\s+com\.speedment\.(Speedment|config\.db\.tables\.Table|runtime\.core\.Application);',
    'OrientDB': r'import\s+com\.orientechnologies\.orient\.(core\.db\.ODatabaseSession|core\.query\.OQuery);'
}

# File path for CSV output
csv_file_path = 'java.csv'

# Write header to CSV file
with open(csv_file_path, mode='w', newline='') as csv_file:
    fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

# Dictionary to store aggregated information per repository
repo_info = {}
count = 0

# Initialize CSV file and write the header if the file doesn't exist
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

# Function to write all repository data to the CSV file
def write_all_repo_data():
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['repo_name', 'num_files', 'languages'] + list(patterns.keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for repo_name, info in repo_info.items():
            writer.writerow({
                'repo_name': repo_name,
                'num_files': info['num_files'],
                'languages': ', '.join(info['languages']),
                **{method: info[method] for method in patterns.keys()}
            })

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
    
    # Write all repository information to the CSV file after each update
    write_all_repo_data()
    
    # TODO: remove these
    count += 1
    if count % 10000 == 0:
      left = 19548190 - count
      per = 100 - count / 19548190 * 100
      print("STAT: There are about ", left, " files left (", per, ").")
      print(count, ": Wrote to .csv file. Latest from ", repo_name, ".")
      
    next(iter(data_stream))

print("END: Data has been written to ", csv_file_path, " incrementally.")
