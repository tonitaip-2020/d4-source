import re
import os
os.environ["HF_DATASETS_OFFLINE"] = "0"

import logging
# logging.basicConfig(level=logging.DEBUG)

import datasets
from datasets import load_dataset
# >>> datasets.__version__
# '2.18.0'

import csv
from collections import defaultdict

# TODO: download the dataset, this will take about 133 d with Java alone
# TODO: check all patterns with real data
# TODO: create a repository for this

# Sample data stream
"""
data_stream = [
    {
        'code': "print('hello world')",
        'repo_name': 'MirekSz/repo1',
        'path': 'app/mods/mod190.py',
        'language': 'Python',
        'license': 'isc',
        'size': 73
    },
    {
        'code': "SELECT * FROM internet;",
        'repo_name': 'MirekSz/repo2',
        'path': 'app/mods/mod190.js',
        'language': 'JavaScript',
        'license': 'isc',
        'size': 73
    },
    {
        'code': "import mod189 from './mod189';\nvar value=mod189+1;\nexport default value;\n",
        'repo_name': 'MirekSz/repo1',
        'path': 'app/mods/mod2000.js',
        'language': 'JavaScript',
        'license': 'isc',
        'size': 73
    }
]
"""

data_stream = load_dataset("codeparrot/github-code", streaming=True, split="train", filter_languages=True, languages=["Java"])

# Define regex patterns for different querying methods
patterns = {
    'Raw SQL': r'createStatement\s*\(|prepareStatement\s*\(|executeQuery\s*\(',
    'Hibernate': r'createQuery\s*\(|createCriteria\s*\(',
    'JPA': r'EntityManager\s*\.\s*(createQuery|createNamedQuery|createNativeQuery|createStoredProcedureQuery)\s*\(',
    'jOOQ': r'DSL\.select\s*\(|DSL\.insert\s*\(|DSL\.update\s*\(|DSL\.delete\s*\(',
    'MyBatis': r'@Select\s*\(|@Insert\s*\(|@Update\s*\(|@Delete\s*\(',
    'Spring Data JPA': r'findBy\s*\(|queryBy\s*\(',
    'QueryDSL': r'JPAQueryFactory\s*\.\s*',
    'JdbcTemplate': r'JdbcTemplate\s*\.\s*(query|update|execute)\s*\(',
    'Ebean ORM': r'find\s*\(|query\s*\(',
    'Criteria API (JPA)': r'CriteriaBuilder\s*\.\s*',
    'Spring JDBC Template': r'jdbcTemplate\s*\.\s*(query|update|execute)\s*\(',
    'Apache Cayenne': r'ObjectSelect\.query\s*\(|SQLTemplate\.query\s*\(',
    'OpenJPA': r'EntityManager\s*\.\s*(createQuery|createNamedQuery|createNativeQuery|createStoredProcedureQuery)\s*\(',
    'ObjectDB': r'EntityManager\s*\.\s*(createQuery|createNamedQuery|createNativeQuery|createStoredProcedureQuery)\s*\(',
    'EclipseLink': r'Session\s*\.\s*(executeQuery|createQuery|createNamedQuery)\s*\(',
    'Slick (Scala)': r'Slick\.\s*',
    'JOOQ': r'DSLContext\s*\.\s*(select|insert|update|delete)\s*\(',
    'Batoo JPA': r'EntityManager\s*\.\s*(createQuery|createNamedQuery|createNativeQuery|createStoredProcedureQuery)\s*\(',
    'Speedment': r'Speedment\s*\.\s*',
    'OrientDB': r'OrientDB\s*\.\s*',
    'ArangoDB': r'ArangoDatabase\s*\.\s*',
    'Elasticsearch': r'Elasticsearch\s*\.\s*',
    'MongoDB': r'MongoCollection\s*\.\s*',
    'Neo4j': r'Neo4j\s*\.\s*',
    'Cassandra': r'Cassandra\s*\.\s*',
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
            'ObjectDB': 0,
            'EclipseLink': 0,
            'Slick (Scala)': 0,
            'JOOQ': 0,
            'Batoo JPA': 0,
            'Speedment': 0,
            'OrientDB': 0,
            'ArangoDB': 0,
            'Elasticsearch': 0,
            'MongoDB': 0,
            'Neo4j': 0,
            'Cassandra': 0,
        }
    
    # Update the repository information
    repo_info[repo_name]['num_files'] += 1
    repo_info[repo_name]['languages'].add(language)
    
    # Count occurrences of each querying method
    for method, pattern in patterns.items():
        repo_info[repo_name][method] += len(re.findall(pattern, code))
    
    # Write all repository information to the CSV file after each update
    write_all_repo_data()

    count += 1
    if count % 20 == 0:
      left = 19548190 - count
      per = 100 - count / 19548190 * 100
      print("STAT: There are about ", left, " files left (", per, ").")
    print(count, ": Wrote a line to the .csv file from repository ", repo_name)
    next(iter(data_stream))

print("END: Data has been written to ", csv_file_path, " incrementally.")
