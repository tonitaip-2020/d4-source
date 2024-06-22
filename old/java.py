import re
import os
import csv
from collections import defaultdict
from datasets import load_dataset

# https://huggingface.co/datasets/codeparrot/github-code


# Function to read a Java file and count the occurrences of querying methods
def count_queries_in_java_code(file_path):
    with open(file_path, 'r') as file:
        code = file.read()

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

    # Initialize a dictionary to store the counts
    counts = {method: 0 for method in patterns}

    # Count occurrences of each querying method
    for method, pattern in patterns.items():
        counts[method] += len(re.findall(pattern, code))

    return counts

# Function to process all Java files in a directory
def process_directory(directory_path):
    total_counts = {method: 0 for method in patterns}

    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.java'):
                file_path = os.path.join(root, file)
                file_counts = count_queries_in_java_code(file_path)
                for method, count in file_counts.items():
                    total_counts[method] += count

    return total_counts

def count_queries_in_code(code):
    counts = {method: 0 for method in patterns}
    for method, pattern in patterns.items():
        counts[method] += len(re.findall(pattern, code))
    return counts


# Function to process data streams and generate the CSV report
def process_data_stream(data_stream):
    repo_data = defaultdict(lambda: defaultdict(int))

    for data in data_stream:
        repo_name = data['repo_name']
        language = data['language']
        code = data['code']

        if language.lower() == 'java':
            repo_data[repo_name]['java_files'] += 1
            query_counts = count_queries_in_code(code)
            has_db_operation = any(count > 0 for count in query_counts.values())
            if has_db_operation:
                repo_data[repo_name]['files_with_db_operations'] += 1
            for method, count in query_counts.items():
                repo_data[repo_name][method] += count

    # Write the results to a CSV file
    with open('repo_query_counts.csv', 'w', newline='') as csvfile:
        fieldnames = ['repo_name', 'java_files', 'files_with_db_operations'] + list(patterns.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for repo, counts in repo_data.items():
            row = {'repo_name': repo, 'java_files': counts['java_files'], 'files_with_db_operations': counts['files_with_db_operations']}
            row.update({method: counts[method] for method in patterns})
            writer.writerow(row)

# Example data stream (you would replace this with your actual data stream)
data_stream = load_dataset("codeparrot/github-code", streaming=True, split="train", languages=["Java"])
print(next(iter(ds))["code"])

# Process the data stream
process_data_stream(data_stream)

