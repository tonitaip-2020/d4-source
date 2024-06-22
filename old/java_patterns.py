import re
import csv

# Sample data stream
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
        'code': "import mod189 from './mod189';\nvar value=mod189+1;\nexport default value;\n",
        'repo_name': 'MirekSz/repo2',
        'path': 'app/mods/mod190.js',
        'language': 'JavaScript',
        'license': 'isc',
        'size': 73
    },
    {
        'code': "import mod189 from './mod189';\nvar value=mod189+1;\nexport default value;\n",
        'repo_name': 'MirekSz/webpack-es6-ts',
        'path': 'app/mods/mod2000.js',
        'language': 'JavaScript',
        'license': 'isc',
        'size': 73
    }
]

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

# Dictionary to store aggregated information per repository
repo_info = {}

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

# Write the aggregated information to a CSV file
with open('repository_info.csv', mode='w', newline='') as csv_file:
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

print("Data has been written to repository_info.csv")

