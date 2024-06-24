import re
import glob

# Define regex patterns for different types of joins
join_patterns = [
    r'\bJOIN\b',                  # General JOIN keyword
    r'\bNATURAL JOIN\b',          # NATURAL JOIN
    r'\bEXISTS\s*\(.*?\)',        # EXISTS + subquery
    r'\bIN\s*\(.*?\)',            # IN + subquery
    r'\bWHERE\b.*?\bJOIN\b'       # Explicit joins in WHERE clause (a bit tricky and not always accurate)
]

# Function to count joins in a single query
def count_joins_in_query(query):
    count = 0
    for pattern in join_patterns:
        matches = re.findall(pattern, query, re.IGNORECASE | re.DOTALL)
        count += len(matches)
    return count

# Function to split SQL content into individual queries
def split_queries(content):
    queries = []
    statement = ""
    in_string = False
    in_comment = False
    escape = False

    for char in content:
        if char == ";" and not in_string and not in_comment:
            queries.append(statement.strip())
            statement = ""
        else:
            if char == "-" and statement.endswith("-") and not in_string:
                in_comment = True
            elif char == "\n" and in_comment:
                in_comment = False
            elif char == "'" or char == '"':
                if not in_string:
                    in_string = char
                elif in_string == char and not escape:
                    in_string = False
            escape = (char == "\\" and not escape)
            statement += char

    if statement.strip():
        queries.append(statement.strip())

    return queries

# Function to process SQL files in a directory
def process_sql_files(directory):
    join_counts = []
    for filepath in glob.glob(directory + '/*.sql'):
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
            queries = split_queries(content)
            for query in queries:
                join_count = count_joins_in_query(query)
                join_counts.append(join_count)
    return join_counts

# Example usage
if __name__ == "__main__":
    directory = 'path/to/your/sql/files'  # Replace with the directory containing your .sql files
    join_counts = process_sql_files(directory)
    for i, count in enumerate(join_counts):
        print(f'Query {i+1}: {count} joins')
