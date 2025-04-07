import csv
import os
from pathlib import Path

def analyze_csv_file(csv_file):
    results = []
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        method_columns = reader.fieldnames[3:]  # Assuming first 3 are repo_name, num_files, languages
        repo_count = 0
        file_count = 0
        method_usage = {method: 0 for method in method_columns}

        for row in reader:
            repo_count += 1
            try:
                file_count += int(row['num_files'])
            except ValueError:
                continue  # Skip if num_files is not an integer

            for method in method_columns:
                try:
                    if int(row[method]) > 0:
                        method_usage[method] += 1
                except (ValueError, KeyError):
                    continue  # Skip invalid or missing method count

    result_lines = [f"{csv_file.name}:"]
    for method in method_columns:
        count = method_usage[method]
        proportion = (count / repo_count * 100) if repo_count > 0 else 0
        result_lines.append(f"{method},{count},{proportion:.3f}")
    result_lines.append(f"Repositories: {repo_count}")
    result_lines.append(f"Files: {file_count}")
    result_lines.append("")  # Blank line between files
    return result_lines

def main():
    current_dir = Path('.')
    csv_files = current_dir.glob("*.csv")
    output_lines = []

    for csv_file in csv_files:
        output_lines.extend(analyze_csv_file(csv_file))

    with open('summary.txt', 'w', encoding='utf-8') as out_file:
        out_file.write('\n'.join(output_lines))

if __name__ == "__main__":
    main()
