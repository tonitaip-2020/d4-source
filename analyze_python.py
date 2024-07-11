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
    'Raw SQL': r'import sqlite3|from sqlite3|import psycopg2|from psycopg2|import MySQLdb|from MySQLdb|import pymysql|from pymysql|import cx_Oracle|from cx_Oracle|import pyodbc|from pyodbc|import ibm_db|from ibm_db',
    'SQLAlchemy': r'import sqlalchemy|from sqlalchemy',
    'Django ORM': r'import django.db|from django.db',
    'Peewee': r'import peewee|from peewee',
    'Tortoise ORM': r'import tortoise|from tortoise',
    'SQLObject': r'import sqlobject|from sqlobject',
    'Pony ORM': r'import pony.orm|from pony.orm',
    'MongoEngine': r'import mongoengine|from mongoengine|djongo',
    'PyMongo': r'import pymongo|from pymongo',
    'Redis': r'import redis|from redis|redis',
    'CouchDB': r'couchdb',
    'Cassandra': r'import cassandra|from cassandra',
    'Neo4J': r'import neo4j|from neo4j',
    'OrientDB': r'import pyorient|from pyorient',
    'Flask-SQLAlchemy': r'import flask_sqlalchemy|from flask_sqlalchemy',
    'Storm ORM': r'import storm|from storm',
    'Dataset': r'import dataset|from dataset',
}

# File path for CSV output
csv_file_path = 'python.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["Python"], num_proc=24)

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
            'SQLAlchemy': 0,
            'Django ORM': 0,
            'Peewee': 0,
            'Tortoise ORM': 0,
            'SQLObject': 0,
            'Pony ORM': 0,
            'MongoEngine': 0,
            'PyMongo': 0,
            'Redis': 0,
            'Cassandra': 0,
            'Neo4J': 0,
            'OrientDB': 0,
            'Flask-SQLAlchemy': 0,
            'Storm ORM': 0,
            'Dataset': 0,
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
        total = 7226626 # Python
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
