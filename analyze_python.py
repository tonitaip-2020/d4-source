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
    # RawSQL: SQLite, PostgreSQL, MySQL, Oracle, DB2, SQL Server, MariaDB
    'RawSQL': r'import sqlite3|from sqlite3|import psycopg2|from psycopg2|import MySQLdb|from MySQLdb|import pymysql|from pymysql|import cx_Oracle|from cx_Oracle|import pyodbc|from pyodbc|import ibm_db|from ibm_db|import mysql.connector|from mysql.connector|import asyncpg|from asyncpg|import pymssql|from pymssql|import mariadb|from mariadb|import fdb|from fdb',

    # ORMs and other abstraction layers
    'SQLAlchemy': r'import sqlalchemy|from sqlalchemy|import flask_sqlalchemy|from flask_sqlalchemy',
    'Django ORM': r'from django.db import models|import django.db',
    'PonyORM': r'import pony.orm|from pony.orm',
    'Tortoise-ORM': r'import tortoise|from tortoise',
    'Peewee': r'import peewee|from peewee',
    'Orator': r'import orator|from orator',
    'Dataset': r'import dataset|from dataset',
    'SQLObject': r'import sqlobject|from sqlobject',

    # NoSQL databases
    'MongoDB': r'import pymongo|from pymongo|import mongoengine|from mongoengine',
    'CouchDB': r'import couchdb|from couchdb',
    'Redis': r'import redis|from redis|import aioredis|from aioredis|import django_redis|from django_redis|import simplekv|from simplekv',
    'Cassandra': r'import cassandra|from cassandra',
    'ScyllaDB': r'import cassandra|from cassandra',
    'HBase': r'import happybase|from happybase',
    'Neo4J': r'import neo4j|from neo4j|import py2neo|from py2neo',
    'Gremlin': r'import gremlin_python|from gremlin_python',
    'InfluxDB': r'import influxdb|from influxdb',

    # Embedded databases
    'OpenTSDB': r'import opentsdb|from opentsdb',
    'BerkeleyDB': r'import bsddb3|from bsddb3',
    'LevelDB': r'import plyvel|from plyvel',
    'LMDB': r'import lmdb|from lmdb',
    'GDBM': r'import dbm.gnu|from dbm.gnu',
    'TDB': r'import tdb|from tdb',
    'PStore': r'import shelve|from shelve',

    # Others
    # Ouf of scope for d4
    #'Google Firestore': r'import google.cloud.firestore|from google.cloud.firestore',
    #'Google Bigtable': r'import google.cloud.bigtable|from google.cloud.bigtable',
    #'Amazon DynamoDB': r'import boto3|from boto3',
    #'Microsoft Azure CosmosDB': r'import azure.cosmos|from azure.cosmos',
    #'RabbitMQ': r'import pika|from pika',
    #'Apache Kafka': r'import confluent_kafka|from confluent_kafka',
    #'ActiveMQ': r'import stomp|from stomp',
    #'ZeroMQ': r'import zmq|from zmq',
    #'NATS': r'import nats|from nats',
    #'Elasticsearch': r'import elasticsearch|from elasticsearch',
    #'Apache Solr': r'import pysolr|from pysolr',
    #'Google BigQuery': r'import google.cloud.bigquery|from google.cloud.bigquery',
    #'DataLoader': r'import dataloader|from dataloader',
    #'GraphQL': r'import graphene|from graphene',
    #'Apollo Client': r'import apollo|from apollo'
}

# File path for CSV output
csv_file_path = 'python.csv'

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
data_stream = load_dataset("codeparrot/github-code", data_files={'train': 'data/train-0*-of-01126.parquet'}, split='train', filter_languages=True, languages=["Python"], num_proc=24)

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
