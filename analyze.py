#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime
import glob
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Pattern, Sequence, Tuple

import pyarrow.dataset as ds

PATTERNS: Dict[str, Dict[str, str]] = {
    "c": {
        "RawSQL": r'#include\s*[<\"](sqlite3\.h|mysql/mysql\.h|libpq-fe\.h|oci\.h|ibase\.h|sybfront\.h|sybdb\.h)[\">]|#include\s*[<\"](sql\.h|sqlext\.h)[\">]',
        "Redis": r'#include\s*[<\"]hiredis/hiredis\.h[\">]',
        "MongoDB": r'#include\s*[<\"]mongoc\.h[\">]',
        "Cassandra": r'#include\s*[<\"]cassandra\.h[\">]',
        "Neo4J": r'#include\s*[<\"]neo4j-client\.h[\">]',
        "Memcached": r'#include\s*[<\"]libmemcached/memcached\.h[\">]',
        "RocksDB": r'#include\s*[<\"]rocksdb/c\.h[\">]',
        "LevelDB": r'#include\s*[<\"]leveldb/c\.h[\">]',
        "LMDB": r'#include\s*[<\"]lmdb\.h[\">]',
        "FoundationDB": r'#include\s*[<\"]fdb_c\.h[\">]',
        "HyperLevelDB": r'#include\s*[<\"]hyperleveldb/db\.h[\">]',
        "BerkeleyDB": r'#include\s*[<\"]db\.h[\">]',
        "GDBM": r'#include\s*[<\"]gdbm\.h[\">]',
        "KyotoCabinet": r'#include\s*[<\"]kcdb\.h[\">]',
        "TokyoCabinet": r'#include\s*[<\"]tcutil\.h[\">]',
        "UnQLite": r'#include\s*[<\"]unqlite\.h[\">]',
    },
    "cpp": {
        "RawSQL": r'#include\s*[<\"]\s*mysql|#include\s*[<\"]\s*pqxx|#include\s*[<\"]\s*pg_query|#include\s*[<\"]\s*mariadb|#include\s*[<\"]\s*sqlite|#include\s*[<\"]\s*oci|#include\s*[<\"]\s*sybfront|#include\s*[<\"]\s*sydb|#include\s*[<\"]\s*ibase|#include\s*[<\"]\s*oci|#include\s*[<\"]\s*odbc',
        "Redis": r'#include\s*[<\"]\s*hiredis',
        "MongoDB": r'#include\s*[<\"]\s*mongocxx',
        "Cassandra": r'#include\s*[<\"]\s*cassandra',
        "Neo4j": r'#include\s*[<\"]\s*neo4j-client',
        "memcached": r'#include\s*[<\"]\s*memcached',
        "Berkeley DB": r'#include\s*[<\"]\s*db_cxx',
        "Kyoto Cabinet": r'#include\s*[<\"]\s*kcdb',
        "Tokyo Cabinet": r'#include\s*[<\"]\s*tcutil',
        "UnQLite": r'#include\s*[<\"]\s*unqlite',
        "GDBM": r'#include\s*[<\"]\s*gdbm',
        "RocksDB": r'#include\s*[<\"]\s*rocksdb',
        "LevelDB": r'#include\s*[<\"]\s*leveldb|#include\s*[<\"]\s*LevelDB',
        "LMDB": r'#include\s*[<\"]\s*lmdb',
        "FoundationDB": r'#include\s*[<\"]\s*fdb_c',
        "HyperLevelDB": r'#include\s*[<\"]\s*hyperleveldb',
        "TinyORM": r'#include\s*[<\"]\s*TinyOrm',
        "ODB": r'#include\s*[<\"]\s*odb/database',
        "sqlpp11": r'#include\s*[<\"]\s*sqlpp11',
    },
    "csharp": {
        "RawSQL": r'using\s+System\.Data\.Odbc|using\s+System\.Data\.SqlClient|using\s+Microsoft\.Data\.SqlClient|using\s+MySql\.Data\.MySqlClient|using\s+Npgsql|using\s+Oracle\.ManagedDataAccess\.Client|using\s+FirebirdSql\.Data\.FirebirdClient|using\s+IBM\.Data\.DB2|using\s+Sybase\.Data\.AseClient|using\s+System\.Data\.OleDb|using\s+System\.Data\.SQLite',
        "OLE DB": r'using\s+System\.Data\.OleDb',
        "Entity Framework": r'using\s+Microsoft\.EntityFrameworkCore|using\s+System\.Data\.Entity',
        "Dapper": r'using\s+Dapper',
        "NHibernate": r'using\s+NHibernate',
        "LINQ": r'using\s+System\.Data\.Linq|using\s+LinqToDB|linq|Linq',
        "ServiceStack OrmLite": r'using\s+ServiceStack\.OrmLite',
        "Telerik Data Access": r'using\s+Telerik\.OpenAccess',
        "RepoDb": r'using\s+RepoDb',
        "SqlKata": r'using\s+SqlKata\.Execution',
        "PetaPoco": r'using\s+PetaPoco',
        "Insight.Database": r'using\s+Insight\.Database',
        "MongoDB": r'using\s+MongoDB\.Driver',
        "Redis": r'using\s+StackExchange\.Redis',
        "Couchbase": r'using\s+Couchbase',
        "RavenDB": r'using\s+Raven\.Client\.Documents',
        "Cassandra": r'using\s+Cassandra',
        "DynamoDB": r'using\s+Amazon\.DynamoDBv2',
        "CosmosDB": r'using\s+Microsoft\.Azure\.Cosmos',
        "CouchDB": r'using\s+MyCouch',
        "LiteDB": r'using\s+LiteDB',
        "OrientDB": r'using\s+Orient\.Client',
        "Neo4j": r'using\s+Neo4jClient',
        "Memcached": r'using\s+Enyim\.Caching',
        "RocksDB.NET": r'using\s+RocksDbSharp',
        "LMDB.NET": r'using\s+LightningDB',
        "InfluxDB": r'using\s+InfluxDB\.Client',
        "TimescaleDB": r'using\s+Npgsql',
        "Gremlin.NET": r'using\s+Gremlin\.Net\.Driver',
        "FluentNHibernate": r'using\s+FluentNHibernate',
        "NPoco": r'using\s+NPoco',
        "DbUp": r'using\s+DbUp',
        "MassTransit": r'using\s+MassTransit',
    },
    "go": {
        "RawSQL": r'(database/sql)|(go\-pg)|mattn/go-sqlite3|go-sql-driver/mysql|jackc/pgx|lib/pq|microsoft/go-mssqldb|go-sql-driver/mysql|godror/godror|ibmdb/go_ibm_db|nakagami/firebirdsql|alexbrainman/odbc',
        "GORM": r'gorm.io/gorm',
        "Ent": r'"(?:entgo\.io/ent|/ent)"',
        "XORM": r'xorm.io/xorm',
        "SQLBoiler": r'volatiletech/sqlboiler',
        "gorm-gen": r'taichi-maker/gorm-gen',
        "Bun": r'uptrace/bun',
        "go-pg": r'go-pg/pg',
        "gorm-adapter": r'casbin/gorm-adapter',
        "MongoDB (go-mongo-driver)": r'go.mongodb.org/mongo-driver|gopkg.in/mgo',
        "CouchDB": r'go-kivik/kivik',
        "Redis": r'redis/go-redis|gomodule/redigo',
        "Olric": r'olric-io/olric',
        "Cassandra": r'gocql/gocql',
        "HBase": r'tsuna/gohbase',
        "Neo4j": r'neo4j-go-driver',
        "Gremlin-Go": r'apache/tinkerpop/gremlin-go',
        "InfluxDB": r'influxdata/influxdb-client-go',
        "VictoriaMetrics": r'VictoriaMetrics/metrics',
        "BadgerDB": r'dgraph-io/badger',
        "BoltDB": r'etcd-io/bbolt',
        "LevelDB": r'syndtr/goleveldb',
        "PebbleDB": r'cockroachdb/pebble',
        "LMDB": r'bmatsuo/lmdb-go',
        "sqlx": r'jmoiron/sqlx',
        "gorm-dialects": r'gorm.io/driver',
        "squirrel": r'Masterminds/squirrel',
        "upper.io": r'upper.io/db.v3',
    },
    "java": {
        "RawSQL": r'org\.sqlite\.JDBC|org\.h2\.Driver|org\.hsqldb\.jdbc\.JDBCDriver|com\.sybase\.jdbc4\.jdbc\.SybDriver|com\.ibm\.db2\.jcc\.DB2Driver|org\.mariadb\.jdbc\.Driver|org\.mariadb\.jdbc\.Driver|com\.microsoft\.sqlserver\.jdbc\.SQLServerDriver|com\.mysql\.cj\.jdbc\.Driver|org\.postgresql\.Driver|oracle\.jdbc\.OracleDriver|java\.sql\..*|sun\.jdbc\.odbc\..*',
        "Hibernate": r'org\.hibernate\.SessionFactory',
        "Jakarta Persistence API (JPA)": r'javax\.persistence\..*',
        "Spring Data JPA": r'org\.springframework\.data\.jpa\.repository\.JpaRepository',
        "EclipseLink": r'org\.eclipse\.persistence\.jpa\.JpaEntityManager',
        "TopLink": r'oracle\.toplink\.essentials\..*',
        "OpenJPA": r'org\.apache\.openjpa\.persistence\..*',
        "JOOQ": r'org\.jooq\.DSLContext',
        "QueryDSL": r'com\.querydsl\.jpa\.impl\.JPAQueryFactory',
        "MyBatis": r'org\.apache\.ibatis\.session\.SqlSession',
        "Apache Cayenne": r'org\.apache\.cayenne\.ObjectContext',
        "JDBI": r'org\.jdbi\.v3\.core\.Jdbi',
        "Ebean ORM": r'io\.ebean\..*',
        "MongoDB": r'com\.mongodb\.client\.MongoClient',
        "CouchDB": r'org\.ektorp\.CouchDbConnector',
        "Couchbase": r'com\.couchbase\.client\.java\.Cluster',
        "RavenDB": r'net\.ravendb\.client\.documents\.DocumentStore',
        "Redis": r'redis\.clients\.jedis\.Jedis|org\.redisson\.api\.RedissonClient',
        "Apache Ignite": r'org\.apache\.ignite\.Ignite',
        "Hazelcast": r'com\.hazelcast\.core\.HazelcastInstance',
        "Ehcache": r'net\.sf\.ehcache\.CacheManager',
        "Memcached": r'net\.spy\.memcached\.MemcachedClient',
        "Apache Cassandra": r'com\.datastax\.oss\.driver\.api\.core\.CqlSession',
        "HBase": r'org\.apache\.hadoop\.hbase\.client\.Connection',
        "ScyllaDB": r'com\.datastax\.oss\.driver\.api\.core\.CqlSession',
        "Neo4j": r'org\.neo4j\.driver\.Driver',
        "TinkerPop Gremlin": r'org\.apache\.tinkerpop\.gremlin\.driver\.Cluster',
        "ArangoDB": r'com\.arangodb\.ArangoDB',
        "InfluxDB": r'org\.influxdb\.InfluxDB',
        "TimescaleDB": r'org\.postgresql\.Driver',
        "Berkeley DB": r'com\.sleepycat\.je\.Database',
        "LevelDB": r'org\.iq80\.leveldb\.DB',
        "LMDB": r'org\.lmdbjava\.Env',
        "Derby (JavaDB)": r'org\.apache\.derby\.jdbc\.EmbeddedDriver',
        "Apache Phoenix": r'org\.apache\.phoenix\.jdbc\.PhoenixDriver',
        "JTA": r'javax\.transaction\..*',
        "Spring Data JDBC": r'org\.springframework\.data\.jdbc\.repository\.config\.EnableJdbcRepositories',
        "Apache Commons DbUtils": r'org\.apache\.commons\.dbutils\.QueryRunner',
    },
    "javascript": {
        "RawSQL": r"'mysql'|'mysql2'|'pg'|'pg-promise'|'sqlite.*?'|'better-sqlite.*?'|'oracledb'|'mariadb'|'ibm_db'",
        "Sequelize": r"'sequelize'",
        "TypeORM": r"'typeorm'",
        "Objection.js": r"'objection'",
        "Knex.js": r"'knex'",
        "Prisma": r"'@prisma/client'",
        "Bookshelf.js": r"'bookshelf'",
        "Waterline": r"'waterline'",
        "Massive.js": r"'massive'",
        "Kysely": r"'kysely'",
        "MongoDB": r"'mongodb'|'mongoose'",
        "CouchDB": r"'nano'",
        "PouchDB": r"'pouchdb'",
        "Redis": r"'redis'|'ioredis'",
        "Keyv": r"'keyv'",
        "Cassandra": r"'cassandra-driver'",
        "ScyllaDB": r"'cassandra-driver'",
        "HBase": r"'hbase'",
        "Neo4J": r"'neo4j-driver'",
        "Gremlin-JS": r"'gremlin'",
        "InfluxDB": r"'@influxdata/influxdb-client'",
        "NeDB": r"'nedb'",
        "LowDB": r"'lowdb'",
        "LokiJS": r"'lokijs'",
        "TaffyDB": r"'taffy'",
        "DataLoader": r"'dataloader'",
        "GraphQL.js": r"'graphql'",
        "Apollo Client": r"'@apollo/client'",
        "Hasura": r"'hasura'",
    },
    "php": {
        "RawSQL": r'mysql_query|mysqli|pg_query|pgsql|sqlite|sqlsrv|oci8|ibm_db2|interbase|odbc|PDO',
        "Doctrine ORM": r'Doctrine\\ORM',
        "Eloquent (Illuminate)": r'Illuminate\\Database',
        "Propel ORM": r'Propel',
        "Cycle ORM": r'cycle\\orm',
        "RedBeanPHP": r'redbeanphp\\rb',
        "Idiorm": r'j4mie\\idiorm',
        "Paris": r'j4mie\\paris',
        "Medoo": r'catfan\\medoo',
        "Atlas ORM": r'atlas\\orm',
        "DBAL": r'doctrine\\dbal',
        "AURA SQL": r'aura\\sql',
        "CakePHP ORM": r'cakephp\\orm',
        "FuelPHP ORM": r'fuelphp\\orm',
        "Zend DB": r'laminas\\laminas-db',
        "MongoDB": r'mongodb\\mongodb',
        "CouchDB": r'couchdb',
        "Redis (phpredis)": r'redis|predis',
        "Memcache": r'memcache',
        "Cassandra": r'datastax\\php-driver',
        "ScyllaDB": r'datastax\\php-driver|cassandra',
        "Neo4J": r'graphaware\\neo4j-php-client|Neo4j',
        "Gremlin-PHP": r'pomm-project\\foundation',
        "InfluxDB": r'influxdb\\influxdb-php',
        "Berkeley DB": r'DB4PHP',
        "LevelDB": r'LevelDB',
        "LMDB": r'LMDB',
        "GDBM": r'GDBM',
        "SOAP": r'SoapClient',
        "XML-RPC": r'xmlrpc_encode_request|xmlrpc_server_call_method',
        "GraphQL": r'GraphQL\\Query|GraphQL\\Mutation',
    },
    "python": {
        "RawSQL": r'import sqlite3|from sqlite3|import psycopg2|from psycopg2|import MySQLdb|from MySQLdb|import pymysql|from pymysql|import cx_Oracle|from cx_Oracle|import pyodbc|from pyodbc|import ibm_db|from ibm_db|import mysql.connector|from mysql.connector|import asyncpg|from asyncpg|import pymssql|from pymssql|import mariadb|from mariadb|import fdb|from fdb',
        "SQLAlchemy": r'import sqlalchemy|from sqlalchemy|import flask_sqlalchemy|from flask_sqlalchemy',
        "Django ORM": r'from django.db import models|import django.db',
        "PonyORM": r'import pony.orm|from pony.orm',
        "Tortoise-ORM": r'import tortoise|from tortoise',
        "Peewee": r'import peewee|from peewee',
        "Orator": r'import orator|from orator',
        "Dataset": r'import dataset|from dataset',
        "SQLObject": r'import sqlobject|from sqlobject',
        "MongoDB": r'import pymongo|from pymongo|import mongoengine|from mongoengine',
        "CouchDB": r'import couchdb|from couchdb',
        "Redis": r'import redis|from redis|import aioredis|from aioredis|import django_redis|from django_redis|import simplekv|from simplekv',
        "Cassandra": r'import cassandra|from cassandra',
        "ScyllaDB": r'import cassandra|from cassandra',
        "HBase": r'import happybase|from happybase',
        "Neo4J": r'import neo4j|from neo4j|import py2neo|from py2neo',
        "Gremlin": r'import gremlin_python|from gremlin_python',
        "InfluxDB": r'import influxdb|from influxdb',
        "OpenTSDB": r'import opentsdb|from opentsdb',
        "BerkeleyDB": r'import bsddb3|from bsddb3',
        "LevelDB": r'import plyvel|from plyvel',
        "LMDB": r'import lmdb|from lmdb',
        "GDBM": r'import dbm.gnu|from dbm.gnu',
        "TDB": r'import tdb|from tdb',
        "PStore": r'import shelve|from shelve',
    },
    "ruby": {
        "RawSQL": r'\b(?:require|gem)\s*[("\']+(?:sqlite3|pg|mysql2|oci8|tiny_tds|ibm_db|fb|odbc)[)"\']|\bPG::Connection\b',
        "Ruby on Rails": r'\bRails\b|(?:require|gem)\s*[("\']+rails[)"\']',
        "ActiveRecord": r'\bActiveRecord\b|(?:require|gem)\s*[("\']+active_record[)"\']',
        "Sequel": r'\bSequel\b|(?:require|gem)\s*[("\']+sequel[)"\']',
        "ROM.rb": r'\bROM\b|(?:require|gem)\s*[("\']+(?:rom|rom-sql)[)"\']',
        "DataMapper": r'\bDataMapper\b|(?:require|gem)\s*[("\']+data_mapper[)"\']',
        "Hanami::Model": r'\bHanami::Model\b|(?:require|gem)\s*[("\']+hanami/model[)"\']',
        "Trailblazer::Reform": r'\bReform\b|(?:require|gem)\s*[("\']+reform[)"\']',
        "Arel": r'\bArel\b|(?:require|gem)\s*[("\']+arel[)"\']',
        "MongoDB": r'\bMongo(?:id|::Client)?\b|(?:require|gem)\s*[("\']+(?:mongo|mongoid)[)"\']',
        "CouchDB": r'(?:require|gem)\s*[("\']+couchrest[)"\']',
        "Redis": r'\bRedis\b|(?:require|gem)\s*[("\']+(?:redis|redic|ohm|moneta)[)"\']',
        "Cassandra": r'\bCassandra\b|(?:require|gem)\s*[("\']+cassandra[)"\']',
        "HBase": r'(?:require|gem)\s*[("\']+hbase[)"\']',
        "Neo4j": r'\bNeo4j\b|(?:require|gem)\s*[("\']+neo4j[)"\']',
        "Gremlin.rb": r'(?:require|gem)\s*[("\']+gremlin[)"\']',
        "InfluxDB": r'\bInfluxDB\b|(?:require|gem)\s*[("\']+influxdb[)"\']',
        "LevelDB": r'\bLevelDB\b|(?:require|gem)\s*[("\']+leveldb[)"\']',
        "LMDB": r'\bLMDB\b|(?:require|gem)\s*[("\']+lmdb[)"\']',
        "GDBM": r'\bGDBM\b|(?:require|gem)\s*[("\']+gdbm[)"\']',
        "TDB": r'(?:require|gem)\s*[("\']+tdb[)"\']',
        "PStore": r'\bPStore\b|(?:require|gem)\s*[("\']+pstore[)"\']',
    },
}

EXTENSIONS: Dict[str, Tuple[str, ...]] = {
    "c": (".c", ".h"),
    "cpp": (".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx"),
    "csharp": (".cs",),
    "go": (".go",),
    "java": (".java",),
    "javascript": (".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx"),
    "php": (".php",),
    "python": (".py",),
    "ruby": (".rb",),
}

TOTALS = {
    "c": 2286711,
    "cpp": 7289750,
    "csharp": 6650395,
    "go": 5498422,
    "java": 12090508,
    "javascript": 28578134,
    "php": 8637557,
    "python": 16218668,
    "ruby": 4473331,
}

ALIASES = {
    "c#": "csharp",
    "cs": "csharp",
    "csharp": "csharp",
    "c++": "cpp",
    "cpp": "cpp",
    "js": "javascript",
    "javascript": "javascript",
    "ts": "javascript",
    "py": "python",
    "python": "python",
    "rb": "ruby",
    "ruby": "ruby",
    "php": "php",
    "java": "java",
    "go": "go",
    "c": "c",
}


def canonical_language(name: str) -> str:
    key = name.strip().lower()
    if key not in ALIASES:
        valid = ", ".join(sorted(set(ALIASES)))
        raise ValueError(f"Unsupported language '{name}'. Supported values: {valid}")
    return ALIASES[key]


def compile_patterns(language: str) -> List[Tuple[str, Pattern[str]]]:
    return [(pattern_name, re.compile(pattern)) for pattern_name, pattern in PATTERNS[language].items()]


def ensure_csv_header(csv_file_path: str, method_names: Iterable[str]) -> None:
    if Path(csv_file_path).exists():
        return
    fieldnames = ["repo_name", "num_files", "languages"] + list(method_names)
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()


def write_all_repo_data(csv_file_path: str, method_names: Sequence[str], repo_info: Dict[str, Dict[str, object]]) -> None:
    if not repo_info:
        return

    fieldnames = ["repo_name", "num_files", "languages"] + list(method_names)
    with open(csv_file_path, mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        for repo_name, info in repo_info.items():
            if any(int(info[method]) > 0 for method in method_names):
                writer.writerow({
                    "repo_name": repo_name,
                    "num_files": info["num_files"],
                    "languages": ", ".join(sorted(info["languages"])),
                    **{method: info[method] for method in method_names},
                })


def iter_parquet_files(pattern: str) -> List[str]:
    return sorted([match for match in glob.glob(pattern) if Path(match).is_file()])


def path_matches_language(path_value: str, extensions: Tuple[str, ...]) -> bool:
    lower = path_value.lower()
    return lower.endswith(tuple(ext.lower() for ext in extensions))


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan local github-code parquet files using d4-source regex patterns.")
    parser.add_argument("language", help="Language name, e.g. ruby, python, c++")
    parser.add_argument("--data-files", default="data/train-0*-of-01126.parquet", help="Glob for local parquet files")
    parser.add_argument("--flush-repos", type=int, default=1000, help="Flush CSV after this many repos are buffered")
    parser.add_argument("--total", type=int, default=None, help="Expected total file count for progress reporting")
    parser.add_argument("--progress-every", type=int, default=10000, help="Print progress every N matching source files")
    parser.add_argument("--batch-size", type=int, default=8192, help="Arrow batch size")
    args = parser.parse_args()

    try:
        language = canonical_language(args.language)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    parquet_files = iter_parquet_files(args.data_files)
    if not parquet_files:
        print(f"Error: no parquet files matched: {args.data_files}", file=sys.stderr)
        return 1

    compiled_patterns = compile_patterns(language)
    extensions = EXTENSIONS[language]
    csv_file_path = f"{language}.csv"
    method_names = [name for name, _ in compiled_patterns]
    ensure_csv_header(csv_file_path, method_names)

    total = args.total if args.total is not None else TOTALS.get(language)
    repo_info: Dict[str, Dict[str, object]] = {}
    count = 0
    job_start_time = datetime.datetime.now()
    loop_start_time = datetime.datetime.now()

    dataset = ds.dataset(parquet_files, format="parquet")
    schema_names = set(dataset.schema.names)
    required = {"repo_name", "path", "content"}
    if not required.issubset(schema_names):
        print(
            f"Error: parquet schema is missing required columns. Found: {sorted(schema_names)}",
            file=sys.stderr,
        )
        return 1

    scanner = dataset.scanner(columns=["repo_name", "path", "content"], batch_size=args.batch_size)

    for batch in scanner.to_batches():
        columns = batch.to_pydict()
        repo_names = columns["repo_name"]
        paths = columns["path"]
        contents = columns["content"]

        for repo_name, path_value, content in zip(repo_names, paths, contents):
            if repo_name is None or path_value is None or content is None:
                continue
            if not path_matches_language(str(path_value), extensions):
                continue

            repo_name = str(repo_name)
            if repo_name not in repo_info:
                repo_info[repo_name] = {
                    "num_files": 0,
                    "languages": {language},
                    **{method: 0 for method, _ in compiled_patterns},
                }

            info = repo_info[repo_name]
            info["num_files"] += 1
            info["languages"].add(language)

            text = str(content)
            for method, cre in compiled_patterns:
                info[method] += len(cre.findall(text))

            count += 1
            if len(repo_info) >= args.flush_repos:
                write_all_repo_data(csv_file_path, method_names, repo_info)
                repo_info.clear()

            if args.progress_every > 0 and count % args.progress_every == 0:
                loop_end_time = datetime.datetime.now()
                loop_duration = loop_end_time - loop_start_time
                formatted_duration = f"{loop_duration.total_seconds():.2f}"
                if total:
                    left = f"{max(total - count, 0):,}"
                    per = round(100 - count / total * 100, 2)
                    print(
                        f"STAT: {left} files left ({per}%). Running for {datetime.datetime.now() - job_start_time}.",
                        end=" ",
                    )
                else:
                    print(f"STAT: {count:,} matching source files scanned. Running for {datetime.datetime.now() - job_start_time}.", end=" ")
                print(f"This loop took {formatted_duration} seconds.")
                loop_start_time = datetime.datetime.now()

    write_all_repo_data(csv_file_path, method_names, repo_info)
    print(f"END: Data has been written to {csv_file_path} incrementally.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
