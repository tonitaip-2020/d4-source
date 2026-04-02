#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import sys
from pathlib import Path
from typing import Dict, Pattern

os.environ.setdefault("HF_DATASETS_OFFLINE", "1")

from datasets import load_dataset

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
        "RawSQL": r'#include\s*[<\"]\s*(?:mysql|pqxx|pg_query|mariadb|sqlite|oci|sybfront|sybdb|ibase|odbc)',
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
        "LevelDB": r'#include\s*[<\"]\s*(?:leveldb|LevelDB)',
        "LMDB": r'#include\s*[<\"]\s*lmdb',
        "FoundationDB": r'#include\s*[<\"]\s*fdb_c',
        "HyperLevelDB": r'#include\s*[<\"]\s*hyperleveldb',
        "TinyORM": r'#include\s*[<\"]\s*TinyOrm',
        "ODB": r'#include\s*[<\"]\s*odb/database',
        "sqlpp11": r'#include\s*[<\"]\s*sqlpp11',
    },
    "csharp": {
        "RawSQL": r'\busing\s+(?:System\.Data\.Odbc|System\.Data\.SqlClient|Microsoft\.Data\.SqlClient|MySql\.Data\.MySqlClient|Npgsql|Oracle\.ManagedDataAccess\.Client|FirebirdSql\.Data\.FirebirdClient|IBM\.Data\.DB2|Sybase\.Data\.AseClient|System\.Data\.OleDb|System\.Data\.SQLite)\s*;',
        "OLE DB": r'\busing\s+System\.Data\.OleDb\s*;',
        "Entity Framework": r'\busing\s+(?:Microsoft\.EntityFrameworkCore|System\.Data\.Entity)\s*;',
        "Dapper": r'\busing\s+Dapper\s*;',
        "NHibernate": r'\busing\s+NHibernate\s*;',
        "LINQ": r'\busing\s+(?:System\.Data\.Linq|LinqToDB|System\.Linq)\s*;',
        "ServiceStack OrmLite": r'\busing\s+ServiceStack\.OrmLite\s*;',
        "Telerik Data Access": r'\busing\s+Telerik\.OpenAccess\s*;',
        "RepoDb": r'\busing\s+RepoDb\s*;',
        "SqlKata": r'\busing\s+SqlKata\.Execution\s*;',
        "PetaPoco": r'\busing\s+PetaPoco\s*;',
        "Insight.Database": r'\busing\s+Insight\.Database\s*;',
        "MongoDB": r'\busing\s+MongoDB\.Driver\s*;',
        "Redis": r'\busing\s+StackExchange\.Redis\s*;',
        "Couchbase": r'\busing\s+Couchbase\s*;',
        "RavenDB": r'\busing\s+Raven\.Client\.Documents\s*;',
        "Cassandra": r'\busing\s+Cassandra\s*;',
        "DynamoDB": r'\busing\s+Amazon\.DynamoDBv2\s*;',
        "CosmosDB": r'\busing\s+Microsoft\.Azure\.Cosmos\s*;',
        "CouchDB": r'\busing\s+MyCouch\s*;',
        "LiteDB": r'\busing\s+LiteDB\s*;',
        "OrientDB": r'\busing\s+Orient\.Client\s*;',
        "Neo4j": r'\busing\s+Neo4jClient\s*;',
        "Memcached": r'\busing\s+Enyim\.Caching\s*;',
        "RocksDB.NET": r'\busing\s+RocksDbSharp\s*;',
        "LMDB.NET": r'\busing\s+LightningDB\s*;',
        "InfluxDB": r'\busing\s+InfluxDB\.Client\s*;',
        "TimescaleDB": r'\busing\s+Npgsql\s*;',
        "Gremlin.NET": r'\busing\s+Gremlin\.Net\.Driver\s*;',
        "FluentNHibernate": r'\busing\s+FluentNHibernate\s*;',
        "NPoco": r'\busing\s+NPoco\s*;',
        "DbUp": r'\busing\s+DbUp\s*;',
        "MassTransit": r'\busing\s+MassTransit\s*;',
    },
    "go": {
        "RawSQL": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?(?:"database/sql"|"github\.com/mattn/go-sqlite3"|"github\.com/go-sql-driver/mysql"|"github\.com/jackc/pgx(?:/v\d+)?(?:/stdlib)?"|"github\.com/lib/pq"|"github\.com/microsoft/go-mssqldb"|"github\.com/godror/godror"|"github\.com/ibmdb/go_ibm_db"|"github\.com/nakagami/firebirdsql"|"github\.com/alexbrainman/odbc")',
        "GORM": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"gorm\.io/gorm"',
        "Ent": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"(?:entgo\.io/ent|.+/ent)"',
        "XORM": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"xorm\.io/xorm"',
        "SQLBoiler": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/volatiletech/sqlboiler',
        "gorm-gen": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/taichi-maker/gorm-gen',
        "Bun": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/uptrace/bun',
        "go-pg": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/go-pg/pg',
        "gorm-adapter": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/casbin/gorm-adapter',
        "MongoDB (go-mongo-driver)": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"(?:go\.mongodb\.org/mongo-driver|gopkg\.in/mgo',
        "CouchDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/go-kivik/kivik',
        "Redis": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"(?:github\.com/redis/go-redis|github\.com/gomodule/redigo)',
        "Olric": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/olric-io/olric',
        "Cassandra": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/gocql/gocql',
        "HBase": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/tsuna/gohbase',
        "Neo4j": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"(?:github\.com/neo4j/neo4j-go-driver|github\.com/neo4j/neo4j-go-driver/v\d+/neo4j)"',
        "Gremlin-Go": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/apache/tinkerpop/gremlin-go',
        "InfluxDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/influxdata/influxdb-client-go',
        "VictoriaMetrics": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/VictoriaMetrics/metrics',
        "BadgerDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/dgraph-io/badger',
        "BoltDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"go\.etcd\.io/bbolt"|(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/etcd-io/bbolt"',
        "LevelDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/syndtr/goleveldb',
        "PebbleDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/cockroachdb/pebble',
        "LMDB": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/bmatsuo/lmdb-go',
        "sqlx": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/jmoiron/sqlx',
        "gorm-dialects": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"gorm\.io/driver/',
        "squirrel": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"github\.com/Masterminds/squirrel',
        "upper.io": r'(?m)^\s*import\s*(?:\(|.)[\s\S]*?"upper\.io/db\.v3',
    },
    "java": {
        "RawSQL": r'\bimport\s+(?:org\.sqlite\.JDBC|org\.h2\.Driver|org\.hsqldb\.jdbc\.JDBCDriver|com\.sybase\.jdbc4\.jdbc\.SybDriver|com\.ibm\.db2\.jcc\.DB2Driver|org\.mariadb\.jdbc\.Driver|com\.microsoft\.sqlserver\.jdbc\.SQLServerDriver|com\.mysql\.cj\.jdbc\.Driver|org\.postgresql\.Driver|oracle\.jdbc\.OracleDriver|java\.sql\.[\w.*]+|sun\.jdbc\.odbc\.[\w.*]+)\s*;',
        "Hibernate": r'\bimport\s+org\.hibernate\.[\w.*]+\s*;',
        "Jakarta Persistence API (JPA)": r'\bimport\s+(?:javax|jakarta)\.persistence\.[\w.*]+\s*;',
        "Spring Data JPA": r'\bimport\s+org\.springframework\.data\.jpa\.[\w.*]+\s*;',
        "EclipseLink": r'\bimport\s+org\.eclipse\.persistence\.jpa\.[\w.*]+\s*;',
        "TopLink": r'\bimport\s+oracle\.toplink\.essentials\.[\w.*]+\s*;',
        "OpenJPA": r'\bimport\s+org\.apache\.openjpa\.persistence\.[\w.*]+\s*;',
        "JOOQ": r'\bimport\s+org\.jooq\.[\w.*]+\s*;',
        "QueryDSL": r'\bimport\s+com\.querydsl\.jpa\.[\w.*]+\s*;',
        "MyBatis": r'\bimport\s+org\.apache\.ibatis\.[\w.*]+\s*;',
        "Apache Cayenne": r'\bimport\s+org\.apache\.cayenne\.[\w.*]+\s*;',
        "JDBI": r'\bimport\s+org\.jdbi\.v3\.[\w.*]+\s*;',
        "Ebean ORM": r'\bimport\s+io\.ebean\.[\w.*]+\s*;',
        "MongoDB": r'\bimport\s+com\.mongodb\.[\w.*]+\s*;',
        "CouchDB": r'\bimport\s+org\.ektorp\.[\w.*]+\s*;',
        "Couchbase": r'\bimport\s+com\.couchbase\.client\.java\.[\w.*]+\s*;',
        "RavenDB": r'\bimport\s+net\.ravendb\.client\.documents\.[\w.*]+\s*;',
        "Redis": r'\bimport\s+(?:redis\.clients\.jedis|org\.redisson)\.[\w.*]+\s*;',
        "Apache Ignite": r'\bimport\s+org\.apache\.ignite\.[\w.*]+\s*;',
        "Hazelcast": r'\bimport\s+com\.hazelcast\.[\w.*]+\s*;',
        "Ehcache": r'\bimport\s+net\.sf\.ehcache\.[\w.*]+\s*;',
        "Memcached": r'\bimport\s+net\.spy\.memcached\.[\w.*]+\s*;',
        "Apache Cassandra": r'\bimport\s+com\.datastax\.oss\.driver\.[\w.*]+\s*;',
        "HBase": r'\bimport\s+org\.apache\.hadoop\.hbase\.[\w.*]+\s*;',
        "ScyllaDB": r'\bimport\s+com\.datastax\.oss\.driver\.[\w.*]+\s*;',
        "Neo4j": r'\bimport\s+org\.neo4j\.driver\.[\w.*]+\s*;',
        "TinkerPop Gremlin": r'\bimport\s+org\.apache\.tinkerpop\.gremlin\.[\w.*]+\s*;',
        "ArangoDB": r'\bimport\s+com\.arangodb\.[\w.*]+\s*;',
        "InfluxDB": r'\bimport\s+org\.influxdb\.[\w.*]+\s*;',
        "TimescaleDB": r'\bimport\s+org\.postgresql\.[\w.*]+\s*;',
        "Berkeley DB": r'\bimport\s+com\.sleepycat\.je\.[\w.*]+\s*;',
        "LevelDB": r'\bimport\s+org\.iq80\.leveldb\.[\w.*]+\s*;',
        "LMDB": r'\bimport\s+org\.lmdbjava\.[\w.*]+\s*;',
        "Derby (JavaDB)": r'\bimport\s+org\.apache\.derby\.jdbc\.[\w.*]+\s*;',
        "Apache Phoenix": r'\bimport\s+org\.apache\.phoenix\.jdbc\.[\w.*]+\s*;',
        "JTA": r'\bimport\s+(?:javax|jakarta)\.transaction\.[\w.*]+\s*;',
        "Spring Data JDBC": r'\bimport\s+org\.springframework\.data\.jdbc\.[\w.*]+\s*;',
        "Apache Commons DbUtils": r'\bimport\s+org\.apache\.commons\.dbutils\.[\w.*]+\s*;',
    },
    "javascript": {
        "RawSQL": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\'](?:mysql|mysql2|pg|pg-promise|sqlite[^\"\']*|better-sqlite[^\"\']*|oracledb|mariadb|ibm_db)[\"\']\s*\)?',
        "Sequelize": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']sequelize[\"\']\s*\)?',
        "TypeORM": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']typeorm[\"\']\s*\)?',
        "Objection.js": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']objection[\"\']\s*\)?',
        "Knex.js": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']knex[\"\']\s*\)?',
        "Prisma": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']@prisma/client[\"\']\s*\)?',
        "Bookshelf.js": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']bookshelf[\"\']\s*\)?',
        "Waterline": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']waterline[\"\']\s*\)?',
        "Massive.js": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']massive[\"\']\s*\)?',
        "Kysely": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']kysely[\"\']\s*\)?',
        "MongoDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\'](?:mongodb|mongoose)[\"\']\s*\)?',
        "CouchDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']nano[\"\']\s*\)?',
        "PouchDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']pouchdb[\"\']\s*\)?',
        "Redis": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\'](?:redis|ioredis)[\"\']\s*\)?',
        "Keyv": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']keyv[\"\']\s*\)?',
        "Cassandra": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']cassandra-driver[\"\']\s*\)?',
        "ScyllaDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']cassandra-driver[\"\']\s*\)?',
        "HBase": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']hbase[\"\']\s*\)?',
        "Neo4J": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']neo4j-driver[\"\']\s*\)?',
        "Gremlin-JS": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']gremlin[\"\']\s*\)?',
        "InfluxDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']@influxdata/influxdb-client[\"\']\s*\)?',
        "NeDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']nedb[\"\']\s*\)?',
        "LowDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']lowdb[\"\']\s*\)?',
        "LokiJS": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']lokijs[\"\']\s*\)?',
        "TaffyDB": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']taffy[\"\']\s*\)?',
        "DataLoader": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']dataloader[\"\']\s*\)?',
        "GraphQL.js": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']graphql[\"\']\s*\)?',
        "Apollo Client": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']@apollo/client[\"\']\s*\)?',
        "Hasura": r'\b(?:import\s+.+?\s+from\s+|require\s*\()\s*[\"\']hasura[\"\']\s*\)?',
    },
    "php": {
        "RawSQL": r'\b(?:mysql_query|mysqli|pg_query|pgsql|sqlite|sqlsrv|oci8|ibm_db2|interbase|odbc|PDO)\b',
        "Doctrine ORM": r'\b(?:use\s+)?Doctrine\\ORM\\',
        "Eloquent (Illuminate)": r'\b(?:use\s+)?Illuminate\\Database\\',
        "Propel ORM": r'\b(?:use\s+)?Propel\\',
        "Cycle ORM": r'\b(?:use\s+)?Cycle\\ORM\\',
        "RedBeanPHP": r'\b(?:use\s+)?RedBeanPHP\\R\b|\bR::',
        "Idiorm": r'\b(?:use\s+)?Idiorm\\|\bORM::',
        "Paris": r'\b(?:use\s+)?Paris\\',
        "Medoo": r'\b(?:use\s+)?Medoo\\',
        "Atlas ORM": r'\b(?:use\s+)?Atlas\\ORM\\',
        "DBAL": r'\b(?:use\s+)?Doctrine\\DBAL\\',
        "AURA SQL": r'\b(?:use\s+)?Aura\\Sql\\',
        "CakePHP ORM": r'\b(?:use\s+)?Cake\\ORM\\',
        "FuelPHP ORM": r'\b(?:use\s+)?Fuel\\Core\\DB|\bOrm_',
        "Zend DB": r'\b(?:use\s+)?(?:Laminas|Zend)\\Db\\',
        "MongoDB": r'\b(?:use\s+)?MongoDB\\',
        "CouchDB": r'\bcouchdb\b|\bCouchRest\b',
        "Redis (phpredis)": r'\b(?:Redis|Predis)\b',
        "Memcache": r'\bMemcache(?:d)?\b',
        "Cassandra": r'\bDatastax\\|\bCassandra\\',
        "ScyllaDB": r'\bDatastax\\php-driver\b|\bCassandra\\',
        "Neo4J": r'\bNeo4j\b|graphaware\\neo4j-php-client',
        "Gremlin-PHP": r'pomm-project\\foundation|\bGremlin\b',
        "InfluxDB": r'\bInfluxDB\\',
        "Berkeley DB": r'\bDB4PHP\b',
        "LevelDB": r'\bLevelDB\b',
        "LMDB": r'\bLMDB\b',
        "GDBM": r'\bGDBM\b',
        "SOAP": r'\bSoapClient\b',
        "XML-RPC": r'\bxmlrpc_(?:encode_request|server_call_method)\b',
        "GraphQL": r'\bGraphQL\\(?:Query|Mutation)\b',
    },
    "python": {
        "RawSQL": r'\b(?:import|from)\s+(?:sqlite3|psycopg2|MySQLdb|pymysql|cx_Oracle|pyodbc|ibm_db|mysql\.connector|asyncpg|pymssql|mariadb|fdb)\b',
        "SQLAlchemy": r'\b(?:import|from)\s+(?:sqlalchemy|flask_sqlalchemy)\b',
        "Django ORM": r'\b(?:from\s+django\.db\s+import\s+models|import\s+django\.db)\b',
        "PonyORM": r'\b(?:import|from)\s+pony\.orm\b',
        "Tortoise-ORM": r'\b(?:import|from)\s+tortoise\b',
        "Peewee": r'\b(?:import|from)\s+peewee\b',
        "Orator": r'\b(?:import|from)\s+orator\b',
        "Dataset": r'\b(?:import|from)\s+dataset\b',
        "SQLObject": r'\b(?:import|from)\s+sqlobject\b',
        "MongoDB": r'\b(?:import|from)\s+(?:pymongo|mongoengine)\b',
        "CouchDB": r'\b(?:import|from)\s+couchdb\b',
        "Redis": r'\b(?:import|from)\s+(?:redis|aioredis|django_redis|simplekv)\b',
        "Cassandra": r'\b(?:import|from)\s+cassandra\b',
        "ScyllaDB": r'\b(?:import|from)\s+cassandra\b',
        "HBase": r'\b(?:import|from)\s+happybase\b',
        "Neo4J": r'\b(?:import|from)\s+(?:neo4j|py2neo)\b',
        "Gremlin": r'\b(?:import|from)\s+gremlin_python\b',
        "InfluxDB": r'\b(?:import|from)\s+influxdb\b',
        "OpenTSDB": r'\b(?:import|from)\s+opentsdb\b',
        "BerkeleyDB": r'\b(?:import|from)\s+bsddb3\b',
        "LevelDB": r'\b(?:import|from)\s+plyvel\b',
        "LMDB": r'\b(?:import|from)\s+lmdb\b',
        "GDBM": r'\b(?:import|from)\s+dbm\.gnu\b',
        "TDB": r'\b(?:import|from)\s+tdb\b',
        "PStore": r'\b(?:import|from)\s+shelve\b',
    },
    "ruby": {
        "RawSQL": r'\b(?:require|gem)\s*[\("\']+(?:sqlite3|pg|mysql2|oci8|tiny_tds|ibm_db|fb|odbc)[\)"\']|\bPG::Connection\b',
        "Ruby on Rails": r'\bRails\b|(?:require|gem)\s*[\("\']+rails[\)"\']',
        "ActiveRecord": r'\bActiveRecord\b|(?:require|gem)\s*[\("\']+active_record[\)"\']',
        "Sequel": r'\bSequel\b|(?:require|gem)\s*[\("\']+sequel[\)"\']',
        "ROM.rb": r'\bROM\b|(?:require|gem)\s*[\("\']+(?:rom|rom-sql)[\)"\']',
        "DataMapper": r'\bDataMapper\b|(?:require|gem)\s*[\("\']+data_mapper[\)"\']',
        "Hanami::Model": r'\bHanami::Model\b|(?:require|gem)\s*[\("\']+hanami/model[\)"\']',
        "Trailblazer::Reform": r'\bReform\b|(?:require|gem)\s*[\("\']+reform[\)"\']',
        "Arel": r'\bArel\b|(?:require|gem)\s*[\("\']+arel[\)"\']',
        "MongoDB": r'\bMongo(?:id|::Client)?\b|(?:require|gem)\s*[\("\']+(?:mongo|mongoid)[\)"\']',
        "CouchDB": r'(?:require|gem)\s*[\("\']+couchrest[\)"\']',
        "Redis": r'\bRedis\b|(?:require|gem)\s*[\("\']+(?:redis|redic|ohm|moneta)[\)"\']',
        "Cassandra": r'\bCassandra\b|(?:require|gem)\s*[\("\']+cassandra[\)"\']',
        "HBase": r'(?:require|gem)\s*[\("\']+hbase[\)"\']',
        "Neo4j": r'\bNeo4j\b|(?:require|gem)\s*[\("\']+neo4j[\)"\']',
        "Gremlin.rb": r'(?:require|gem)\s*[\("\']+gremlin[\)"\']',
        "InfluxDB": r'\bInfluxDB\b|(?:require|gem)\s*[\("\']+influxdb[\)"\']',
        "LevelDB": r'\bLevelDB\b|(?:require|gem)\s*[\("\']+leveldb[\)"\']',
        "LMDB": r'\bLMDB\b|(?:require|gem)\s*[\("\']+lmdb[\)"\']',
        "GDBM": r'\bGDBM\b|(?:require|gem)\s*[\("\']+gdbm[\)"\']',
        "TDB": r'(?:require|gem)\s*[\("\']+tdb[\)"\']',
        "PStore": r'\bPStore\b|(?:require|gem)\s*[\("\']+pstore[\)"\']',
    },
}

ALIASES = {
    "c": "c",
    "cpp": "cpp",
    "c++": "cpp",
    "csharp": "csharp",
    "c#": "csharp",
    "cs": "csharp",
    "go": "go",
    "java": "java",
    "javascript": "javascript",
    "js": "javascript",
    "ts": "javascript",
    "php": "php",
    "python": "python",
    "py": "python",
    "ruby": "ruby",
    "rb": "ruby",
}

HF_LANGUAGE_NAMES = {
    "c": "C",
    "cpp": "C++",
    "csharp": "C#",
    "go": "GO",
    "java": "Java",
    "javascript": "JavaScript",
    "php": "PHP",
    "python": "Python",
    "ruby": "Ruby",
}

TOTALS = {
    "c": 1328934,
    "cpp": 7380520,
    "csharp": 6811652,
    "go": 2265436,
    "java": 19548190,
    "javascript": 11839883,
    "php": 11177610,
    "python": 7226626,
    "ruby": 4473331,
}


def canonical_language(name: str) -> str:
    key = name.strip().lower()
    if key not in ALIASES:
        valid = ", ".join(sorted(ALIASES))
        raise ValueError(f"Unsupported language '{name}'. Supported values: {valid}")
    return ALIASES[key]


def compile_patterns(language: str) -> Dict[str, Pattern[str]]:
    return {name: re.compile(pattern, re.MULTILINE) for name, pattern in PATTERNS[language].items()}


def ensure_csv(path: Path, fieldnames: list[str]) -> None:
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()


def write_all_repo_data(csv_path: Path, patterns: Dict[str, str], repo_info: dict[str, dict[str, object]]) -> None:
    if not repo_info:
        return
    fieldnames = ["repo_name", "num_files", "languages"] + list(patterns.keys())
    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        for repo_name, info in repo_info.items():
            if any(int(info[method]) > 0 for method in patterns):
                writer.writerow({
                    "repo_name": repo_name,
                    "num_files": info["num_files"],
                    "languages": ", ".join(sorted(info["languages"])),
                    **{method: info[method] for method in patterns},
                })


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze local github-code parquet files for DB/query library usage by language.")
    parser.add_argument("language", help="Language to analyze, e.g. ruby, python, c++, c#")
    parser.add_argument(
        "--data-files",
        default="data/train-0*-of-01126.parquet",
        help="Local parquet glob or path passed to datasets.load_dataset (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path. Defaults to <language>.csv",
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        default=24,
        help="Number of worker processes for local dataset loading (default: %(default)s)",
    )
    parser.add_argument(
        "--flush-repos",
        type=int,
        default=1000,
        help="Write partial results after this many repos are buffered (default: %(default)s)",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=None,
        help="Override expected total file count shown in progress output",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        language = canonical_language(args.language)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    output_path = Path(args.output) if args.output else Path(f"{language}.csv")
    fieldnames = ["repo_name", "num_files", "languages"] + list(PATTERNS[language].keys())
    ensure_csv(output_path, fieldnames)

    compiled_patterns = compile_patterns(language)
    hf_language = HF_LANGUAGE_NAMES[language]
    total = args.total if args.total is not None else TOTALS.get(language)

    repo_info: dict[str, dict[str, object]] = {}
    count = 0
    job_start_time = dt.datetime.now()
    loop_start_time = dt.datetime.now()

    dataset = load_dataset(
        "codeparrot/github-code",
        data_files={"train": args.data_files},
        split="train",
        filter_languages=True,
        languages=[hf_language],
        num_proc=args.num_proc,
    )

    for obj in dataset:
        repo_name = obj["repo_name"]
        lang_value = obj["language"]
        code = obj.get("code") or ""

        info = repo_info.setdefault(
            repo_name,
            {"num_files": 0, "languages": set(), **{method: 0 for method in PATTERNS[language]}},
        )
        info["num_files"] += 1
        info["languages"].add(lang_value)

        for method, cre in compiled_patterns.items():
            info[method] += len(cre.findall(code))

        if len(repo_info) >= args.flush_repos:
            write_all_repo_data(output_path, PATTERNS[language], repo_info)
            repo_info.clear()

        count += 1
        if count % 10000 == 0:
            loop_end_time = dt.datetime.now()
            loop_duration = loop_end_time - loop_start_time
            formatted_duration = f"{loop_duration.total_seconds():.2f}"
            if total is not None:
                left = f"{max(total - count, 0):,}"
                per = round(100 - count / total * 100, 2)
                print(
                    f"STAT: {left} files left ({per}%). Running for {dt.datetime.now() - job_start_time}.",
                    end=" ",
                )
            else:
                print(f"STAT: Processed {count:,} files. Running for {dt.datetime.now() - job_start_time}.", end=" ")
            print(f"This loop took {formatted_duration} seconds.")
            loop_start_time = dt.datetime.now()

    write_all_repo_data(output_path, PATTERNS[language], repo_info)
    print("END: Data has been written to", output_path, "incrementally.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
