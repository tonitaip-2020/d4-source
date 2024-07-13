CREATE TABLE c (
  repo_name     TEXT
, num_files     INT
, languages     TEXT
, raw_sql       INT
, odbc          INT
, berkeleydb    INT
, leveldb       INT
, lmdb          INT
, redis         INT
, mongodb       INT
, cassandra     INT
, neo4j         INT
);

\copy c
FROM 'c.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE c_sharp (
  repo_name        TEXT
, num_files        INT
, languages        TEXT
, raw_sql          INT
, entity_framework INT
, dapper           INT
, nhibernate       INT
, ormlite          INT
, linq             INT --check that these are right
, mongodb          INT
, redis            INT
, cassandra        INT
, neo4j            INT
, orientdb         INT
, ravendb          INT
, litedb           INT
);

\copy c_sharp
FROM 'c#.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE c_plus_plus (
  repo_name  TEXT
, num_files  INT
, languages  TEXT
, raw_sql    INT
, odbc       INT
, berkeleydb INT
, leveldb    INT
, lmdb       INT
, redis      INT
, mongodb    INT
, cassandra  INT
, neo4j      INT
, orientdb   INT
);

\copy c_plus_plus
FROM 'c++.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE go (
  repo_name TEXT
, num_files INT
, languages TEXT
, raw_sql   INT
, gorm      INT
, xorm      INT
, upper     INT
, qbs       INT
, sqlx      INT
, pgx       INT
, gorp      INT
, entgo     INT
, redis     INT
, mongodb   INT
, cassandra INT
, neo4j     INT
, orientdb  INT
, none      INT --for extra data, ignore
);

\copy go
FROM 'go.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE java (
  repo_name    TEXT
, num_files    INT
, languages    TEXT
, raw_sql      INT
, hibernate    INT
, jpa          INT
, jooq         INT
, mybatis      INT
, spring_jpa   INT
, querydsl     INT
, jdbctemplate INT
, ebean_orm    INT
, criteria_api INT
, spring_jdbc  INT
, cayenne      INT
, openjpa      INT
, redis        INT
, mongodb      INT
, cassandra    INT
, neo4j        INT
, objectdb     INT
, eclipselink  INT
, batoo_jpa    INT
, speedment    INT
, orientdb     INT
);

\copy java
FROM 'java.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE javascript (
  repo_name    TEXT
, num_files    INT
, languages    TEXT
, raw_sql      INT
, sequelize    INT
, knex         INT
, typeorm      INT
, bookshelf    INT
, objection_js INT
, waterline    INT
, massive_js   INT
, redis        INT
, mongodb      INT
, cassandra    INT
, neo4j        INT
, orientdb     INT
);

\copy javascript
FROM 'javascript.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE php (
  repo_name     TEXT
, num_files     INT
, languages     TEXT
, raw_sql       INT
, doctrine_orm  INT
, illuminate    INT
, propel        INT
, active_record INT
, querybuilder  INT
, laravel       INT
, pdo           INT
, mongodb       INT
, redis         INT
, cassandra     INT
, neo4j         INT
, soap          INT
, xml_rpc       INT
, graphql       INT
);

\copy php
FROM 'php.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE python (
  repo_name     TEXT
, num_files     INT
, languages     TEXT
, raw_sql       INT
, sqlalchemy    INT
, django_orm    INT
, peewee        INT
, tortoise_orm  INT
, sqlobject     INT
, pony_orm      INT
, mongoengine   INT
, pymongo       INT
, redis         INT
, couchdb       INT
, cassandra     INT
, neo4j         INT
, orientdb      INT
, flask_sqlalc  INT
, storm_orm     INT
, dataset       INT
);

\copy python
FROM 'python.csv'
DELIMITER ','
HEADER CSV;

CREATE TABLE ruby (
  repo_name     TEXT
, num_files     INT
, languages     TEXT
, raw_sql       INT
, ruby_on_rails INT
, activerecord  INT
, sequel        INT
, datamapper    INT
, mongodb       INT
, redis         INT
, cassandra     INT
, neo4j         INT
, orientdb      INT
, ravendb       INT
, litedb        INT
);

\copy ruby
FROM 'ruby.csv'
DELIMITER ','
HEADER CSV;
