=============
describe
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``describe`` command to query metadata of the index. ``describe`` command could be only used as the first command in the PPL query.


Syntax
============
describe <dataSource>.<schema>.<tablename>

* dataSource: optional. If dataSource is not provided, it resolves to opensearch dataSource.
* schema: optional.  If schema is not provided, it resolves to  default schema.
* tablename: mandatory. describe command must specify which tablename to query from.



Example 1: Fetch all the metadata
=================================

The example describes accounts index.

PPL query::

    os> describe accounts;
    fetched rows / total rows = 11/11
    +----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
    | TABLE_CAT      | TABLE_SCHEM | TABLE_NAME | COLUMN_NAME    | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH | DECIMAL_DIGITS | NUM_PREC_RADIX | NULLABLE | REMARKS | COLUMN_DEF | SQL_DATA_TYPE | SQL_DATETIME_SUB | CHAR_OCTET_LENGTH | ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG | SCOPE_SCHEMA | SCOPE_TABLE | SOURCE_DATA_TYPE | IS_AUTOINCREMENT | IS_GENERATEDCOLUMN |
    |----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------|
    | docTestCluster | null        | accounts   | account_number | null      | bigint    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 0                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | address        | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 1                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | age            | null      | bigint    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 2                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | balance        | null      | bigint    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 3                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | city           | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 4                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | email          | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 5                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | employer       | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 6                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | firstname      | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 7                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | gender         | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 8                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | lastname       | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 9                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | state          | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 10               |             | null          | null         | null        | null             | NO               |                    |
    +----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+

Example 2: Fetch metadata with condition and filter
===================================================

The example retrieves columns with type long in accounts index.

PPL query::

    os> describe accounts | where TYPE_NAME="bigint" | fields COLUMN_NAME;
    fetched rows / total rows = 3/3
    +----------------+
    | COLUMN_NAME    |
    |----------------|
    | account_number |
    | age            |
    | balance        |
    +----------------+


Example 3: Fetch metadata for table in prometheus dataSource
=========================================================

The example retrieves table info for ``prometheus_http_requests_total`` metric in prometheus dataSource.

PPL query::

    os> describe my_prometheus.prometheus_http_requests_total;
    fetched rows / total rows = 6/6
    +---------------+--------------+--------------------------------+-------------+-----------+
    | TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME                     | COLUMN_NAME | DATA_TYPE |
    |---------------+--------------+--------------------------------+-------------+-----------|
    | my_prometheus | default      | prometheus_http_requests_total | handler     | string    |
    | my_prometheus | default      | prometheus_http_requests_total | code        | string    |
    | my_prometheus | default      | prometheus_http_requests_total | instance    | string    |
    | my_prometheus | default      | prometheus_http_requests_total | @timestamp  | timestamp |
    | my_prometheus | default      | prometheus_http_requests_total | @value      | double    |
    | my_prometheus | default      | prometheus_http_requests_total | job         | string    |
    +---------------+--------------+--------------------------------+-------------+-----------+
