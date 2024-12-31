
================
Metadata Queries
================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Querying Metadata
=================

Description
-----------

You can query your indices metadata by ``SHOW`` and ``DESCRIBE`` statement. These commands are very useful for database management tool to enumerate all existing indices and get basic information from the cluster.

Syntax
------

``SHOW TABLES LIKE "<pattern>"``

``DESCRIBE TABLES LIKE "<pattern>" [COLUMNS LIKE "<pattern>"]``

Pattern accepts SQL style wildcards where `_` mathes any character and `%` matches any characters.

Example 1: Show All Indices Information
---------------------------------------

``SHOW`` statement lists all indices that match the search pattern. By using wildcard '%', information for all indices in the cluster is returned.

SQL query::

    os> SHOW TABLES LIKE '%'
    fetched rows / total rows = 10/10
    +----------------+-------------+-----------------+------------+---------+----------+------------+-----------+---------------------------+----------------+
    | TABLE_CAT      | TABLE_SCHEM | TABLE_NAME      | TABLE_TYPE | REMARKS | TYPE_CAT | TYPE_SCHEM | TYPE_NAME | SELF_REFERENCING_COL_NAME | REF_GENERATION |
    |----------------+-------------+-----------------+------------+---------+----------+------------+-----------+---------------------------+----------------|
    | docTestCluster | null        | .ql-datasources | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | account2        | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | accounts        | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | apache          | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | books           | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | nested          | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | nyc_taxi        | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | people          | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | weblogs         | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | wildcard        | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    +----------------+-------------+-----------------+------------+---------+----------+------------+-----------+---------------------------+----------------+

Example 2: Show Specific Index Information
------------------------------------------

Here is an example that searches metadata for index name prefixed by 'acc'. Besides index name and pattern with wildcard characters, searching by index alias is also supported. So you can ``SHOW`` or ``DESCRIBE`` an index alias which gives you same result as doing this with an index name.

SQL query::

    os> SHOW TABLES LIKE "acc%"
    fetched rows / total rows = 2/2
    +----------------+-------------+------------+------------+---------+----------+------------+-----------+---------------------------+----------------+
    | TABLE_CAT      | TABLE_SCHEM | TABLE_NAME | TABLE_TYPE | REMARKS | TYPE_CAT | TYPE_SCHEM | TYPE_NAME | SELF_REFERENCING_COL_NAME | REF_GENERATION |
    |----------------+-------------+------------+------------+---------+----------+------------+-----------+---------------------------+----------------|
    | docTestCluster | null        | account2   | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    | docTestCluster | null        | accounts   | BASE TABLE | null    | null     | null       | null      | null                      | null           |
    +----------------+-------------+------------+------------+---------+----------+------------+-----------+---------------------------+----------------+

Example 3: Describe Index Fields Information
--------------------------------------------

``DESCRIBE`` statement lists all fields for indices that can match the search pattern.

SQL query::

    os> DESCRIBE TABLES LIKE 'accounts'
    fetched rows / total rows = 11/11
    +----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
    | TABLE_CAT      | TABLE_SCHEM | TABLE_NAME | COLUMN_NAME    | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH | DECIMAL_DIGITS | NUM_PREC_RADIX | NULLABLE | REMARKS | COLUMN_DEF | SQL_DATA_TYPE | SQL_DATETIME_SUB | CHAR_OCTET_LENGTH | ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG | SCOPE_SCHEMA | SCOPE_TABLE | SOURCE_DATA_TYPE | IS_AUTOINCREMENT | IS_GENERATEDCOLUMN |
    |----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------|
    | docTestCluster | null        | accounts   | account_number | null      | long      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 0                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | firstname      | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 1                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | address        | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 2                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | balance        | null      | long      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 3                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | gender         | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 4                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | city           | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 5                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | employer       | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 6                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | state          | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 7                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | age            | null      | long      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 8                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | email          | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 9                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | lastname       | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 10               |             | null          | null         | null        | null             | NO               |                    |
    +----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+

Example 4: Describe Index With Fields Filter
--------------------------------------------

``DESCRIBE`` statement fields that can match the search pattern for indices that can match the search pattern.

SQL query::

    os> DESCRIBE TABLES LIKE "accounts" COLUMNS LIKE "%name"
    fetched rows / total rows = 2/2
    +----------------+-------------+------------+-------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
    | TABLE_CAT      | TABLE_SCHEM | TABLE_NAME | COLUMN_NAME | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH | DECIMAL_DIGITS | NUM_PREC_RADIX | NULLABLE | REMARKS | COLUMN_DEF | SQL_DATA_TYPE | SQL_DATETIME_SUB | CHAR_OCTET_LENGTH | ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG | SCOPE_SCHEMA | SCOPE_TABLE | SOURCE_DATA_TYPE | IS_AUTOINCREMENT | IS_GENERATEDCOLUMN |
    |----------------+-------------+------------+-------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------|
    | docTestCluster | null        | accounts   | firstname   | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 1                |             | null          | null         | null        | null             | NO               |                    |
    | docTestCluster | null        | accounts   | lastname    | null      | text      | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 10               |             | null          | null         | null        | null             | NO               |                    |
    +----------------+-------------+------------+-------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
