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
describe <index>

* index: mandatory. describe command must specify which index to query from.


Example
=======

The example describes accounts index.

PPL query::

    os> describe accounts;
    fetched rows / total rows = 11/11
    +----------------+---------------+--------------+----------------+-------------+-------------+---------------+-----------------+------------------+------------------+------------+-----------+--------------+-----------------+--------------------+---------------------+--------------------+---------------+-----------------+----------------+---------------+--------------------+--------------------+----------------------+
    | TABLE_CAT      | TABLE_SCHEM   | TABLE_NAME   | COLUMN_NAME    | DATA_TYPE   | TYPE_NAME   | COLUMN_SIZE   | BUFFER_LENGTH   | DECIMAL_DIGITS   | NUM_PREC_RADIX   | NULLABLE   | REMARKS   | COLUMN_DEF   | SQL_DATA_TYPE   | SQL_DATETIME_SUB   | CHAR_OCTET_LENGTH   | ORDINAL_POSITION   | IS_NULLABLE   | SCOPE_CATALOG   | SCOPE_SCHEMA   | SCOPE_TABLE   | SOURCE_DATA_TYPE   | IS_AUTOINCREMENT   | IS_GENERATEDCOLUMN   |
    |----------------+---------------+--------------+----------------+-------------+-------------+---------------+-----------------+------------------+------------------+------------+-----------+--------------+-----------------+--------------------+---------------------+--------------------+---------------+-----------------+----------------+---------------+--------------------+--------------------+----------------------|
    | docTestCluster | null          | accounts     | account_number | null        | long        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 0                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | firstname      | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 1                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | address        | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 2                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | balance        | null        | long        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 3                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | gender         | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 4                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | city           | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 5                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | employer       | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 6                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | state          | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 7                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | age            | null        | long        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 8                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | email          | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 9                  |               | null            | null           | null          | null               | NO                 |                      |
    | docTestCluster | null          | accounts     | lastname       | null        | text        | null          | null            | null             | 10               | 2          | null      | null         | null            | null               | null                | 10                 |               | null            | null           | null          | null               | NO                 |                      |
    +----------------+---------------+--------------+----------------+-------------+-------------+---------------+-----------------+------------------+------------------+------------+-----------+--------------+-----------------+--------------------+---------------------+--------------------+---------------+-----------------+----------------+---------------+--------------------+--------------------+----------------------+

