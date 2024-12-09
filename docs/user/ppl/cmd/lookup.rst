=============
lookup
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Use the ``lookup`` command to do a lookup from another index and add the fields and values from the lookup document to the search result.

Syntax
============
lookup <lookup-index> <lookup-field> [AS <local-lookup-field>] ["," <lookup-field> [AS <local-lookup-field>]]... [overwrite=<bool>] [<source-field> [AS <local-source-field>]] ["," <source-field> [AS <local-source-field>]]...

* lookup-index: mandatory. the name of the lookup index. If more than one is provided, all of them must match.
* lookup-field: mandatory. the name of the lookup field. Must be existing in the lookup-index. It is used to match to a local field (in the current search) to get the lookup document. When there is no lookup document matching it is a no-op. If there is more than one an exception is thrown.
* local-lookup-field: optional. the name of a field in the current search to match against the lookup-field. **Default:** value of lookup-field.
* overwrite: optional. indicates if the values to copy over to the search result from the lookup document should overwrite existing values. If true no existing values are overwritten. **Default:** false.
* source-field: optional. the fields to copy over from the lookup document to the search result. If no such fields are given all fields are copied. **Default:** all fields
* local-source-field: optional. the final name of the field in the search result (if different from the field name in the lookup document). **Default:** value of source-field.

Note: To check if there is a match between the lookup index and the current search result a term and a match query for the field value of lookup-field is performed.

Example 1: Simple lookup
=============================

The example shows a simple lookup to add the name of a person from a lookup index.

PPL query::

    os> source=account_data;
    fetched rows / total rows = 2/2
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 13               | F        |
    +------------------+----------+

    os> source=hr;
    fetched rows / total rows = 2/2
    +------------------+----------+
    | account_number   | name     |
    |------------------+----------|
    | 1                | John     |
    | 13               | Alice    |
    +------------------+----------+

    os> source=account_data | lookup hr account_number;
    fetched rows / total rows = 2/2
    +------------------+----------+----------+
    | account_number   | gender   | name     |
    |------------------+----------|----------|
    | 1                | M        | John     |
    | 13               | F        | Alice    |
    +------------------+----------+----------+


Example 2: Lookup with different field names
============================================

The example show a lookup to add the name of a person from a lookup index with different field names.

PPL query::

    os> source=account_data;
    fetched rows / total rows = 2/2
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 13               | F        |
    +------------------+----------+

    os> source=hr;
    fetched rows / total rows = 2/2
    +------------------+----------+
    | employee_number  | name     |
    |------------------+----------|
    | 1                | John     |
    | 13               | Alice    |
    +------------------+----------+

    os> source=account_data | lookup hr employee_number AS account_number name AS given_name;
    fetched rows / total rows = 2/2
    +------------------+----------+----------------+
    | account_number   | gender   | given_name     |
    |------------------+----------|----------------|
    | 1                | M        | John           |
    | 13               | F        | Alice          |
    +------------------+----------+----------------+

Example 3: Full lookup example
==============================

The example show a lookup to add the name of a person from a lookup index with different field names.

PPL query::

    os> source=account_data;
    fetched rows / total rows = 4/4
    +------------------+----------+------------+------------------+
    | account_number   | gender   | department | name             |
    |------------------+----------+------------+------------------|
    | 1                | M        | finance    | John Miller      |
    | 13               | F        | it         | Melinda Williams |
    | 20               | M        | it         | NULL             |
    | 21               | F        | finance    | Mandy Smith      |
    +------------------+----------+------------+------------------+

    os> source=hr;
    fetched rows / total rows = 5/5
    +------------------+--------------+------------+--------+
    | employee_number  | name         | dep        | active |
    |------------------+--------------|------------|--------|
    | 1                | John n/a     | finance    | true   |
    | 13               | Alice n/a    | finance    | false  |
    | 13               | Melinda n/a  | it         | true   |
    | 19               | Jack n/a     | finance    | true   |
    | 21               | NULL         | finance    | false  |
    +------------------+--------------+------------+--------+

    os> source=account_data | lookup hr employee_number AS account_number, dep AS department overwrite=true;
    fetched rows / total rows = 4/4
    +------------------+----------+------------------+------------+-----------+---------+-----------------+
    | account_number   | gender   | name             | department | active    | dep     | employee_number |
    |------------------+----------|------------------|------------|-----------|---------|-----------------|
    | 1                | M        | John Miller      | finance    | true      | finance | 1               |
    | 13               | F        | Melinda Williams | it         | true      | it      | 13              |
    | 20               | M        | NULL             | it         | NULL      | NULL    | NULL            |
    | 21               | F        | Mandy Smith      | it         | NULL      | it      | 21              |
    +------------------+----------+------------------+------------+-----------+---------+-----------------+

    os> source=account_data | lookup hr employee_number AS account_number, dep AS department overwrite=false;
    fetched rows / total rows = 4/4
    +------------------+----------+------------------+------------+-----------+---------+-----------------+
    | account_number   | gender   | name             | department | active    | dep     | employee_number |
    |------------------+----------|------------------|------------|-----------|---------|-----------------|
    | 1                | M        | John n/a         | finance    | true      | finance | 1               |
    | 13               | F        | Melinda /na      | it         | true      | it      | 13              |
    | 20               | M        | NULL             | it         | NULL      | NULL    | NULL            |
    | 21               | F        | Mandy Smith      | it         | NULL      | it      | 21              |
    +------------------+----------+------------------+------------+-----------+---------+-----------------+


Limitation
==========
The ``lookup`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
