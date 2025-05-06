===========
Identifiers
===========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Introduction
============

Identifiers are used for naming your database objects, such as index name, field name, alias etc. Basically there are two types of identifiers: regular identifiers and delimited identifiers.


Regular Identifiers
===================

Description
-----------

According to ANSI SQL standard, a regular identifier is a string of characters that must start with ASCII letter (lower or upper case). The subsequent character can be a combination of letter, digit, underscore (``_``). It cannot be a reversed key word. And whitespace and other special characters are not allowed. Additionally in our SQL parser, we make extension to the rule for OpenSearch storage as shown in next sub-section.

Extensions
----------

For OpenSearch, the following identifiers are supported extensionally by our SQL parser for convenience (without the need of being delimited as shown in next section):

1. Identifiers prefixed by dot ``.``: this is called hidden index in OpenSearch, for example ``.opensearch_dashboards``.
2. Identifiers prefixed by at sign ``@``: this is common for meta fields generated in Logstash ingestion.
3. Identifiers with ``-`` in the middle: this is mostly the case for index name with date information.
4. Identifiers with star ``*`` present: this is mostly an index pattern for wildcard match.

Examples
--------

Here are examples for using index pattern directly without quotes::

    os> SELECT * FROM *cc*nts;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+


Delimited Identifiers
=====================

Description
-----------

A delimited identifier is an identifier enclosed in back ticks `````. In this case, the identifier enclosed is not necessarily a regular identifier. In other words, it can contain any special character not allowed by regular identifier.

Please note the difference between single quote and double quotes in SQL syntax. Single quote is used to enclose a string literal while double quotes have same purpose as back ticks to escape special characters in an identifier.

Use Cases
---------

Here are typical examples of the use of delimited identifiers:

1. Identifiers of reserved key word name
2. Identifiers with dot ``.`` present: similarly as ``-`` in index name to include date information, it is required to be quoted so parser can differentiate it from identifier with qualifiers.
3. Identifiers with other special character: OpenSearch has its own rule which allows more special character, for example Unicode character is supported in index name.

Examples
--------

Here are examples for quoting an index name by back ticks::

    os> SELECT * FROM `accounts`;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+


Case Sensitivity
================

Description
-----------

In SQL-92, regular identifiers are case insensitive and converted to upper case automatically just like key word. While characters in a delimited identifier appear as they are. However, in our SQL implementation, identifiers are treated in case sensitive manner. So it must be exactly same as what is stored in OpenSearch which is different from ANSI standard.

Examples
--------

For example, if you run ``SELECT * FROM ACCOUNTS``, it will end up with an index not found exception from our plugin because the actual index name is under lower case.


Identifier Qualifiers
=====================

Description
-----------

An identifier can be qualified by qualifier(s) or not. The qualifier is meant to avoid ambiguity when interpreting the identifier name. Thus, the name symbol can be associated with a concrete field in OpenSearch correctly.

In particular, identifier qualifiers follow the specification as below:

1. **Definitions**: A qualified name consists of multiple individual identifiers separated by dot ``.``. An unqualified name can only be a single identifier.
2. **Qualifier types**: For now, index identifier does not support qualification. Field identifier can be qualified by either full index name or its alias specified in ``FROM`` clause.
3. **Delimitation**: If necessary, delimit identifiers in each part of a qualified name separately. Do not enclose the entire name which would be interpreted as a single identifier mistakenly. For example, use ``"table"."column"`` rather than ``"table.column"``.

Examples
--------

The first example is to show a column name qualified by full table name originally in ``FROM`` clause. The qualifier is optional if no ambiguity::

    os> SELECT city, accounts.age, ABS(accounts.balance) FROM accounts WHERE accounts.age < 30;
    fetched rows / total rows = 1/1
    +-------+-----+--------------+
    | city  | age | ABS(balance) |
    |-------+-----+--------------|
    | Nogal | 28  | 32838        |
    +-------+-----+--------------+

The second example is to show a field name qualified by index alias specified. Similarly, the alias qualifier is optional in this case::

    os> SELECT city, acc.age, ABS(acc.balance) FROM accounts AS acc WHERE acc.age > 30;
    fetched rows / total rows = 3/3
    +--------+-----+------------------+
    | city   | age | ABS(acc.balance) |
    |--------+-----+------------------|
    | Brogan | 32  | 39225            |
    | Dante  | 36  | 5686             |
    | Orick  | 33  | 4180             |
    +--------+-----+------------------+

Note that in both examples above, the qualifier is removed in response. This happens only when identifiers selected is a simple field name. In other cases, expressions rather than an atom field, the column name in response is exactly the same as the text in ``SELECT``clause.

Multiple Indices
================

Description
-----------

To query multiple indices, you could

1. Include ``*`` in index name, this is an index pattern for wildcard match.
2. Delimited multiple indices and seperated them by ``,``. Note: no space allowed between each index.


Examples
---------

Query wildcard indices::

    os> SELECT count(*) as cnt FROM acc*;
    fetched rows / total rows = 1/1
    +-----+
    | cnt |
    |-----|
    | 5   |
    +-----+


Query delimited multiple indices seperated by ``,``::

    os> SELECT count(*) as cnt FROM `accounts,account2`;
    fetched rows / total rows = 1/1
    +-----+
    | cnt |
    |-----|
    | 5   |
    +-----+



Fully Qualified Table Names
===========================

Description
-----------
With the introduction of different datasources along with Opensearch, support for fully qualified table names became compulsory to resolve tables to a datasource.

Format for fully qualified table name.
``<datasourceName>.<schemaName>.<tableName>``

* datasourceName:[Mandatory] Datasource information is mandatory when querying over tables from datasources other than opensearch connector.

* schemaName:[Optional] Schema is a logical abstraction for a group of tables. In the current state, we only support ``default`` and ``information_schema``. Any schema mentioned in the fully qualified name other than these two will be resolved to be part of tableName.

* tableName:[Mandatory] tableName is mandatory.

The current resolution algorithm works in such a way, the old queries on opensearch work without specifying any datasource name.
So queries on opensearch indices doesn't need a fully qualified table name.

Table Name Resolution Algorithm.
--------------------------------

Fully qualified Name is divided into parts based on ``.`` character.

TableName resolution algorithm works in the following manner.

1. Take the first part of the qualified name and resolve it to a datasource from the list of datasources configured.
If it doesn't resolve to any of the datasource names configured, datasource name will default to ``@opensearch`` datasource.

2. Take the first part of the remaining qualified name after capturing the datasource name.
If this part represents any of the supported schemas under datasource, it will resolve to the same otherwise schema name will resolve to ``default`` schema.
Currently ``default`` and ``information_schema`` are the only schemas supported.

3. Rest of the parts are combined to resolve tablename.

** Only table name identifiers are supported with fully qualified names, identifiers used for columns and other attributes doesn't require prefixing with datasource and schema information.**

Examples
--------
Assume [my_prometheus] is the only datasource configured other than default opensearch engine.

1. ``my_prometheus.default.http_requests_total``

datasourceName = ``my_prometheus`` [Is in the list of datasources configured].

schemaName = ``default`` [Is in the list of schemas supported].

tableName = ``http_requests_total``.

2. ``logs.12.13.1``


datasourceName = ``@opensearch`` [Resolves to default @opensearch connector since [my_prometheus] is the only dataSource configured.]

schemaName = ``default`` [No supported schema found, so default to `default`].

tableName = ``logs.12.13.1``.


3. ``my_prometheus.http_requests_total``


datasourceName = ``my_prometheus`` [Is in the list of datasources configured].

schemaName = ``default`` [No supported schema found, so default to `default`].

tableName =  ``http_requests_total``.

4. ``prometheus.http_requests_total``

datasourceName = ``@opensearch`` [Resolves to default @opensearch connector since [my_prometheus] is the only datasource configured.]

schemaName = ``default`` [No supported schema found, so default to `default`].

tableName = ``prometheus.http_requests_total``.

5. ``prometheus.default.http_requests_total.1.2.3``

datasourceName = ``@opensearch`` [Resolves to default @opensearch connector since [my_prometheus] is the only dataSource configured.]

schemaName = ``default`` [No supported schema found, so default to `default`].

tableName = ``prometheus.default.http_requests_total.1.2.3``.
