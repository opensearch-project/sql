=============
search
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``search`` command to retrieve document from the index. ``search`` command could be only used as the first command in the PPL query.


Syntax
============
search source=[<remote-cluster>:]<index> [search-expression]

* search: search keywords, which could be ignored.
* index: mandatory. search command must specify which index to query from. The index name can be prefixed by "<cluster name>:" for cross-cluster search.
* search-expression: optional. Search expression that gets converted to OpenSearch query_string function.


Cross-Cluster Search
====================
Cross-cluster search lets any node in a cluster execute search requests against other clusters. Refer to `Cross-Cluster Search <admin/cross_cluster_search.rst>`_ for configuration.


Example 1: Fetch all the data
=============================

The example show fetch all the document from accounts index.

PPL query::

    os> source=accounts;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+

Example 2: Fetch data with condition
====================================

The example shows fetching documents from accounts index with a condition.

PPL query::

    os> source=accounts account_number=1 or gender="F";
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address            | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane    | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    | 13             | Nanette   | 789 Madison Street | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                 | Bates    |
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+

Search Expression
=================

The search command supports search expressions that are converted to OpenSearch ``query_string`` function calls.

Syntax
------
The search expression syntax supports:

* Field-value comparisons: ``field=value``, ``field!=value``, ``field>value``, ``field>=value``, ``field<value``, ``field<=value``
* Boolean operators: ``AND``, ``OR``, ``NOT``
* Grouping with parentheses: ``(expression)``
* IN operator for multiple values: ``field IN (value1, value2, value3)``
* Text search without field (searches default field): ``"search term"``

Example 3: Field-Value Pair Matching
=====================================

Search for specific field values with OR conditions::

    os> search firstname="Amber" OR lastname="Duke" source=accounts;

Expected output: Documents where firstname equals "Amber" OR lastname equals "Duke"

Example 4: Boolean and Comparison Operators
============================================

Combine multiple conditions with AND, OR, and comparison operators::

    os> search (age=32 OR age=36 OR age=28) AND city!="Brogan" AND balance>5000 source=accounts;

Expected output: Documents where age is 32, 36, or 28 AND city field exists and is not "Brogan" AND balance is greater than 5000

Example 5: Using the IN Operator
=================================

Use IN operator for multiple values on the same field::

    os> search age IN (32, 36, 28) AND city!="Brogan" source=accounts;

Expected output: Documents where age is 32, 36, or 28 AND city field exists and is not "Brogan"

The IN operator is equivalent to multiple OR conditions.

Example 6: NOT vs != Semantics
===============================

Important distinction between NOT and != operators:

**NOT operator** - Returns everything except where field equals value::

    os> search NOT gender="F" source=accounts;

* Returns all documents except those where gender="F"
* **Includes** documents where gender field doesn't exist

**!= operator** - Returns only documents where field exists AND doesn't equal value::

    os> search gender!="F" source=accounts;

* Returns only documents where gender field exists and is not "F"
* **Excludes** documents where gender field doesn't exist

Example 7: Text Search (Default Field) and Phrase Search
=========================================================

Search text without specifying a field searches the default field::

    os> search "Amber" source=accounts;

Expected output: Documents containing "Amber" in the default search field

**Phrase Search**: Double quotes indicate phrase search::

    os> search "Bristol Street" source=accounts;

Expected output: Documents containing the exact phrase "Bristol Street" in the default search field

Multiple terms with OR::

    os> search "Amber" OR "Nanette" source=accounts;

Expected output: Documents containing either "Amber" or "Nanette" in the default search field

Example 8: Range Queries
=========================

Use comparison operators for range queries::

    os> search age>30 AND age<=35 source=accounts;

Expected output: Documents where age is greater than 30 AND less than or equal to 35

Character Escaping and Phrase Search
=====================================

Phrase Search with Double Quotes
---------------------------------
Double quotes in search values indicate phrase search:

* ``search address="Bristol Street" source=accounts`` 
* Expected output: Documents where address field contains the exact phrase "Bristol Street"

Special Characters
------------------
The following Lucene special characters are automatically escaped in values:
``+ - && || ! ( ) { } [ ] ^ " ~ * ? : \ /``

Examples:

* ``search email="user+test@example.com" source=accounts``
  * Expected output: Documents where email contains the literal text "user+test@example.com"
* ``search address="123 Main St. (Suite #4)" source=accounts``
  * Expected output: Documents where address contains the literal text "123 Main St. (Suite #4)"

Field Names
-----------
* Dots (``.``) in field names are preserved for nested fields
* Example: ``user.name`` accesses the nested field

Important Notes
===============

Wildcard Behavior
-----------------
* Wildcards (``*`` and ``?``) are currently escaped and treated as literal characters
* When wildcard support is enabled, wildcards work only on keyword fields and text fields (text fields may not return expected results as the text is analyzed)
* Text searches without field specification use the default field

Field Types and Search Behavior
--------------------------------
* **Text fields**: Analyzed for full-text search
* **Keyword fields**: Exact value matching
* **Numeric fields**: Support range queries (>, <, >=, <=)
* **Date fields**: Support range queries with date values

Operator Precedence
-------------------
Parentheses are used to ensure proper operator precedence in complex expressions.

