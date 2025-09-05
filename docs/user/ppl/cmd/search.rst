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

* search: search keyword, which could be ignored.
* index: mandatory. search command must specify which index to query from. The index name can be prefixed by "<cluster name>:" for cross-cluster search.
* search-expression: optional. Search expression that gets converted to OpenSearch query_string function.

Search Expression
=================

The search command supports search expressions that are converted to OpenSearch ``query_string`` function calls.
The search expression syntax supports:

* Text search without field: ``"error"`` - Searches the default field configured by the ``index.query.default_field`` setting (defaults to ``*`` which searches all fields)
* Field-value comparisons: ``field=value``, ``field!=value``, ``field>value``, ``field>=value``, ``field<value``, ``field<=value``
* Boolean operators: ``AND``, ``OR``, ``NOT``
* Grouping with parentheses: ``(expression)``
* IN operator for multiple values: ``field IN (value1, value2, value3)``

Operator Precedence
-------------------
Operators are evaluated in the following order (from highest to lowest precedence):

1. **Parentheses** ``()`` - Highest precedence, evaluated first
2. **NOT** - Unary negation operator
3. **OR** - Logical OR
4. **AND** - Logical AND (lowest precedence)

Examples:

* ``A OR B AND C`` is evaluated as ``(A OR B) AND C``
* ``NOT A OR B`` is evaluated as ``(NOT A) OR B``
* ``A AND B OR C AND D`` is evaluated as ``((A AND B) OR C) AND D``
* Use parentheses to explicitly control evaluation order: ``A OR (B AND C)``

Cross-Cluster Search
====================
Cross-cluster search lets any node in a cluster execute search requests against other clusters. Refer to `Cross-Cluster Search <admin/cross_cluster_search.rst>`_ for configuration.


Example 1: Full-Text Search (Default Field) and Phrase Search
==============================================================

**The search command excels at full-text search**, leveraging OpenSearch's powerful ``query_string`` capabilities. When you search without specifying a field, it searches the default field configured by the ``index.query.default_field`` index setting (defaults to ``*`` which searches all fields).

You can check or modify the default field setting::

    GET /accounts/_settings/index.query.default_field
    
    PUT /accounts/_settings
    {
      "index.query.default_field": "firstname,lastname,email"
    }

**Free-text search** - searching default field::

    os> search "Amber" source=accounts;
    fetched rows / total rows = 1/1
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address         | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+

**Phrase Search** - Double quotes indicate exact phrase matching::

    os> search "Bristol Street" source=accounts;
    fetched rows / total rows = 1/1
    +----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address            | balance | gender | city  | employer | state | age | email                 | lastname |
    |----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------|
    | 6              | Hattie    | 671 Bristol Street | 5686    | M      | Dante | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    +----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------+

**Multiple search terms** with OR::

    os> search "Amber" OR "Nanette" source=accounts;
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address            | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane    | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    | 13             | Nanette   | 789 Madison Street | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                 | Bates    |
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+


Example 2: Fetch all the data
==============================

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

Example 3: Field-Value Pair Matching
=====================================

Search for specific field values with OR conditions::

    os> search firstname="Amber" OR lastname="Duke" source=accounts;
    fetched rows / total rows = 1/1
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address         | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+

Example 4: Range Queries
=========================

Use comparison operators for range queries::

    os> search age>30 AND age<=35 source=accounts;
    fetched rows / total rows = 2/2
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com  | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+

Example 5: Using the IN Operator
=================================

Use IN operator for multiple values on the same field::

    os> search age IN (32, 36, 28) AND city!="Brogan" source=accounts;
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address            | balance | gender | city  | employer | state | age | email                 | lastname |
    |----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------|
    | 6              | Hattie    | 671 Bristol Street | 5686    | M      | Dante | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 13             | Nanette   | 789 Madison Street | 32838   | F      | Nogal | Quility  | VA    | 28  | null                  | Bates    |
    +----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------+

The IN operator is equivalent to multiple OR conditions.

Example 6: Boolean and Comparison Operators
============================================

Combine multiple conditions with AND, OR, and comparison operators::

    os> search (age=32 OR age=36 OR age=28) AND city!="Brogan" AND balance>5000 source=accounts;
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address            | balance | gender | city  | employer | state | age | email                 | lastname |
    |----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------|
    | 6              | Hattie    | 671 Bristol Street | 5686    | M      | Dante | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 13             | Nanette   | 789 Madison Street | 32838   | F      | Nogal | Quility  | VA    | 28  | null                  | Bates    |
    +----------------+-----------+--------------------+---------+--------+-------+----------+-------+-----+-----------------------+----------+

Example 7: NOT vs != Semantics
===============================

Important distinction between NOT and != operators:

!= operator (inequality) - Field must exist and not equal the value
--------------------------------------------------------------------

* Use ``!=`` when you want to exclude events where a field **exists** but has a certain value
* Documents where the field doesn't exist are **NOT** returned

Example::

    os> search employer!="Quility" source=accounts;
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address            | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane    | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+

Returns only documents where:
  - employer field **exists** AND
  - employer field value is **not** "Quility"
  - Note: Dale Adams (account 18) with employer=null is NOT returned

NOT operator (boolean negation) - Excludes all matching conditions
-------------------------------------------------------------------

* Use ``NOT field=value`` when you want to exclude events where a field either exists with that value OR doesn't exist at all
* Documents where the field doesn't exist **ARE** returned

Example::

    os> search NOT employer="Quility" source=accounts;
    fetched rows / total rows = 3/3
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+

Returns all documents **except** those where employer="Quility", including:
  - Documents where employer field has any other value ("Pyrami", etc.)
  - Documents where employer field doesn't exist


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
``+ - && || ! ( ) { } [ ] ^ " ~ : \``.
`*`, `?`, `/` are dealt specially as mentioned in the wildcard pattern matching section.

Examples:

* ``search email="user+test@example.com" source=accounts``
  * Expected output: Documents where email contains the literal text "user+test@example.com"
* ``search address="123 Main St. (Suite #4)" source=accounts``
  * Expected output: Documents where address contains the literal text "123 Main St. (Suite #4)"

Important Notes
===============

Wildcard Pattern Matching
-------------------------
Wildcards are supported for pattern matching in search expressions:

* ``*`` - Matches zero or more characters
* ``?`` - Matches exactly one character

**Wildcard Examples:**

* ``search severityText=ERR* source=logs`` - Matches ERROR, ERROR2, ERROR3, etc.
* ``search name=John? source=accounts`` - Matches Johns, Johny, etc. (exactly 5 characters)
* ``search email=*@example.com source=accounts`` - Matches any email ending with @example.com
* ``search fail* source=logs`` - Free text search for words starting with "fail"

**Escaping for Literal Matching:**

To search for literal ``*``, ``?``, or ``\`` characters, you must escape them:

.. list-table:: 
   :widths: 30 35 35
   :header-rows: 1

   * - Search For
     - PPL Syntax
     - Example
   * - ``prefix*`` (wildcard)
     - ``field=prefix*``
     - Matches "prefix", "prefixABC", etc.
   * - ``prefix*`` (literal)
     - ``field="prefix\\*"``
     - Matches exactly "prefix*"
   * - ``user?`` (wildcard)
     - ``field=user?``
     - Matches "user1", "userA", etc.
   * - ``user?`` (literal)
     - ``field="user\\?"``
     - Matches exactly "user?"

**Important Considerations:**
* Wildcards work on both keyword and text fields
* For text fields, wildcards apply to individual tokens after analysis
* Leading wildcards (``*term``) may impact performance on large datasets

Field Types and Search Behavior
--------------------------------

Each field type in OpenSearch has specific capabilities and limitations when used with the search command:

**Text Fields**
~~~~~~~~~~~~~~~
* **Support**: Full-text search, phrase search, boolean operators, analyzed tokens
* **Limitations**: 
  - Wildcards (``*``, ``?``) apply to individual tokens after analysis, not the entire field value
  - Case-insensitive by default due to analysis
  - Exact matching requires using ``.keyword`` subfield if available
* **Example**: ``search address="Bristol" source=accounts`` matches "671 Bristol Street"

**Keyword Fields**
~~~~~~~~~~~~~~~~~~
* **Support**: Exact matching, wildcard patterns, case-sensitive comparison
* **Limitations**: 
  - No text analysis (searches must match exactly including case)
  - Wildcards work on the entire field value
  - No relevance scoring for text search
* **Example**: ``search status.keyword="ACTIVE" source=logs``

**Numeric Fields** (integer, long, float, double)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* **Support**: Range queries (``>``, ``<``, ``>=``, ``<=``), exact matching, IN operator
* **Limitations**: 
  - No wildcard support
  - No text search capabilities
  - Values must be numeric (no quotes needed)
* **Example**: ``search age>=18 AND balance<50000 source=accounts``

**Date Fields**
~~~~~~~~~~~~~~~
* **Support**: Range queries, exact matching, date math expressions
* **Limitations**: 
  - No wildcard support
  - Must use proper date format or date math
  - Time zone considerations may apply
* **Example**: ``search timestamp>="2024-01-01" AND timestamp<"2024-02-01" source=logs``

**Boolean Fields**
~~~~~~~~~~~~~~~~~~
* **Support**: Exact matching with true/false values
* **Limitations**: 
  - No wildcard or range query support
  - Only accepts ``true`` or ``false`` values
* **Example**: ``search active=true source=users``

**IP Fields**
~~~~~~~~~~~~~
* **Support**: Exact matching, CIDR notation, range queries
* **Limitations**: 
  - No wildcard support for partial IP matching
  - Must use valid IP format
  - CIDR notation support depends on field mapping
* **Example**: ``search client_ip="192.168.1.0/24" source=logs`` or ``search host_ip="192.168.1.100" source=network``

**Nested Fields**
~~~~~~~~~~~~~~~~~
* **Support**: Dot notation for accessing nested fields
* **Limitations**: 
  - Complex nested queries may require special syntax
  - Array handling varies by field type
* **Example**: ``search user.email="*@example.com" source=accounts``

**Multi-fields**
~~~~~~~~~~~~~~~~
* **Support**: Access to different analyzers via subfields
* **Common pattern**: 
  - ``fieldname`` for analyzed text search
  - ``fieldname.keyword`` for exact matching
* **Example**: ``search city="new york" source=accounts`` vs ``search city.keyword="New York" source=accounts``

.. note::
   Field type limitations are important to understand for optimal query performance. Using the appropriate field type for your search pattern ensures accurate results and better performance.
