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
* search-expression: optional. Search expression that gets converted to OpenSearch `query_string <https://docs.opensearch.org/latest/query-dsl/full-text/query-string/>`_ function which uses `Lucene Query Syntax <https://lucene.apache.org/core/2_9_4/queryparsersyntax.html>`_.

Search Expression
=================

The search expression syntax supports:

* **Full text search**: ``error`` or ``"error message"`` - Searches the default field configured by the ``index.query.default_field`` setting (defaults to ``*`` which searches all fields)
* **Field-value comparisons**: ``field=value``, ``field!=value``, ``field>value``, ``field>=value``, ``field<value``, ``field<=value``
* **Time modifiers**: ``earliest=timeModifier``, ``latest=timeModifier`` - Filter results by time range using the implicit ``@timestamp`` field
* **Boolean operators**: ``AND``, ``OR``, ``NOT``. Default operator if not specified is ``AND``.
* **Grouping with parentheses**: ``(expression)``
* **IN operator for multiple values**: ``field IN (value1, value2, value3)``
* **Wildcards**: ``*`` (zero or more characters), ``?`` (exactly one character)

**Full Text Search**: Unlike other PPL commands, search supports both quoted and unquoted strings. Unquoted terms are limited to alphanumeric characters, hyphens, underscores, and wildcards. Any other characters require double quotes.

* Unquoted: ``search error``, ``search user-123``, ``search log_*``
* Quoted: ``search "error message"``, ``search "user@example.com"``

**Field Values**: Follow the same quoting rules as search text.

* Unquoted: ``status=active``, ``code=ERR-401``
* Quoted: ``email="user@example.com"``, ``message="server error"``

**Time Modifiers**: Filter search results by time range using the implicit ``@timestamp`` field. Time modifiers support the same formats as the `EARLIEST and LATEST condition functions <https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/functions/condition.rst#earliest>`_:

1. **Current time**: ``now`` or ``now()`` - the current time
2. **Absolute format**: ``MM/dd/yyyy:HH:mm:ss`` or ``yyyy-MM-dd HH:mm:ss``
3. **Unix timestamp**: Numeric values (seconds since epoch) like ``1754020060.123``
4. **Relative format**: ``(+|-)<time_integer><time_unit>[+<...>]@<snap_unit>`` - Time offset from current time

**Relative Time Components**:

* **Time offset**: ``+`` (future) or ``-`` (past)
* **Time amount**: Numeric value + time unit (``second``, ``minute``, ``hour``, ``day``, ``week``, ``month``, ``year``, and their variants)
* **Snap to unit**: Optional ``@<unit>`` to round to nearest unit (hour, day, month, etc.)

**Examples of Time Modifier Values**:

* ``earliest=now`` - From current time
* ``latest='2024-12-31 23:59:59'`` - Until a specific date
* ``earliest=-7d`` - From 7 days ago
* ``latest='+1d@d'`` - Until tomorrow at start of day
* ``earliest='-1month@month'`` - From start of previous month
* ``latest=1754020061`` - Until a unix timestamp (August 1, 2025 03:47:41 at UTC)

Read more details on time modifiers `here <https://github.com/opensearch-project/opensearch-spark/blob/main/docs/ppl-lang/functions/ppl-datetime.md#relative_timestamp>`_.

**Notes:**

* **Column name conflicts**: If your data contains columns named "earliest" or "latest", use backticks to access them as regular fields (e.g., ```earliest`="value"``) to avoid conflicts with time modifier syntax.
* **Time snap syntax**: Time modifiers with chained time offsets must be wrapped in quotes (e.g., ``latest='+1d@month-10h'``) for proper query parsing.

Default Field Configuration
===========================
When you search without specifying a field, it searches the default field configured by the ``index.query.default_field`` index setting (defaults to ``*`` which searches all fields).

You can check or modify the default field setting::

    GET /accounts/_settings/index.query.default_field
    
    PUT /accounts/_settings
    {
      "index.query.default_field": "firstname,lastname,email"
    }

Field Types and Search Behavior
================================

**Text Fields**: Full-text search, phrase search

* ``search message="error occurred" source=logs``

* Limitations: Wildcards apply to terms after analysis, not entire field value.

**Keyword Fields**: Exact matching, wildcard patterns

* ``search status="ACTIVE" source=logs``

* Limitations: No text analysis, case-sensitive matching

**Numeric Fields**: Range queries, exact matching, IN operator

* ``search age>=18 AND balance<50000 source=accounts``

* Limitations: No wildcard or text search support

**Date Fields**: Range queries, exact matching, IN operator

* ``search timestamp>="2024-01-01" source=logs``

* Limitations: Must use index mapping date format, no wildcards

**Boolean Fields**: true/false values only, exact matching, IN operator

* ``search active=true source=users``

* Limitations: No wildcards or range queries

**IP Fields**: Exact matching, CIDR notation

* ``search client_ip="192.168.1.0/24" source=logs``

* Limitations: No wildcards for partial IP matching

**Field Type Performance Tips**:

   * Each field type has specific search capabilities and limitations. Using the wrong field type during ingestion impacts performance and accuracy
   * For wildcard searches on non-keyword fields: Add a keyword field copy for better performance. Example: If you need wildcards on a text field, create ``message.keyword`` alongside ``message``

Cross-Cluster Search
====================
Cross-cluster search lets any node in a cluster execute search requests against other clusters. Refer to `Cross-Cluster Search <admin/cross_cluster_search.rst>`_ for configuration.

Examples
========

Example 1: Text Search
-----------------------------------

**Basic Text Search** (unquoted single term)::

    os> search ERROR source=otellogs | sort @timestamp | fields severityText, body | head 1;
    fetched rows / total rows = 1/1
    +--------------+---------------------------------------------------------+
    | severityText | body                                                    |
    |--------------+---------------------------------------------------------|
    | ERROR        | Payment failed: Insufficient funds for user@example.com |
    +--------------+---------------------------------------------------------+

**Phrase Search** (requires quotes for multi-word exact match)::

    os> search "Payment failed" source=otellogs | fields body;
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | body                                                    |
    |---------------------------------------------------------|
    | Payment failed: Insufficient funds for user@example.com |
    +---------------------------------------------------------+

**Implicit AND with Multiple Terms** (unquoted literals are combined with AND)::

    os> search user email source=otellogs | sort @timestamp | fields body | head 1;
    fetched rows / total rows = 1/1
    +--------------------------------------------------------------------------------------------------------------------+
    | body                                                                                                               |
    |--------------------------------------------------------------------------------------------------------------------|
    | Executing SQL: SELECT * FROM users WHERE email LIKE '%@gmail.com' AND status != 'deleted' ORDER BY created_at DESC |
    +--------------------------------------------------------------------------------------------------------------------+

Note: ``search user email`` is equivalent to ``search user AND email``. Multiple unquoted terms are automatically combined with AND.

**Enclose in double quotes for terms which contain special characters**::

    os> search "john.doe+newsletter@company.com" source=otellogs | fields body;
    fetched rows / total rows = 1/1
    +--------------------------------------------------------------------------------------------------------------------+
    | body                                                                                                               |
    |--------------------------------------------------------------------------------------------------------------------|
    | Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome! Your order #12345 is confirmed' |
    +--------------------------------------------------------------------------------------------------------------------+

**Mixed Phrase and Boolean**::

    os> search "User authentication" OR OAuth2 source=otellogs | sort @timestamp | fields body | head 1;
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------------------------------------------------+
    | body                                                                                                     |
    |----------------------------------------------------------------------------------------------------------|
    | [2024-01-15 10:30:09] production.INFO: User authentication successful for admin@company.org using OAuth2 |
    +----------------------------------------------------------------------------------------------------------+

Example 2: Boolean Logic and Operator Precedence
-------------------------------------------------

**Boolean Operators**::

    os> search severityText="ERROR" OR severityText="FATAL" source=otellogs | sort @timestamp | fields severityText | head 3;
    fetched rows / total rows = 3/3
    +--------------+
    | severityText |
    |--------------|
    | ERROR        |
    | FATAL        |
    | ERROR        |
    +--------------+

    os> search severityText="INFO" AND `resource.attributes.service.name`="cart-service" source=otellogs | fields body | head 1;
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------------------------+
    | body                                                                             |
    |----------------------------------------------------------------------------------|
    | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
    +----------------------------------------------------------------------------------+

**Operator Precedence** (highest to lowest): Parentheses → NOT → OR → AND::

    os> search severityText="ERROR" OR severityText="WARN" AND severityNumber>15 source=otellogs | sort @timestamp | fields severityText, severityNumber | head 2;
    fetched rows / total rows = 2/2
    +--------------+----------------+
    | severityText | severityNumber |
    |--------------+----------------|
    | ERROR        | 17             |
    | ERROR        | 17             |
    +--------------+----------------+

The above evaluates as ``(severityText="ERROR" OR severityText="WARN") AND severityNumber>15``

Example 3: NOT vs != Semantics
-------------------------------

**!= operator** (field must exist and not equal the value)::

    os> search employer!="Quility" source=accounts;
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address            | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane    | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+

**NOT operator** (excludes matching conditions, includes null fields)::

    os> search NOT employer="Quility" source=accounts;
    fetched rows / total rows = 3/3
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+

**Key difference**: ``!=`` excludes null values, ``NOT`` includes them.

Dale Adams (account 18) has ``employer=null``. He appears in ``NOT employer="Quility"`` but not in ``employer!="Quility"``.

Example 4: Wildcards
--------------------

**Wildcard Patterns**::

    os> search severityText=ERR* source=otellogs | sort @timestamp | fields severityText | head 3;
    fetched rows / total rows = 3/3
    +--------------+
    | severityText |
    |--------------|
    | ERROR        |
    | ERROR        |
    | ERROR2       |
    +--------------+

    os> search body=user* source=otellogs | sort @timestamp | fields body | head 2;
    fetched rows / total rows = 2/2
    +----------------------------------------------------------------------------------+
    | body                                                                             |
    |----------------------------------------------------------------------------------|
    | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
    | Payment failed: Insufficient funds for user@example.com                          |
    +----------------------------------------------------------------------------------+

**Wildcard Rules**:

* ``*`` - Matches zero or more characters
* ``?`` - Matches exactly one character

**Single character wildcard (?)**::

    os> search severityText="INFO?" source=otellogs | sort @timestamp | fields severityText | head 3;
    fetched rows / total rows = 3/3
    +--------------+
    | severityText |
    |--------------|
    | INFO2        |
    | INFO3        |
    | INFO4        |
    +--------------+


Example 5: Range Queries
-------------------------

Use comparison operators (>, <, >=, <=) to filter numeric and date fields within specific ranges. Range queries are particularly useful for filtering by age, price, timestamps, or any numeric metrics.

::

    os> search severityNumber>15 AND severityNumber<=20 source=otellogs | sort @timestamp | fields severityNumber | head 3;
    fetched rows / total rows = 3/3
    +----------------+
    | severityNumber |
    |----------------|
    | 17             |
    | 17             |
    | 18             |
    +----------------+

    os> search `attributes.payment.amount`>=1000.0 AND `attributes.payment.amount`<=2000.0 source=otellogs | fields body;
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | body                                                    |
    |---------------------------------------------------------|
    | Payment failed: Insufficient funds for user@example.com |
    +---------------------------------------------------------+

Example 6: Field Search with Wildcards
---------------------------------------

When searching in text or keyword fields, wildcards enable partial matching. This is particularly useful for finding records where you only know part of the value. Note that wildcards work best with keyword fields, while text fields may produce unexpected results due to tokenization.

**Partial Search in Keyword Fields**::

    os> search employer=Py* source=accounts | fields firstname, employer;
    fetched rows / total rows = 1/1
    +-----------+----------+
    | firstname | employer |
    |-----------+----------|
    | Amber     | Pyrami   |
    +-----------+----------+

**Combining Wildcards with Field Comparisons**::

    os> search firstname=A* AND age>30 source=accounts | fields firstname, age, city;
    fetched rows / total rows = 1/1
    +-----------+-----+--------+
    | firstname | age | city   |
    |-----------+-----+--------|
    | Amber     | 32  | Brogan |
    +-----------+-----+--------+

**Important Notes on Wildcard Usage**:

* **Keyword fields**: Best for wildcard searches - exact value matching with pattern support
* **Text fields**: Wildcards apply to individual tokens after analysis, not the entire field value
* **Performance**: Leading wildcards (e.g., ``*@example.com``) are slower than trailing wildcards
* **Case sensitivity**: Keyword field wildcards are case-sensitive unless normalized during indexing

Example 7: IN Operator and Field Comparisons
---------------------------------------------

The IN operator efficiently checks if a field matches any value from a list. This is cleaner and more performant than chaining multiple OR conditions for the same field.

**IN Operator**::

    os> search severityText IN ("ERROR", "WARN", "FATAL") source=otellogs | sort @timestamp | fields severityText | head 3;
    fetched rows / total rows = 3/3
    +--------------+
    | severityText |
    |--------------|
    | ERROR        |
    | WARN         |
    | FATAL        |
    +--------------+

**Field Comparison Examples**::

    os> search severityNumber=17 source=otellogs | sort @timestamp | fields body | head 1;
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | body                                                    |
    |---------------------------------------------------------|
    | Payment failed: Insufficient funds for user@example.com |
    +---------------------------------------------------------+

    os> search `attributes.user.email`="user@example.com" source=otellogs | fields body;
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | body                                                    |
    |---------------------------------------------------------|
    | Payment failed: Insufficient funds for user@example.com |
    +---------------------------------------------------------+

Example 8: Complex Expressions
-------------------------------

Combine multiple conditions using boolean operators and parentheses to create sophisticated search queries.

::

    os> search (severityText="ERROR" OR severityText="WARN") AND severityNumber>10 source=otellogs | sort @timestamp | fields severityText | head 3;
    fetched rows / total rows = 3/3
    +--------------+
    | severityText |
    |--------------|
    | ERROR        |
    | WARN         |
    | ERROR        |
    +--------------+

    os> search `attributes.user.email`="user@example.com" OR (`attributes.error.code`="INSUFFICIENT_FUNDS" AND severityNumber>15) source=otellogs | fields body;
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | body                                                    |
    |---------------------------------------------------------|
    | Payment failed: Insufficient funds for user@example.com |
    +---------------------------------------------------------+

Example 9: Time Modifiers
--------------------------

Time modifiers filter search results by time range using the implicit ``@timestamp`` field. They support various time formats for precise temporal filtering.

**Absolute Time Filtering**::

    os> search earliest='2024-01-15 10:30:05' latest='2024-01-15 10:30:10' source=otellogs | fields @timestamp, severityText;
    fetched rows / total rows = 6/6
    +-------------------------------+--------------+
    | @timestamp                    | severityText |
    |-------------------------------+--------------|
    | 2024-01-15 10:30:05.678901234 | FATAL        |
    | 2024-01-15 10:30:06.789012345 | TRACE        |
    | 2024-01-15 10:30:07.890123456 | ERROR        |
    | 2024-01-15 10:30:08.901234567 | WARN         |
    | 2024-01-15 10:30:09.012345678 | INFO         |
    | 2024-01-15 10:30:10.123456789 | TRACE2       |
    +-------------------------------+--------------+

**Relative Time Filtering** (before 30 seconds ago)::

    os> search latest=-30s source=otellogs | sort @timestamp | fields @timestamp, severityText | head 3;
    fetched rows / total rows = 3/3
    +-------------------------------+--------------+
    | @timestamp                    | severityText |
    |-------------------------------+--------------|
    | 2024-01-15 10:30:00.123456789 | INFO         |
    | 2024-01-15 10:30:01.23456789  | ERROR        |
    | 2024-01-15 10:30:02.345678901 | WARN         |
    +-------------------------------+--------------+

**Time Snapping** (before start of current minute)::

    os> search latest='@m' source=otellogs | fields @timestamp, severityText | head 2;
    fetched rows / total rows = 2/2
    +-------------------------------+--------------+
    | @timestamp                    | severityText |
    |-------------------------------+--------------|
    | 2024-01-15 10:30:00.123456789 | INFO         |
    | 2024-01-15 10:30:01.23456789  | ERROR        |
    +-------------------------------+--------------+

**Unix Timestamp Filtering**::

    os> search earliest=1705314600 latest=1705314605 source=otellogs | fields @timestamp, severityText;
    fetched rows / total rows = 5/5
    +-------------------------------+--------------+
    | @timestamp                    | severityText |
    |-------------------------------+--------------|
    | 2024-01-15 10:30:00.123456789 | INFO         |
    | 2024-01-15 10:30:01.23456789  | ERROR        |
    | 2024-01-15 10:30:02.345678901 | WARN         |
    | 2024-01-15 10:30:03.456789012 | DEBUG        |
    | 2024-01-15 10:30:04.567890123 | INFO         |
    +-------------------------------+--------------+

Example 10: Special Characters and Escaping
-------------------------------------------

Understand when and how to escape special characters in your search queries. There are two categories of characters that need escaping:

**Characters that must be escaped**:
* **Backslashes (\)**: Always escape as ``\\`` to search for literal backslash
* **Quotes (")**: Escape as ``\"`` when inside quoted strings

**Wildcard characters (escape only to search literally)**:
* **Asterisk (*)**: Use as-is for wildcard, escape as ``\\*`` to search for literal asterisk
* **Question mark (?)**: Use as-is for wildcard, escape as ``\\?`` to search for literal question mark

.. list-table:: Wildcard vs Literal Search
   :widths: 25 35 40
   :header-rows: 1

   * - Intent
     - PPL Syntax
     - Result
   * - Wildcard search
     - ``field=user*``
     - Matches "user", "user123", "userABC"
   * - Literal "user*"
     - ``field="user\\*"``
     - Matches only "user*"
   * - Wildcard search
     - ``field=log?``
     - Matches "log1", "logA", "logs"
   * - Literal "log?"
     - ``field="log\\?"``
     - Matches only "log?"

**Backslash in file paths**::

    os> search `attributes.error.type`="C:\\\\Users\\\\admin" source=otellogs | fields `attributes.error.type`;
    fetched rows / total rows = 1/1
    +-----------------------+
    | attributes.error.type |
    |-----------------------|
    | C:\Users\admin        |
    +-----------------------+

Note: Each backslash in the search value needs to be escaped with another backslash. When using REST API with JSON, additional JSON escaping is required.

**Quotes within strings**::

    os> search body="\"exact phrase\"" source=otellogs | sort @timestamp | fields body | head 1;
    fetched rows / total rows = 1/1
    +--------------------------------------------------------------------------------------------------------------------------------------------------------+
    | body                                                                                                                                                   |
    |--------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------+

**Text with special characters**::

    os> search "wildcard\\* fuzzy~2" source=otellogs | fields body | head 1;
    fetched rows / total rows = 1/1
    +--------------------------------------------------------------------------------------------------------------------------------------------------------+
    | body                                                                                                                                                   |
    |--------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------+

Example 11: Fetch All Data
----------------------------

Retrieve all documents from an index by specifying only the source without any search conditions. This is useful for exploring small datasets or verifying data ingestion.

::

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