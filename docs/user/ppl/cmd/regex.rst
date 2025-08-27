=============
regex
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``regex`` command filters search results by matching field values against a regular expression pattern. Only documents where the specified field matches the pattern are included in the results.

Version
=======
3.3.0

Syntax
============
regex <field> = <pattern>
regex <field> != <pattern>
regex <pattern>

* field: optional. The field name to match against. If not specified, the pattern will be matched against the default field.
* pattern: mandatory string. The regular expression pattern to match. Supports Java regex syntax including named groups, lookahead/lookbehind, and character classes.
* = : optional operator for positive matching (default behavior)
* != : optional operator for negative matching (exclude matches)

Regular Expression Engine
==========================

The regex command uses Java's built-in regular expression engine, which supports:

* **Standard regex features**: Character classes, quantifiers, anchors
* **Named capture groups**: ``(?<name>pattern)`` syntax
* **Lookahead/lookbehind**: ``(?=...)`` and ``(?<=...)`` assertions
* **Inline flags**: Case-insensitive ``(?i)``, multiline ``(?m)``, dotall ``(?s)``, and other modes

For complete documentation of Java regex patterns and available modes, see the `Java Pattern documentation <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`_.

Example 1: Basic pattern matching
=================================

The example shows how to filter documents where the ``lastname`` field matches names starting with uppercase letters.

PPL query::

    os> source=accounts | regex lastname="^[A-Z][a-z]+$" | fields account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+


Example 2: Negative matching
============================

The example shows how to exclude documents where the ``lastname`` field ends with "son".

PPL query::

    os> source=accounts | regex lastname!=".*son$" | fields account_number, lastname;
    fetched rows / total rows = 3/3
    +----------------+----------+
    | account_number | lastname |
    |----------------+----------|
    | 1              | Duke     |
    | 6              | Bond     |
    | 13             | Bates    |
    +----------------+----------+


Example 3: Email domain matching
================================

The example shows how to filter documents by email domain patterns.

PPL query::

    os> source=accounts | regex email="@pyrami\.com$" | fields account_number, email;
    fetched rows / total rows = 1/1
    +----------------+----------------------+
    | account_number | email                |
    |----------------+----------------------|
    | 1              | amberduke@pyrami.com |
    +----------------+----------------------+


Example 4: Complex patterns with character classes
==================================================

The example shows how to use complex regex patterns with character classes and quantifiers.

PPL query::

    os> source=accounts | regex address="\d{3,4}\s+[A-Z][a-z]+\s+(Street|Lane|Court)" | fields account_number, address;
    fetched rows / total rows = 2/2
    +----------------+------------------+
    | account_number | address          |
    |----------------+------------------|
    | 6              | 671 Bristol Street |
    | 18             | 880 Holmes Lane   |
    +----------------+------------------+


Example 5: Case-sensitive matching
==================================

The example demonstrates that regex matching is case-sensitive by default.

PPL query::

    os> source=accounts | regex state="virginia" | fields account_number, state;
    fetched rows / total rows = 0/0
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    +----------------+-------+

PPL query::

    os> source=accounts | regex state="Virginia" | fields account_number, state;
    fetched rows / total rows = 1/1
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 1              | VA    |
    +----------------+-------+


Limitations
===========

* **String fields only**: The regex command currently only supports string fields. Using it on numeric or boolean fields will result in an error
* **Performance**: Complex regex patterns may impact query performance, especially on large datasets
* **Memory usage**: Pattern compilation results are cached, but very large numbers of unique patterns may consume memory
