=====
regex
=====

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``regex`` command filters search results by matching field values against a regular expression pattern. Only documents where the specified field matches the pattern are included in the results.

Syntax
======
regex <field> = <pattern>
regex <field> != <pattern>

* field: mandatory. The field name to match against.
* pattern: mandatory string. The regular expression pattern to match. Supports Java regex syntax including named groups, lookahead/lookbehind, and character classes.
* = : operator for positive matching (include matches)
* != : operator for negative matching (exclude matches)

Regular Expression Engine
=========================

The regex command uses Java's built-in regular expression engine, which supports:

* **Standard regex features**: Character classes, quantifiers, anchors
* **Named capture groups**: ``(?<name>pattern)`` syntax
* **Lookahead/lookbehind**: ``(?=...)`` and ``(?<=...)`` assertions
* **Inline flags**: Case-insensitive ``(?i)``, multiline ``(?m)``, dotall ``(?s)``, and other modes

For complete documentation of Java regex patterns and available modes, see the `Java Pattern documentation <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`_.

Example 1: Basic pattern matching
=================================

This example shows how to filter documents where the ``lastname`` field matches names starting with uppercase letters.

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

This example shows how to exclude documents where the ``lastname`` field ends with "son".

PPL query::

    os> source=accounts | regex lastname!=".*son$" | fields account_number, lastname;
    fetched rows / total rows = 4/4
    +----------------+----------+
    | account_number | lastname |
    |----------------+----------|
    | 1              | Duke     |
    | 6              | Bond     |
    | 13             | Bates    |
    | 18             | Adams    |
    +----------------+----------+


Example 3: Email domain matching
================================

This example shows how to filter documents by email domain patterns.

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

This example shows how to use complex regex patterns with character classes and quantifiers.

PPL query::

    os> source=accounts | regex address="\d{3,4}\s+[A-Z][a-z]+\s+(Street|Lane|Court)" | fields account_number, address;
    fetched rows / total rows = 4/4
    +----------------+----------------------+
    | account_number | address              |
    |----------------+----------------------|
    | 1              | 880 Holmes Lane      |
    | 6              | 671 Bristol Street   |
    | 13             | 789 Madison Street   |
    | 18             | 467 Hutchinson Court |
    +----------------+----------------------+


Example 5: Case-sensitive matching
==================================

This example demonstrates that regex matching is case-sensitive by default.

PPL query::

    os> source=accounts | regex state="va" | fields account_number, state;
    fetched rows / total rows = 0/0
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    +----------------+-------+

PPL query::

    os> source=accounts | regex state="VA" | fields account_number, state;
    fetched rows / total rows = 1/1
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 13             | VA    |
    +----------------+-------+


Limitations
===========

| * **Field specification required**: A field name must be specified in the regex command. Pattern-only syntax (e.g., ``regex "pattern"``) is not currently supported
| * **String fields only**: The regex command currently only supports string fields. Using it on numeric or boolean fields will result in an error
