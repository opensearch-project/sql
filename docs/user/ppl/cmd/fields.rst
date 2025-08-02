=============
fields
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
Using ``fields`` command to keep or remove fields from the search result. The ``table`` command is an alias for ``fields`` and provides identical functionality.

The ``fields`` and ``table`` command supports multiple field specification formats:

* **Space-delimited syntax**: Fields can be separated by spaces (``firstname lastname age``)
* **Wildcard pattern matching**: Use ``*`` for prefix (``account*``), suffix (``*name``), or contains (``*a*``) patterns
* **Mixed delimiters**: Combine spaces and commas (``firstname lastname, age``)
* **Field deduplication**: Automatically prevents duplicate columns when wildcards expand to already specified fields

Syntax
======
fields [+|-] <field-list>
table [+|-] <field-list>

* prefix: optional. if the plus (+) is used, only the fields specified in the field list will be kept. if the minus (-) is used, all the fields specified in the field list will be removed. **Default** +
* field-list: mandatory. Fields can be specified using:
  - Comma-delimited: ``field1, field2, field3``
  - Space-delimited: ``field1 field2 field3``
  - Mixed delimiters: ``field1 field2, field3``
  - Wildcard patterns: ``account*``, ``*name``, ``*a*``


Example 1: Select specified fields from result
==============================================

The example shows fetching account_number, firstname and lastname fields from search results.

PPL query::

    os> source=accounts | fields account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+

Using ``table`` command::

    os> source=accounts | table account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+

Example 2: Remove specified fields from result
==============================================

The example shows removing the account_number field from search results.

PPL query::

    os> source=accounts | fields account_number, firstname, lastname | fields - account_number;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Using ``table`` command::

    os> source=accounts | table account_number, firstname, lastname | table - account_number;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Example 3: Space-delimited field syntax
=======================================

Fields can be specified using spaces instead of commas.

PPL query::

    os> source=accounts | fields firstname lastname age;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----|
    | Amber     | Duke     | 32  |
    | Hattie    | Bond     | 36  |
    | Nanette   | Bates    | 28  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+

Using ``table`` command::

    os> source=accounts | table firstname lastname age;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----|
    | Amber     | Duke     | 32  |
    | Hattie    | Bond     | 36  |
    | Nanette   | Bates    | 28  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+

Example 4: Wildcard pattern matching
====================================

**Prefix wildcard** - Select all fields starting with "account":

PPL query::

    os> source=accounts | fields account*;
    fetched rows / total rows = 4/4
    +----------------+
    | account_number |
    |----------------|
    | 1              |
    | 6              |
    | 13             |
    | 18             |
    +----------------+

Using ``table`` command::

    os> source=accounts | table account*;
    fetched rows / total rows = 4/4
    +----------------+
    | account_number |
    |----------------|
    | 1              |
    | 6              |
    | 13             |
    | 18             |
    +----------------+

**Suffix wildcard** - Select all fields ending with "name":

PPL query::

    os> source=accounts | fields *name;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Using ``table`` command::

    os> source=accounts | table *name;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

**Contains wildcard** - Select all fields containing "a":

PPL query::

    os> source=accounts | fields *a* | head 1;
    fetched rows / total rows = 1/1
    +----------------+-----------------+-----+---------+----------------------+-----------+----------+-------+
    | account_number | address         | age | balance | email                | firstname | lastname | state |
    |----------------+-----------------+-----+---------+----------------------+-----------+----------+-------|
    | 1              | 880 Holmes Lane | 32  | 39225   | amberduke@pyrami.com | Amber     | Duke     | IL    |
    +----------------+-----------------+-----+---------+----------------------+-----------+----------+-------+

Using ``table`` command::

    os> source=accounts | table *a* | head 1;
    fetched rows / total rows = 1/1
    +----------------+-----------------+-----+---------+----------------------+-----------+----------+-------+
    | account_number | address         | age | balance | email                | firstname | lastname | state |
    |----------------+-----------------+-----+---------+----------------------+-----------+----------+-------|
    | 1              | 880 Holmes Lane | 32  | 39225   | amberduke@pyrami.com | Amber     | Duke     | IL    |
    +----------------+-----------------+-----+---------+----------------------+-----------+----------+-------+

Example 5: Mixed delimiters and wildcards
=========================================

Combine explicit fields, wildcards, and mixed delimiters.

PPL query::

    os> source=accounts | fields firstname, account* *name;
    fetched rows / total rows = 4/4
    +-----------+----------------+----------+
    | firstname | account_number | lastname |
    |-----------+----------------+----------|
    | Amber     | 1              | Duke     |
    | Hattie    | 6              | Bond     |
    | Nanette   | 13             | Bates    |
    | Dale      | 18             | Adams    |
    +-----------+----------------+----------+

Using ``table`` command::

    os> source=accounts | table firstname, account* *name;
    fetched rows / total rows = 4/4
    +-----------+----------------+----------+
    | firstname | account_number | lastname |
    |-----------+----------------+----------|
    | Amber     | 1              | Duke     |
    | Hattie    | 6              | Bond     |
    | Nanette   | 13             | Bates    |
    | Dale      | 18             | Adams    |
    +-----------+----------------+----------+

Example 6: Table command alias
=============================

The ``table`` command works identically to ``fields``.

PPL query::

    os> source=accounts | table firstname, lastname, age;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----|
    | Amber     | Duke     | 32  |
    | Hattie    | Bond     | 36  |
    | Nanette   | Bates    | 28  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+

Example 7: Wildcard exclusion
=============================

Remove fields using wildcard patterns.

PPL query::

    os> source=accounts | fields - *name;
    fetched rows / total rows = 4/4
    +----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+
    | account_number | address              | balance | gender | city   | employer | state | age | email                 |
    |----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------|
    | 1              | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  |
    | 6              | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com |
    | 13             | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  |
    | 18             | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   |
    +----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+

Using ``table`` command::

    os> source=accounts | table - *name;
    fetched rows / total rows = 4/4
    +----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+
    | account_number | address              | balance | gender | city   | employer | state | age | email                 |
    |----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------|
    | 1              | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  |
    | 6              | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com |
    | 13             | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  |
    | 18             | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   |
    +----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+



