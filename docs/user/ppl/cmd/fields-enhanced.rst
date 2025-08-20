===================
Enhanced Fields
===================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
Enhanced fields features provide advanced field selection capabilities when the Calcite engine is enabled. These features extend the basic ``fields`` and ``table`` commands with powerful syntax options for more flexible field management.

.. note::
   All features described in this document require the Calcite engine to be enabled. When Calcite is disabled, only basic comma-delimited field selection is supported.

Enhanced Features
=================

Space-Delimited Syntax
----------------------
Fields can be specified using spaces instead of commas, providing a more concise syntax.

**Syntax**: ``fields field1 field2 field3``

**Example**::

    os> source=accounts | fields firstname lastname age;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----+
    | Amber     | Duke     | 32  |
    | Hattie    | Bond     | 36  |
    | Nanette   | Bates    | 28  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+

Wildcard Pattern Matching
-------------------------
Use wildcards to select multiple fields based on patterns.

**Prefix Wildcard** - Select fields starting with a pattern::

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

**Suffix Wildcard** - Select fields ending with a pattern::

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

**Contains Wildcard** - Select fields containing a pattern::

    os> source=accounts | fields *a* | head 1;
    fetched rows / total rows = 1/1
    +----------------+-----------+-----------------+---------+-------+-----+----------------------+----------+
    | account_number | firstname | address         | balance | state | age | email                | lastname |
    |----------------+-----------+-----------------+---------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane | 39225   | IL    | 32  | amberduke@pyrami.com | Duke     |
    +----------------+-----------+-----------------+---------+-------+-----+----------------------+----------+

Mixed Delimiters
---------------
Combine spaces and commas for flexible field specification.

**Example**::

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

Field Deduplication
------------------
Automatically prevents duplicate columns when wildcards expand to already specified fields.

**Example**::

    os> source=accounts | fields firstname, *name;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Note: Even though ``firstname`` is explicitly specified and would also match ``*name``, it appears only once due to automatic deduplication.

Full Wildcard Selection
-----------------------
Select all available fields using ``*``.

**Example**::

    os> source=accounts | fields * | head 1;
    fetched rows / total rows = 1/1
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address         | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+

Wildcard Exclusion
------------------
Remove fields using wildcard patterns with the minus (-) operator.

**Example**::

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

Table Command Alias
===================
All enhanced features are also available through the ``table`` command, which is an alias for ``fields``.

**Example**::

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

Requirements
============
- **Calcite Engine**: All enhanced features require the Calcite engine to be enabled
- **Backward Compatibility**: Basic comma-delimited syntax continues to work when Calcite is disabled
- **Error Handling**: Attempting to use enhanced features without Calcite will result in an ``UnsupportedOperationException``