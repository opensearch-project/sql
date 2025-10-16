=============
dedup
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
The ``dedup`` command removes duplicate documents defined by specified fields from the search result.

Syntax
======
dedup [int] <field-list> [keepempty=<bool>] [consecutive=<bool>]

* int: optional. The ``dedup`` command retains multiple events for each combination when you specify <int>. The number for <int> must be greater than 0. All other duplicates are removed from the results. **Default:** 1
* keepempty: optional. If set to true, keep the document if the any field in the field-list has NULL value or field is MISSING. **Default:** false.
* consecutive: optional. If set to true, removes only events with duplicate combinations of values that are consecutive. **Default:** false.
* field-list: mandatory. The comma-delimited field list. At least one field is required.

Example 1: Dedup by one field
=============================

This example shows deduplicating documents by gender field.

PPL query::

    os> source=accounts | dedup gender | fields account_number, gender | sort account_number;
    fetched rows / total rows = 2/2
    +----------------+--------+
    | account_number | gender |
    |----------------+--------|
    | 1              | M      |
    | 13             | F      |
    +----------------+--------+

Example 2: Keep 2 duplicates documents
======================================

This example shows deduplicating documents by gender field while keeping 2 duplicates.

PPL query::

    os> source=accounts | dedup 2 gender | fields account_number, gender | sort account_number;
    fetched rows / total rows = 3/3
    +----------------+--------+
    | account_number | gender |
    |----------------+--------|
    | 1              | M      |
    | 6              | M      |
    | 13             | F      |
    +----------------+--------+

Example 3: Keep or Ignore the empty field by default
====================================================

This example shows deduplicating documents while keeping null values.

PPL query::

    os> source=accounts | dedup email keepempty=true | fields account_number, email | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+-----------------------+
    | account_number | email                 |
    |----------------+-----------------------|
    | 1              | amberduke@pyrami.com  |
    | 6              | hattiebond@netagy.com |
    | 13             | null                  |
    | 18             | daleadams@boink.com   |
    +----------------+-----------------------+


This example shows deduplicating documents while ignoring null values.

PPL query::

    os> source=accounts | dedup email | fields account_number, email | sort account_number;
    fetched rows / total rows = 3/3
    +----------------+-----------------------+
    | account_number | email                 |
    |----------------+-----------------------|
    | 1              | amberduke@pyrami.com  |
    | 6              | hattiebond@netagy.com |
    | 18             | daleadams@boink.com   |
    +----------------+-----------------------+


Example 4: Dedup in consecutive document
========================================

This example shows deduplicating consecutive documents.

PPL query::

    os> source=accounts | dedup gender consecutive=true | fields account_number, gender | sort account_number;
    fetched rows / total rows = 3/3
    +----------------+--------+
    | account_number | gender |
    |----------------+--------|
    | 1              | M      |
    | 13             | F      |
    | 18             | M      |
    +----------------+--------+

Limitations
===========
The ``dedup`` with ``consecutive=true`` command can only work with ``plugins.calcite.enabled=false``.
