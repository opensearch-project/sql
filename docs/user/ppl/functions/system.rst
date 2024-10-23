================
System Functions
================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

TYPEOF
------

Description
>>>>>>>>>>>

Usage: typeof(expr) function returns name of the data type of the value that is passed to it. This can be helpful for troubleshooting or dynamically constructing SQL queries.

Argument type: ANY

Return type: STRING

Example::

    os> source=people | eval `typeof(date)` = typeof(DATE('2008-04-14')), `typeof(int)` = typeof(1), `typeof(now())` = typeof(now()), `typeof(column)` = typeof(accounts) | fields `typeof(date)`, `typeof(int)`, `typeof(now())`, `typeof(column)`
    fetched rows / total rows = 1/1
    +--------------+-------------+---------------+----------------+
    | typeof(date) | typeof(int) | typeof(now()) | typeof(column) |
    |--------------+-------------+---------------+----------------|
    | DATE         | INTEGER     | TIMESTAMP     | OBJECT         |
    +--------------+-------------+---------------+----------------+
