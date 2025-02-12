=============
fieldsummary
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Use ``fieldsummary`` command to provide summary statistics for all fields in the current result set.

This command will:
 - Calculate basic statistics for each field (count, distinct count and min, max, avg for numeric fields)
 - Determine the data type of each field

Syntax
============
fieldsummary [includefields="<field_name>[,<field_name>]"]

* includefields: optional. specify which fields to include in the summary.

Example 1: Perform fieldsummary without additional fields
==============================================

PPL query::

    os> source=nyc_taxi | fieldsummary;
    fetched rows / total rows = 4/4
    +--------------+-------+----------+--------------------+---------+--------+--------+
    | Field        | Count | Distinct | Avg                | Max     | Min    | Type   |
    |--------------+-------+----------|--------------------+---------+--------+--------|
    | anomaly_type | 973   | 1        |                    |         |        | string |
    | category     | 973   | 2        |                    |         |        | string |
    | value        | 973   | 953      | 14679.110996916752 | 29985.0 | 1769.0 | float  |
    | timestamp    | 973   | 973      |                    |         |        | date   |
    +--------------+-------+----------+--------------------+---------+--------+--------+

Example 2: Perform fieldsummary with includefields
==============================================

PPL query::

    os> source=accounts | fieldsummary includefields="category,value" ;
    fetched rows / total rows = 2/2
    +--------------+-------+----------+--------------------+---------+--------+--------+
    | Field        | Count | Distinct | Avg                | Max     | Min    | Type   |
    |--------------+-------+----------|--------------------+---------+--------+--------|
    | category     | 973   | 2        |                    |         |        | string |
    | value        | 973   | 953      | 14679.110996916752 | 29985.0 | 1769.0 | float  |
    +--------------+-------+----------+--------------------+---------+--------+--------+


