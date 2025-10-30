=============
multisearch
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| (Experimental)
| Using ``multisearch`` command to run multiple search subsearches and merge their results together. The command allows you to combine data from different queries on the same or different sources, and optionally apply subsequent processing to the combined result set.

| Key aspects of ``multisearch``:

1. Combines results from multiple search operations into a single result set.
2. Each subsearch can have different filtering criteria, data transformations, and field selections.
3. Results are merged and can be further processed with aggregations, sorting, and other PPL commands.
4. Particularly useful for comparative analysis, union operations, and creating comprehensive datasets from multiple search criteria.
5. Supports timestamp-based result interleaving when working with time-series data.

| Use Cases:

* **Comparative Analysis**: Compare metrics across different segments, regions, or time periods
* **Success Rate Monitoring**: Calculate success rates by comparing successful vs. total operations
* **Multi-source Data Combination**: Merge data from different indices or apply different filters to the same source
* **A/B Testing Analysis**: Combine results from different test groups for comparison
* **Time-series Data Merging**: Interleave events from multiple sources based on timestamps

Syntax
======
| multisearch <subsearch1> <subsearch2> <subsearch3> ...

**Requirements:**

* **Minimum 2 subsearches required** - multisearch must contain at least two subsearch blocks
* **Maximum unlimited** - you can specify as many subsearches as needed

**Subsearch Format:**

* Each subsearch must be enclosed in square brackets: ``[search ...]``
* Each subsearch must start with the ``search`` keyword
* Syntax: ``[search source=index | commands...]``
* Description: Each subsearch is a complete search pipeline enclosed in square brackets
 * Supported commands in subsearches: All PPL commands are supported (``where``, ``eval``, ``fields``, ``head``, ``rename``, ``stats``, ``sort``, ``dedup``, etc.)

* result-processing: optional. Commands applied to the merged results.

 * Description: After the multisearch operation, you can apply any PPL command to process the combined results, such as ``stats``, ``sort``, ``head``, etc.

Limitations
===========

* **Minimum Subsearches**: At least two subsearches must be specified
* **Schema Compatibility**: When fields with the same name exist across subsearches but have incompatible types, the query will fail with an error. To avoid type conflicts, ensure that fields with the same name have the same data type across all subsearches, or use different field names (e.g., by renaming with ``eval`` or using ``fields`` to select non-conflicting columns).

Usage
=====

Basic multisearch::

    | multisearch [search source=table | where condition1] [search source=table | where condition2]
    | multisearch [search source=index1 | fields field1, field2] [search source=index2 | fields field1, field2]
    | multisearch [search source=table | where status="success"] [search source=table | where status="error"]

Example 1: Basic Age Group Analysis
===================================

Combine young and adult customers into a single result set for further analysis.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval age_group = "young" | fields firstname, age, age_group] [search source=accounts | where age >= 30 | eval age_group = "adult" | fields firstname, age, age_group] | sort age;
    fetched rows / total rows = 4/4
    +-----------+-----+-----------+
    | firstname | age | age_group |
    |-----------+-----+-----------|
    | Nanette   | 28  | young     |
    | Amber     | 32  | adult     |
    | Dale      | 33  | adult     |
    | Hattie    | 36  | adult     |
    +-----------+-----+-----------+

Example 2: Success Rate Pattern
===============================

Combine high-balance and all valid accounts for comparison analysis.

PPL query::

    os> | multisearch [search source=accounts | where balance > 20000 | eval query_type = "high_balance" | fields firstname, balance, query_type] [search source=accounts | where balance > 0 AND balance <= 20000 | eval query_type = "regular" | fields firstname, balance, query_type] | sort balance desc;
    fetched rows / total rows = 4/4
    +-----------+---------+--------------+
    | firstname | balance | query_type   |
    |-----------+---------+--------------|
    | Amber     | 39225   | high_balance |
    | Nanette   | 32838   | high_balance |
    | Hattie    | 5686    | regular      |
    | Dale      | 4180    | regular      |
    +-----------+---------+--------------+

Example 3: Timestamp Interleaving
==================================

Combine time-series data from multiple sources with automatic timestamp-based ordering.

PPL query::

    os> | multisearch [search source=time_data | where category IN ("A", "B")] [search source=time_data2 | where category IN ("E", "F")] | fields @timestamp, category, value, timestamp | head 5;
    fetched rows / total rows = 5/5
    +---------------------+----------+-------+---------------------+
    | @timestamp          | category | value | timestamp           |
    |---------------------+----------+-------+---------------------|
    | 2025-08-01 04:00:00 | E        | 2001  | 2025-08-01 04:00:00 |
    | 2025-08-01 03:47:41 | A        | 8762  | 2025-08-01 03:47:41 |
    | 2025-08-01 02:30:00 | F        | 2002  | 2025-08-01 02:30:00 |
    | 2025-08-01 01:14:11 | B        | 9015  | 2025-08-01 01:14:11 |
    | 2025-08-01 01:00:00 | E        | 2003  | 2025-08-01 01:00:00 |
    +---------------------+----------+-------+---------------------+

Example 4: Type Compatibility - Missing Fields
=================================================

Demonstrate how missing fields are handled with NULL insertion.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval young_flag = "yes" | fields firstname, age, young_flag] [search source=accounts | where age >= 30 | fields firstname, age] | sort age;
    fetched rows / total rows = 4/4
    +-----------+-----+------------+
    | firstname | age | young_flag |
    |-----------+-----+------------|
    | Nanette   | 28  | yes        |
    | Amber     | 32  | null       |
    | Dale      | 33  | null       |
    | Hattie    | 36  | null       |
    +-----------+-----+------------+

