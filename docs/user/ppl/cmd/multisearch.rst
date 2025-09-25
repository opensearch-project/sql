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

Version
=======
3.3.0

Syntax
======
multisearch [search subsearch1] [search subsearch2] [search subsearch3]...

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
* **Schema Compatibility**: Fields with the same name across subsearches should have compatible types

Usage
=====

Basic multisearch::

    | multisearch [search source=table | where condition1] [search source=table | where condition2]
    | multisearch [search source=index1 | fields field1, field2] [search source=index2 | fields field1, field2] | stats count
    | multisearch [search source=table | where status="success"] [search source=table | where status="error"] | stats count by status

Example 1: Basic Age Group Analysis
===================================

Combine young and adult customers into a single result set for further analysis.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval age_group = "young"] [search source=accounts | where age >= 30 | eval age_group = "adult"] | stats count by age_group | sort age_group;
    fetched rows / total rows = 2/2
    +-------+-----------+
    | count | age_group |
    |-------+-----------|
    | 3     | adult     |
    | 1     | young     |
    +-------+-----------+

Example 2: Success Rate Pattern
===============================

Calculate success rates by comparing good accounts vs. total valid accounts.

PPL query::

    os> | multisearch [search source=accounts | where balance > 20000 | eval query_type = "good"] [search source=accounts | where balance > 0 | eval query_type = "valid"] | stats count(eval(query_type = "good")) as good_accounts, count(eval(query_type = "valid")) as total_valid;
    fetched rows / total rows = 1/1
    +---------------+-------------+
    | good_accounts | total_valid |
    |---------------+-------------|
    | 2             | 4           |
    +---------------+-------------+

Example 3: Multi-Region Analysis
=================================

Combine data from multiple regions for comparative analysis.

PPL query::

    os> | multisearch [search source=accounts | where state = "IL" | eval region = "Illinois"] [search source=accounts | where state = "TN" | eval region = "Tennessee"] [search source=accounts | where state = "CA" | eval region = "California"] | stats count by region | sort region;
    fetched rows / total rows = 2/2
    +-------+-----------+
    | count | region    |
    |-------+-----------|
    | 1     | Illinois  |
    | 1     | Tennessee |
    +-------+-----------+

Example 4: Gender-based Analysis with Aggregations
===================================================

Compare customer segments by gender with complex aggregations.

PPL query::

    os> | multisearch [search source=accounts | where gender = "M" | eval segment = "male"] [search source=accounts | where gender = "F" | eval segment = "female"] | stats count as customer_count, avg(balance) as avg_balance by segment | sort segment;
    fetched rows / total rows = 2/2
    +----------------+--------------------+---------+
    | customer_count | avg_balance        | segment |
    |----------------+--------------------+---------|
    | 1              | 32838.0            | female  |
    | 3              | 16363.666666666666 | male    |
    +----------------+--------------------+---------+

Example 5: Cross-Source Pattern with Field Projection
======================================================

Combine specific fields from different search criteria.

PPL query::

    os> | multisearch [search source=accounts | where gender = "M" | fields firstname, lastname, balance] [search source=accounts | where gender = "F" | fields firstname, lastname, balance] | head 5;
    fetched rows / total rows = 4/4
    +-----------+----------+---------+
    | firstname | lastname | balance |
    |-----------+----------+---------|
    | Amber     | Duke     | 39225   |
    | Hattie    | Bond     | 5686    |
    | Dale      | Adams    | 4180    |
    | Nanette   | Bates    | 32838   |
    +-----------+----------+---------+

Example 6: Timestamp Interleaving
==================================

Combine time-series data from multiple sources with automatic timestamp-based ordering.

PPL query::

    os> | multisearch [search source=time_data | where category IN ("A", "B")] [search source=time_data2 | where category IN ("E", "F")] | head 5;
    fetched rows / total rows = 5/5
    +-------+---------------------+----------+-------+---------------------+
    | index | @timestamp          | category | value | timestamp           |
    |-------+---------------------+----------+-------+---------------------|
    | null  | 2025-08-01 04:00:00 | E        | 2001  | 2025-08-01 04:00:00 |
    | null  | 2025-08-01 03:47:41 | A        | 8762  | 2025-08-01 03:47:41 |
    | null  | 2025-08-01 02:30:00 | F        | 2002  | 2025-08-01 02:30:00 |
    | null  | 2025-08-01 01:14:11 | B        | 9015  | 2025-08-01 01:14:11 |
    | null  | 2025-08-01 01:00:00 | E        | 2003  | 2025-08-01 01:00:00 |
    +-------+---------------------+----------+-------+---------------------+

Example 7: Balance Category Segmentation
=========================================

Analyze accounts across different balance ranges.

PPL query::

    os> | multisearch [search source=accounts | where balance > 40000 | eval balance_category = "high"] [search source=accounts | where balance <= 40000 AND balance > 20000 | eval balance_category = "medium"] [search source=accounts | where balance <= 20000 | eval balance_category = "low"] | stats count, avg(balance) as avg_bal by balance_category | sort balance_category;
    fetched rows / total rows = 2/2
    +-------+---------+------------------+
    | count | avg_bal | balance_category |
    |-------+---------+------------------|
    | 2     | 4933.0  | low              |
    | 2     | 36031.5 | medium           |
    +-------+---------+------------------+

Example 8: Handling Empty Results
==================================

Multisearch gracefully handles cases where some subsearches return no results.

PPL query::

    os> | multisearch [search source=accounts | where age > 25] [search source=accounts | where age > 200 | eval impossible = "yes"] | stats count;
    fetched rows / total rows = 1/1
    +-------+
    | count |
    |-------|
    | 4     |
    +-------+

Example 9: Type Compatibility - Numeric Promotion
===================================================

Demonstrate how numeric types are automatically promoted in multisearch operations.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval score = 85.0] [search source=accounts | where age >= 30 | eval score = 90.5] | head 2;
    fetched rows / total rows = 2/2
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+-------+
    | account_number | firstname | address            | balance | gender | city   | employer | state | age | email                | lastname | score |
    |----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+-------|
    | 13             | Nanette   | 789 Madison Street | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                 | Bates    | 85.0  |
    | 1              | Amber     | 880 Holmes Lane    | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     | 90.5  |
    +----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+----------------------+----------+-------+

Example 10: Type Compatibility - String Length Promotion
==========================================================

Demonstrate how VARCHAR types with different lengths are handled.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval status = "OK"] [search source=accounts | where age >= 30 | eval status = "APPROVED"] | stats count by status | sort status;
    fetched rows / total rows = 2/2
    +-------+----------+
    | count | status   |
    |-------+----------|
    | 3     | APPROVED |
    | 1     | OK       |
    +-------+----------+

Example 11: Type Compatibility - Missing Fields
=================================================

Demonstrate how missing fields are handled with NULL insertion.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval young_flag = "yes" | fields firstname, age, young_flag] [search source=accounts | where age >= 30 | fields firstname, age] | stats count() as total_count, count(young_flag) as young_flag_count;
    fetched rows / total rows = 1/1
    +-------------+------------------+
    | total_count | young_flag_count |
    |-------------+------------------|
    | 4           | 1                |
    +-------------+------------------+

Example 12: Type Compatibility - Explicit Casting
===================================================

Demonstrate how to resolve type conflicts using explicit casting.

PPL query::

    os> | multisearch [search source=accounts | where age < 30 | eval mixed_field = CAST(age AS STRING) | fields mixed_field] [search source=accounts | where age >= 30 | eval mixed_field = CAST(balance AS STRING) | fields mixed_field] | head 3;
    fetched rows / total rows = 3/3
    +-------------+
    | mixed_field |
    |-------------|
    | 28          |
    | 39225       |
    | 5686        |
    +-------------+

Common Patterns
===============

**Success Rate Calculation**::

    | multisearch
        [search source=logs | where status="success" | eval result="success"]
        [search source=logs | where status!="success" | eval result="total"]
    | stats count(eval(result="success")) as success_count, count() as total_count

**A/B Testing Analysis**::

    | multisearch
        [search source=experiments | where group="A" | eval test_group="A"]
        [search source=experiments | where group="B" | eval test_group="B"]
    | stats avg(conversion_rate) by test_group

**Multi-timeframe Comparison**::

    | multisearch
        [search source=metrics | where timestamp >= "2024-01-01" AND timestamp < "2024-02-01" | eval period="current"]
        [search source=metrics | where timestamp >= "2023-01-01" AND timestamp < "2023-02-01" | eval period="previous"]
    | stats avg(value) by period

