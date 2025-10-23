=============
chart
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============

The ``chart`` command transforms search results by applying a statistical aggregation function and optionally grouping the data by one or two fields. The results are suitable for visualization as a two-dimension chart when grouping by two fields, where unique values in the second group key can be pivoted to column names.

Version
=======
3.4.0

Syntax
============

.. code-block:: text

   chart
   [limit=(top|bottom) <number>] [useother=<boolean>] [usenull=<boolean>] [nullstr=<string>] [otherstr=<string>]
   <aggregation_function>
   [ by <row_split> <column_split> ] | [over <row_split> ] [ by <column_split>]

**Parameters:**

* **limit**: optional. Specifies the number of distinct values to display when using column split.

  * Default: 10
  * Syntax: ``limit=(top|bottom) <number>`` or ``limit=<number>`` (defaults to top)
  * When there are more distinct values than the limit, the additional values are grouped into an "OTHER" category if useother is not set to false.
  * Set to 0 to show all distinct values without any limit.
  * Only applies when using column split (over...by clause).

* **useother**: optional. Controls whether to create an "OTHER" category for values beyond the limit.

  * Default: true
  * When set to false, only the top/bottom N values (based on limit) are shown without an "OTHER" category.
  * When set to true, values beyond the limit are grouped into an "OTHER" category.
  * Only applies when using column split and when there are more distinct values than the limit.

* **usenull**: optional. Controls whether to include null values as a separate category.

  * Default: true
  * When set to false, events with null values in the split-by field are excluded from results.
  * When set to true, null values appear as a separate category.

* **nullstr**: optional. Specifies the string to display for null values.

  * Default: "NULL"
  * Only applies when usenull is set to true.

* **otherstr**: optional. Specifies the string to display for the "OTHER" category.

  * Default: "OTHER"
  * Only applies when useother is set to true and there are values beyond the limit.

* **aggregation_function**: mandatory. The aggregation function to apply to the data.

  * Currently, only a single aggregation function is supported.
  * Available functions: All aggregation functions supported by the :doc:`stats <stats>` command.

* **by**: optional. Groups the results by the specified field as rows.

  * If not specified, the aggregation is performed across all documents.

* **over...by**: optional. Alternative syntax for grouping by multiple fields.

  * ``over <row_split> by <column_split>`` groups the results by both fields.
  * The row_split field becomes the primary grouping dimension.
  * The column_split field becomes the secondary grouping dimension.
  * Results are returned as individual rows for each combination.

Notes
=====

* The ``chart`` command transforms results into a table format suitable for visualization.
* When using multiple grouping fields (over...by syntax), the output contains individual rows for each combination of the grouping fields.
* The limit parameter determines how many columns to show when there are many distinct values.
* Results are ordered by the aggregated values to determine top/bottom selections.

Examples
========

Example 1: Basic aggregation without grouping
==============================================

This example calculates the average balance across all accounts.

PPL query::

    os> source=accounts | chart avg(balance)
    fetched rows / total rows = 1/1
    +--------------+
    | avg(balance) |
    |--------------|
    | 20482.25     |
    +--------------+

Example 2: Group by single field
=================================

This example calculates the count of accounts grouped by gender.

PPL query::

    os> source=accounts | chart count() by gender
    fetched rows / total rows = 2/2
    +---------+--------+
    | count() | gender |
    |---------+--------|
    | 1       | F      |
    | 3       | M      |
    +---------+--------+

Example 3: Using over and by for multiple field grouping
========================================================

This example shows average balance grouped by both gender and age fields.

PPL query::

    os> source=accounts | chart avg(balance) over gender by age
    fetched rows / total rows = 4/4
    +--------+-----+--------------+
    | gender | age | avg(balance) |
    |--------+-----+--------------|
    | F      | 28  | 32838.0      |
    | M      | 32  | 39225.0      |
    | M      | 33  | 4180.0       |
    | M      | 36  | 5686.0       |
    +--------+-----+--------------+

Example 4: Using basic limit functionality
========================================

This example limits the results to show only the top 1 age group.

PPL query::

    os> source=accounts | chart limit=1 count() over gender by age
    fetched rows / total rows = 3/3
    +--------+-------+---------+
    | gender | age   | count() |
    |--------+-------+---------|
    | M      | OTHER | 2       |
    | M      | 33    | 1       |
    | F      | OTHER | 1       |
    +--------+-------+---------+

Example 5: Using limit with other parameters
=============================================

This example shows using limit with useother and custom otherstr parameters.

PPL query::

    os> source=accounts | chart limit=top 2 useother=true otherstr='remaining_accounts' max(balance) over state by gender
    fetched rows / total rows = 4/4
    +-------+--------+--------------+
    | state | gender | max(balance) |
    |-------+--------+--------------|
    | TN    | M      | 5686         |
    | MD    | M      | 4180         |
    | IL    | M      | 39225        |
    | VA    | F      | 32838        |
    +-------+--------+--------------+

Example 6: Using span with chart command
=======================================

This example demonstrates using span for grouping age ranges.

PPL query::

    os> source=accounts | chart max(balance) by age span=10
    fetched rows / total rows = 2/2
    +--------------+-----+
    | max(balance) | age |
    |--------------+-----|
    | 32838        | 20  |
    | 39225        | 30  |
    +--------------+-----+

Limitations
============
* Only a single aggregation function is supported per chart command.
* When using both row and column splits, the column split field is converted to string type so that it can be used as column names.