=========
append
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| (Experimental)
| (From 3.2.0)
| Using ``append`` command to append the result of a sub-search and attach it as additional rows to the bottom of the input search results (The main search).
The command aligns columns with the same field names and types. For different column fields between the main search and sub-search, NULL values are filled in the respective rows.

Version
=======
3.2.0

Syntax
============
append <sub-search>

* sub-search: mandatory. Executes PPL commands as a secondary search. The sub-search uses the same data specified in the source clause of the main search results as its input.

Configuration
=============
This command requires Calcite enabled.

Enable Calcite::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.calcite.enabled" : true
	  }
	}'

Result set::

    {
      "acknowledged": true,
      "persistent": {
        "plugins": {
          "calcite": {
            "enabled": "true"
          }
        }
      },
      "transient": {}
    }

Example 1: Append rows from a count aggregation to existing search result
===============================================================

This example appends rows from "count by gender" to "sum by gender, state".

PPL query::

    PPL> source=accounts | stats sum(age) by gender, state | sort -`sum(age)` | head 5 | append [ stats count(age) by gender ];
    fetched rows / total rows = 7/7
    +----------+--------+-------+------------+
    | sum(age) | gender | state | count(age) |
    |----------+--------+-------+------------|
    | 580      | M      | MD    | NULL       |
    | 547      | M      | ID    | NULL       |
    | 485      | F      | TX    | NULL       |
    | 472      | M      | OK    | NULL       |
    | 452      | M      | ME    | NULL       |
    | NULL     | F      | NULL  | 493        |
    | NULL     | M      | NULL  | 507        |
    +----------+--------+-------+------------+

Example 2: Append rows with merged column names
====================================================================================

This example appends rows from "sum by gender" to "sum by gender, state" with merged column of same field name and type.

PPL query::

    PPL> source=accounts | stats sum(age) as sum by gender, state | sort -sum | head 5 | append [ stats sum(age) as sum by gender ];
    fetched rows / total rows = 7/7
    +--------+--------+-------+
    | sum    | gender | state |
    |--------+--------+-------+
    | 580    | M      | MD    |
    | 547    | M      | ID    |
    | 485    | F      | TX    |
    | 472    | M      | OK    |
    | 452    | M      | ME    |
    | 14947  | F      | NULL  |
    | 15224  | M      | NULL  |
    +--------+--------+-------+

Example 3: Append rows with column type conflict
=============================================

This example shows how column type conflicts are handled when appending results. Same name columns with different types will generate two different columns in appended result.

PPL query::

    PPL> source=accounts | stats sum(age) as sum by gender, state | sort -sum | head 5 | append [ stats sum(age) as sum by gender | eval sum = cast(sum as double) ];
    fetched rows / total rows = 7/7
    +------+--------+-------+-------+
    | sum  | gender | state | sum0  |
    |------+--------+-------+-------+
    | 580  | M      | MD    | NULL  |
    | 547  | M      | ID    | NULL  |
    | 485  | F      | TX    | NULL  |
    | 472  | M      | OK    | NULL  |
    | 452  | M      | ME    | NULL  |
    | NULL | F      | NULL  | 14947 |
    | NULL | M      | NULL  | 15224 |
    +------+--------+-------+-------+

