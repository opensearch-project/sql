=============
evenstats
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| (Experimental)
| Using ``evenstats`` command to enriches your event data with calculated summary statistics. It operates by analyzing specified fields within your events, computing various statistical measures, and then appending these results as new fields to each original event.

| Key aspects of `eventstats`:

1. It performs calculations across the entire result set or within defined groups.
2. The original events remain intact, with new fields added to contain the statistical results.
3. The command is particularly useful for comparative analysis, identifying outliers, or providing additional context to individual events.

| Difference between ``stats`` and ``eventstats``
The ``stats`` and ``eventstats`` commands are both used for calculating statistics, but they have some key differences in how they operate and what they produce:

* Output Format:
 * ``stats``: Produces a summary table with only the calculated statistics.
 * ``eventstats``: Adds the calculated statistics as new fields to the existing events, preserving the original data.
* Event Retention:
 * ``stats``: Reduces the result set to only the statistical summary, discarding individual events.
 * ``eventstats``: Retains all original events and adds new fields with the calculated statistics.
* Use Cases:
 * ``stats``: Best for creating summary reports or dashboards. Often used as a final command to summarize results.
 * ``eventstats``: Useful when you need to enrich events with statistical context for further analysis or filtering. Can be used mid-search to add statistics that can be used in subsequent commands.


Version
=======
3.1.0+


Syntax
======
eventstats <function>... [by-clause]


* function: mandatory. A aggregation function or window function.

* by-clause: optional.

 * Syntax: by [span-expression,] [field,]...
 * Description: The by clause could be the fields and expressions like scalar functions and aggregation functions. Besides, the span clause can be used to split specific field into buckets in the same interval, the stats then does the aggregation by these span buckets.
 * Default: If no <by-clause> is specified, the stats command returns only one row, which is the aggregation over the entire result set.

* span-expression: optional, at most one.

 * Syntax: span(field_expr, interval_expr)
 * Description: The unit of the interval expression is the natural unit by default. If the field is a date and time type field, and the interval is in date/time units, you will need to specify the unit in the interval expression. For example, to split the field ``age`` into buckets by 10 years, it looks like ``span(age, 10)``. And here is another example of time span, the span to split a ``timestamp`` field into hourly intervals, it looks like ``span(timestamp, 1h)``.

* Available time unit:
+----------------------------+
| Span Interval Units        |
+============================+
| millisecond (ms)           |
+----------------------------+
| second (s)                 |
+----------------------------+
| minute (m, case sensitive) |
+----------------------------+
| hour (h)                   |
+----------------------------+
| day (d)                    |
+----------------------------+
| week (w)                   |
+----------------------------+
| month (M, case sensitive)  |
+----------------------------+
| quarter (q)                |
+----------------------------+
| year (y)                   |
+----------------------------+

Aggregation Functions
=====================
COUNT
-----

Description
>>>>>>>>>>>

Usage: Returns a count of the number of expr in the rows retrieved by a SELECT statement.

Example::

    PPL> source=accounts | eventstats count();
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | count() |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 4       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 4       |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 4       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 4       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------+

SUM
---

Description
>>>>>>>>>>>

Usage: SUM(expr). Returns the sum of expr.

Example::

    PPL> source=accounts | eventstats sum(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | sum(age) |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28       |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 101      |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 101      |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 101      |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+

AVG
---

Description
>>>>>>>>>>>

Usage: AVG(expr). Returns the average value of expr.

Example::

    PPL> source=accounts | eventstats avg(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | avg(age)           |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28.0               |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 33.666666666666664 |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 33.666666666666664 |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 33.666666666666664 |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+

MAX
---

Description
>>>>>>>>>>>

Usage: MAX(expr). Returns the maximum value of expr.

Example::

    PPL> source=accounts | eventstats max(age);
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | max(age) |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 36       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 36       |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 36       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 36       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+

MIN
---

Description
>>>>>>>>>>>

Usage: MIN(expr). Returns the minimum value of expr.

Example::

    PPL> source=accounts | eventstats min(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | min(age) |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28       |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 32       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 32       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+


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

Usage
=====

Eventstats::

    source = table | eventstats avg(a)
    source = table | where a < 50 | eventstats count(c)
    source = table | eventstats min(c), max(c) by b
    source = table | eventstats count(c) as count_by by b | where count_by > 1000


Example 1: Calculate the average, sum and count of a field by group
==================================================================

The example show calculate the average age, sum age and count of events of all the accounts group by gender.

PPL query::

    PPL> source=accounts | eventstats avg(age), sum(age), count() by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+----------+---------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | avg(age)           | sum(age) | count() |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+----------+---------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28.0               | 28       | 1       |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 33.666666666666664 | 101      | 3       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 33.666666666666664 | 101      | 3       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 33.666666666666664 | 101      | 3       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+----------+---------+


Example 2: Calculate the count by a gender and span
===================================================

The example gets the count of age by the interval of 10 years and group by gender.

PPL query::

    PPL> source=accounts | eventstats count() as cnt by span(age, 5) as age_span, gender
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-----+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | cnt |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-----|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 2   |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 2   |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 1   |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 1   |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-----+

