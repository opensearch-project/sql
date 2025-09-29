=============
join
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| Using ``join`` command to combines two datasets together. The left side could be an index or results from a piped commands, the right side could be either an index or a subsearch.

Version
=======
3.0.0

Basic syntax in 3.0.0
=====================
| [joinType] join [leftAlias] [rightAlias] (on | where) <joinCriteria> <right-dataset>

* joinType: optional. The type of join to perform. The default is ``inner`` if not specified. Other option is ``left``, ``semi``, ``anti`` and performance sensitive types ``right``, ``full`` and ``cross``.
* leftAlias: optional. The subsearch alias to use with the left join side, to avoid ambiguous naming. Fixed pattern: ``left = <leftAlias>``
* rightAlias: optional. The subsearch alias to use with the right join side, to avoid ambiguous naming. Fixed pattern: ``right = <rightAlias>``
* joinCriteria: mandatory. It could be any comparison expression. Must follow with ``on`` (since 3.0.0) or ``where`` (since 3.3.0) keyword.
* right-dataset: mandatory. Right dataset could be either an ``index`` or a ``subsearch`` with/without alias.

Extended syntax since 3.3.0
===========================
| join [type=<joinType>] [overwrite=<bool>] [max=n] (<join-field-list> | [leftAlias] [rightAlias] (on | where) <joinCriteria>) <right-dataset>
| From 3.3.0, the join syntax is enhanced to support more join options and join with field list.

* type=<joinType>: optional. The type of join to perform. The default is ``inner`` if not specified. Other option is ``left``, ``outer``(alias of ``left``), ``semi``, ``anti`` and performance sensitive types ``right``, ``full`` and ``cross``.
* overwrite=<bool>: optional. Only works with ``join-field-list``. Specifies whether duplicate-named fields from <right-dataset> (subsearch results) should replace corresponding fields in the main search results. The default value is ``true``.
* max=n: optional. Controls how many subsearch results could be joined against to each row in main search. The default value is 0, means unlimited.
* join-field-list: optional. The fields used to build the join criteria. The join field list must exist on both sides. If no join field list is specified, all fields common to both sides will be used as join keys. The comma is optional.

Configuration
=============
This command requires Calcite enabled. In 3.0.0, as an experimental the Calcite configuration is disabled by default.

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

Join on criteria (in 3.0.0)::

    source = table1 | inner join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | inner join left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | left join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | right join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | full left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | cross join left = l right = r on 1=1 table2
    source = table1 | left semi join left = l right = r on l.a = r.a table2
    source = table1 | left anti join left = l right = r on l.a = r.a table2
    source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ]
    source = table1 | inner join on table1.a = table2.a table2 | fields table1.a, table2.a, table1.b, table1.c
    source = table1 | inner join on a = c table2 | fields a, b, c, d
    source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields l.a, r.a
    source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields t1.a, t2.a
    source = table1 | join left = l right = r on l.a = r.a [ source = table2 ] as s | fields l.a, s.a

Extended syntax and option supported (since 3.3.0)::

    source = table1 | join type=outer left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | join type=left left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | join type=inner max=1 left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | join a table2 | fields a, b, c
    source = table1 | join a, b table2 | fields a, b, c
    source = table1 | join type=outer a b table2 | fields a, b, c
    source = table1 | join type=inner max=1 a, b table2 | fields a, b, c
    source = table1 | join type=left overwrite=false max=0 a, b [source=table2 | rename d as b] | fields a, b, c

Example 1: Two indices join
===========================

PPL query::

    os> source = state_country | inner join left=a right=b ON a.name = b.name occupation | stats avg(salary) by span(age, 10) as age_span, b.country;
    fetched rows / total rows = 5/5
    +-------------+----------+-----------+
    | avg(salary) | age_span | b.country |
    |-------------+----------+-----------|
    | 120000.0    | 40       | USA       |
    | 105000.0    | 20       | Canada    |
    |  0.0        | 40       | Canada    |
    | 70000.0     | 30       | USA       |
    | 100000.0    | 70       | England   |
    +-------------+----------+-----------+

Example 2: Join with subsearch
==============================

PPL query::

    PPL> source = state_country as a | where country = 'USA' OR country = 'England' | left join ON a.name = b.name [ source = occupation | where salary > 0 | fields name, country, salary | sort salary | head 3 ] as b | stats avg(salary) by span(age, 10) as age_span, b.country;
    fetched rows / total rows = 3/3
    +-------------+----------+-----------+
    | avg(salary) | age_span | b.country |
    |-------------+----------+-----------|
    | null        | 40       | null      |
    | 70000.0     | 30       | USA       |
    | 100000.0    | 70       | England   |
    +-------------+----------+-----------+

Example 3: Join with field list
===============================

PPL query::

    PPL> source = state_country | where country = 'USA' OR country = 'England' | join type=left overwrite=true name [ source = occupation | where salary > 0 | fields name, country, salary | sort salary | head 3 ] | stats avg(salary) by span(age, 10) as age_span, country;
    fetched rows / total rows = 3/3
    +-------------+----------+---------+
    | avg(salary) | age_span | country |
    |-------------+----------+---------|
    | null        | 40       | null    |
    | 70000.0     | 30       | USA     |
    | 100000.0    | 70       | England |
    +-------------+----------+---------+

Example 4: Join with options
============================

PPL query::

    os> source = state_country | join type=inner overwrite=false max=1 name occupation | stats avg(salary) by span(age, 10) as age_span, country;
    fetched rows / total rows = 4/4
    +-------------+----------+---------+
    | avg(salary) | age_span | country |
    |-------------+----------+---------|
    | 120000.0    | 40       | USA     |
    | 100000.0    | 70       | USA     |
    | 105000.0    | 20       | Canada  |
    | 70000.0     | 30       | USA     |
    +-------------+----------+---------+

Limitations
===========
For basic syntax in 3.0.0, if fields in the left outputs and right outputs have the same name. Typically, in the join criteria
``ON t1.id = t2.id``, the names ``id`` in output are ambiguous. To avoid ambiguous, the ambiguous
fields in output rename to ``<alias>.id``, or else ``<tableName>.id`` if no alias existing.

Assume table1 and table2 only contain field ``id``, following PPL queries and their outputs are:

.. list-table::
   :widths: 75 25
   :header-rows: 1

   * - Query
     - Output
   * - source=table1 | join left=t1 right=t2 on t1.id=t2.id table2 | eval a = 1
     - t1.id, t2.id, a
   * - source=table1 | join on table1.id=table2.id table2 | eval a = 1
     - table1.id, table2.id, a
   * - source=table1 | join on table1.id=t2.id table2 as t2 | eval a = 1
     - table1.id, t2.id, a
   * - source=table1 | join right=tt on table1.id=t2.id [ source=table2 as t2 | eval b = id ] | eval a = 1
     - table1.id, tt.id, tt.b, a

For extended syntax (join with field list) in 3.3.0, when duplicate-named fields in output results are deduplicated, the fields in output determined by the value of 'overwrite' option.

Since 3.3.0, join types ``inner``, ``left``, ``outer`` (alias of ``left``), ``semi`` and ``anti`` are supported by default. ``right``, ``full``, ``cross`` are performance sensitive join types which are disabled by default. Set config ``plugins.calcite.all_join_types.allowed = true`` to enable.
