=============
join
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| (Experimental)
| (From 3.0.0)
| Using ``join`` command to combines two datasets together. The left side could be an index or results from a piped commands, the right side could be either an index or a subquery.

Version
=======
3.0.0

Syntax
======
[joinType] JOIN [leftAlias] [rightAlias] ON <joinCriteria> <right-dataset>

* joinType: optional. The type of join to perform. The default is ``INNER`` if not specified. Other option is ``LEFT [OUTER]``, ``RIGHT [OUTER]``, ``FULL [OUTER]``, ``CROSS``, ``[LEFT] SEMI``, ``[LEFT] ANTI``.
* leftAlias: optional. The subquery alias to use with the left join side, to avoid ambiguous naming. Fixed pattern: ``left = <leftAlias>``
* rightAlias: optional. The subquery alias to use with the right join side, to avoid ambiguous naming. Fixed pattern: ``right = <rightAlias>``
* joinCriteria: mandatory. It could be any comparison expression.
* right-dataset: mandatory. Right dataset could be either an index or a subquery with/without alias.

Configuration
=============
This command requires Calcite enabled. In 3.0.0-beta, as an experimental the Calcite configuration is disabled by default.

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

Join::

    source = table1 | inner join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | left join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | right join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | full left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
    source = table1 | cross join left = l right = r table2
    source = table1 | left semi join left = l right = r on l.a = r.a table2
    source = table1 | left anti join left = l right = r on l.a = r.a table2
    source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ]
    source = table1 | inner join on table1.a = table2.a table2 | fields table1.a, table2.a, table1.b, table1.c
    source = table1 | inner join on a = c table2 | fields a, b, c, d
    source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields l.a, r.a
    source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields t1.a, t2.a
    source = table1 | join left = l right = r on l.a = r.a [ source = table2 ] as s | fields l.a, s.a


Example 1: Two indices join
===========================

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
	  source = state_country
	  | inner join left=a right=b ON a.name = b.name occupation
	  | stats avg(salary) by span(age, 10) as age_span, b.country
	  """
	}'

Result set::

    {
      "schema": [
        {
          "name": "avg(salary)",
          "type": "double"
        },
        {
          "name": "age_span",
          "type": "integer"
        },
        {
          "name": "b.country",
          "type": "string"
        }
      ],
      "datarows": [
        [
          120000.0,
          40,
          "USA"
        ],
        [
          105000.0,
          20,
          "Canada"
        ],
        [
          0.0,
          40,
          "Canada"
        ],
        [
          70000.0,
          30,
          "USA"
        ],
        [
          100000.0,
          70,
          "England"
        ]
      ],
      "total": 5,
      "size": 5
    }

Example 2: Join with subsearch
==============================

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
          source = state_country as a
          | where country = 'USA' OR country = 'England'
          | left join ON a.name = b.name [
              source = occupation
              | where salary > 0
              | fields name, country, salary
              | sort salary
              | head 3
            ] as b
          | stats avg(salary) by span(age, 10) as age_span, b.country
	  """
	}'

Result set::

    {
      "schema": [
        {
          "name": "avg(salary)",
          "type": "double"
        },
        {
          "name": "age_span",
          "type": "integer"
        },
        {
          "name": "b.country",
          "type": "string"
        }
      ],
      "datarows": [
        [
          null,
          40,
          null
        ],
        [
          70000.0,
          30,
          "USA"
        ],
        [
          100000.0,
          70,
          "England"
        ]
      ],
      "total": 3,
      "size": 3
    }

