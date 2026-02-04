.. highlight:: sh

========
Endpoint
========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

To send query request to SQL plugin, you MUST use HTTP POST request. POST request doesn't have length limitation and allows for other parameters passed to plugin for other functionality such as prepared statement. And also the explain endpoint is used very often for query translation and troubleshooting.

POST
====

Description
-----------

You can also send HTTP POST request with your query in request body.

Example
-------

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql -d '{
	  "query" : "SELECT * FROM accounts"
	}'

Explain
=======

Description
-----------

To translate your query, send it to explain endpoint. The explain output is OpenSearch domain specific language (DSL) in JSON format. You can just copy and paste it to your console to run it against OpenSearch directly.

For queries which run with Calcite engine (V3), explain output could be set different formats: ``standard`` (the default format), ``simple`` and ``extended``.

Example 1
-------

Explain query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql/_explain -d '{
	  "query" : "SELECT firstname, lastname FROM accounts WHERE age > 20"
	}'

Explain::

	{
	  "from" : 0,
	  "size" : 200,
	  "query" : {
	    "bool" : {
	      "filter" : [
	        {
	          "bool" : {
	            "must" : [
	              {
	                "range" : {
	                  "age" : {
	                    "from" : 20,
	                    "to" : null,
	                    "include_lower" : false,
	                    "include_upper" : true,
	                    "boost" : 1.0
	                  }
	                }
	              }
	            ],
	            "adjust_pure_negative" : true,
	            "boost" : 1.0
	          }
	        }
	      ],
	      "adjust_pure_negative" : true,
	      "boost" : 1.0
	    }
	  },
	  "_source" : {
	    "includes" : [
	      "firstname",
	      "lastname"
	    ],
	    "excludes" : [ ]
	  }
	}

Example 2 with Calcite: standard
-------
To enable Calcite engine, set `plugins.calcite.enabled <../../admin/settings.rst>`_ to ``true``.

Explain query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql/_explain -d '{
	  "query" : "source = state_country | where country = 'USA' OR country = 'England' | stats count() by country"
	}'

Explain::

    {
      "calcite": {
        "logical": """LogicalProject(count()=[$1], country=[$0])
      LogicalAggregate(group=[{1}], count()=[COUNT()])
        LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))])
          CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
    """,
        "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0])
      CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#57:LogicalAggregate.NONE.[](input=RelSubset#47,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)])
    """
      }
    }

Example 3 with Calcite: simple
-------

To enable Calcite engine, set `plugins.calcite.enabled <../../admin/settings.rst>`_ to ``true``.

Explain query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql/_explain?format=simple -d '{
	  "query" : "source = state_country | where country = 'USA' OR country = 'England' | stats count() by country"
	}'

Explain::

    {
      "calcite": {
        "logical": """LogicalProject
      LogicalAggregate
        LogicalFilter
          CalciteLogicalIndexScan
    """
      }
    }

Example 4 with Calcite: extended
-------

To enable Calcite engine, set `plugins.calcite.enabled <../../admin/settings.rst>`_ to ``true``.

Explain query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql/_explain?format=extended -d '{
	  "query" : "source = state_country | where country = 'USA' OR country = 'England' | stats count() by country"
	}'

Explain::

    {
      "calcite": {
        "logical": """LogicalProject(count()=[$1], country=[$0])
      LogicalAggregate(group=[{1}], count()=[COUNT()])
        LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))])
          CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
    """,
        "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0])
      CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#125:LogicalAggregate.NONE.[](input=RelSubset#115,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)])
    """,
        "extended": """public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {
      final org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan v1stashed = (org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan) root.get("v1stashed");
      final org.apache.calcite.linq4j.Enumerable _inputEnumerable = v1stashed.scan();
      return new org.apache.calcite.linq4j.AbstractEnumerable(){
          public org.apache.calcite.linq4j.Enumerator enumerator() {
            return new org.apache.calcite.linq4j.Enumerator(){
                public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable.enumerator();
                public void reset() {
                  inputEnumerator.reset();
                }

                public boolean moveNext() {
                  return inputEnumerator.moveNext();
                }

                public void close() {
                  inputEnumerator.close();
                }

                public Object current() {
                  final Object[] current = (Object[]) inputEnumerator.current();
                  final Object input_value = current[1];
                  final Object input_value0 = current[0];
                  return new Object[] {
                      input_value,
                      input_value0};
                }

              };
          }

        };
    }


    public Class getElementType() {
      return java.lang.Object[].class;
    }


    """
      }
    }

Cursor (SQL)
============

Description
-----------

To get paginated response for a SQL query, user needs to provide `fetch_size` parameter as part of normal query. The value of `fetch_size` should be greater than `0`. In absence of `fetch_size` or a value of `0`, it will fallback to non-paginated response. This feature is only available over `jdbc` format for now.

Example
-------

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql -d '{
	  "fetch_size" : 5,
	  "query" : "SELECT firstname, lastname FROM accounts WHERE age > 20 ORDER BY state ASC"
	}'

Result set::

    {
      "schema": [
        {
          "name": "firstname",
          "type": "text"
        },
        {
          "name": "lastname",
          "type": "text"
        }
      ],
      "cursor": "d:eyJhIjp7fSwicyI6IkRYRjFaWEo1UVc1a1JtVjBZMmdCQUFBQUFBQUFBQU1XZWpkdFRFRkZUMlpTZEZkeFdsWnJkRlZoYnpaeVVRPT0iLCJjIjpbeyJuYW1lIjoiZmlyc3RuYW1lIiwidHlwZSI6InRleHQifSx7Im5hbWUiOiJsYXN0bmFtZSIsInR5cGUiOiJ0ZXh0In1dLCJmIjo1LCJpIjoiYWNjb3VudHMiLCJsIjo5NTF9",
      "total": 956,
      "datarows": [
        [
          "Cherry",
          "Carey"
        ],
        [
          "Lindsey",
          "Hawkins"
        ],
        [
          "Sargent",
          "Powers"
        ],
        [
          "Campos",
          "Olsen"
        ],
        [
          "Savannah",
          "Kirby"
        ]
      ],
      "size": 5,
      "status": 200
    }

Fetch Size (PPL)
================

Description
-----------

PPL also supports the ``fetch_size`` parameter, but with different semantics from SQL. In PPL, ``fetch_size`` limits the number of rows returned in a single, complete response. **PPL does not support cursor-based pagination** — no cursor is returned and there is no way to fetch additional pages. The value of ``fetch_size`` should be greater than ``0``. In absence of ``fetch_size`` or a value of ``0``, it will use the system default behavior (no limit). The effective upper bound is governed by the ``plugins.query.size_limit`` cluster setting (defaults to ``index.max_result_window``, which is 10000).

``fetch_size`` can be specified either as a URL parameter or in the JSON request body. If both are provided, the JSON body value takes precedence.

+--------------------+-------------------------------------+------------------------------------+
| Aspect             | SQL ``fetch_size``                  | PPL ``fetch_size``                 |
+====================+=====================================+====================================+
| Purpose            | Cursor-based pagination             | Response size limiting             |
+--------------------+-------------------------------------+------------------------------------+
| Returns cursor?    | Yes                                 | No                                 |
+--------------------+-------------------------------------+------------------------------------+
| Can fetch more?    | Yes (with cursor)                   | No (single response)               |
+--------------------+-------------------------------------+------------------------------------+

Example 1: JSON body
-------

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "fetch_size" : 5,
	  "query" : "source = accounts | fields firstname, lastname | where age > 20"
	}'

Example 2: URL parameter
-------

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl?fetch_size=5 -d '{
	  "query" : "source = accounts | fields firstname, lastname | where age > 20"
	}'

Result set::

    {
      "schema": [
        {
          "name": "firstname",
          "type": "text"
        },
        {
          "name": "lastname",
          "type": "text"
        }
      ],
      "total": 5,
      "datarows": [
        ["Cherry", "Carey"],
        ["Lindsey", "Hawkins"],
        ["Sargent", "Powers"],
        ["Campos", "Olsen"],
        ["Savannah", "Kirby"]
      ],
      "size": 5,
      "status": 200
    }

Note that unlike the SQL response above, there is no ``cursor`` field in the PPL response. The response is complete and final.
