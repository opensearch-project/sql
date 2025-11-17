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

To send query request to PPL plugin, you MUST use HTTP POST request. POST request doesn't have length limitation and allows for other parameters passed to plugin for other functionality such as prepared statement. And also the explain endpoint is used very often for query translation and troubleshooting.

POST
====

Description
-----------

You can send HTTP POST request to endpoint **/_plugins/_ppl** with your query in request body.

Example
-------

PPL query::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl \
    ... -d '{"query" : "source=accounts | fields firstname, lastname"}'
    {
      "schema": [
        {
          "name": "firstname",
          "type": "string"
        },
        {
          "name": "lastname",
          "type": "string"
        }
      ],
      "datarows": [
        [
          "Amber",
          "Duke"
        ],
        [
          "Hattie",
          "Bond"
        ],
        [
          "Nanette",
          "Bates"
        ],
        [
          "Dale",
          "Adams"
        ]
      ],
      "total": 4,
      "size": 4
    }

Explain
=======

Description
-----------

You can send HTTP explain request to endpoint **/_plugins/_ppl/_explain** with your query in request body to understand the execution plan for the PPL query. The explain endpoint is useful when user want to get insight how the query is executed in the engine.

Description
-----------

To translate your query, send it to explain endpoint. The explain output is OpenSearch domain specific language (DSL) in JSON format. You can just copy and paste it to your console to run it against OpenSearch directly.

Explain output could be set different formats: ``standard`` (the default format), ``simple``, ``extended``, ``dsl``.


Example 1 default (standard) format
-----------------------------------

Explain query::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl/_explain \
    ... -d '{"query" : "source=state_country | where age>30"}'
    {
      "calcite": {
        "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n  LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])\n    LogicalFilter(condition=[>($5, 30)])\n      CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])\n",
        "physical": "CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":30,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}},\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"],\"excludes\":[]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])\n"
      }
    }

Example 2 simple format
-----------------------

Explain query::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl/_explain?format=simple \
    ... -d '{"query" : "source=state_country | where age>30"}'
    {
      "calcite": {
        "logical": "LogicalSystemLimit\n  LogicalProject\n    LogicalFilter\n      CalciteLogicalIndexScan\n"
      }
    }

Example 3 extended format
-------------------------

Explain query::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl/_explain?format=extended \
    ... -d '{"query" : "source=state_country | where age>30 | dedup age"}'
    {
      "calcite": {
        "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n  LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])\n    LogicalFilter(condition=[<=($12, 1)])\n      LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5], _id=[$6], _index=[$7], _score=[$8], _maxscore=[$9], _sort=[$10], _routing=[$11], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $5 ORDER BY $5)])\n        LogicalFilter(condition=[IS NOT NULL($5)])\n          LogicalFilter(condition=[>($5, 30)])\n            CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])\n",
        "physical": "EnumerableLimit(fetch=[10000])\n  EnumerableCalc(expr#0..6=[{inputs}], expr#7=[1], expr#8=[<=($t6, $t7)], proj#0..5=[{exprs}], $condition=[$t8])\n    EnumerableWindow(window#0=[window(partition {5} order by [5] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])])\n      CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30)], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":30,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}},\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"],\"excludes\":[]}}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)])\n",
        "extended": "public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {\n  final org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan v1stashed = (org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan) root.get(\"v1stashed\");\n  int prevStart;\n  int prevEnd;\n  final java.util.Comparator comparator = new java.util.Comparator(){\n    public int compare(Object[] v0, Object[] v1) {\n      final int c;\n      c = org.apache.calcite.runtime.Utilities.compareNullsLast((Long) v0[5], (Long) v1[5]);\n      if (c != 0) {\n        return c;\n      }\n      return 0;\n    }\n\n    public int compare(Object o0, Object o1) {\n      return this.compare((Object[]) o0, (Object[]) o1);\n    }\n\n  };\n  final org.apache.calcite.runtime.SortedMultiMap multiMap = new org.apache.calcite.runtime.SortedMultiMap();\n  v1stashed.scan().foreach(new org.apache.calcite.linq4j.function.Function1() {\n    public Object apply(Object[] v) {\n      Long key = (Long) v[5];\n      multiMap.putMulti(key, v);\n      return null;\n    }\n    public Object apply(Object v) {\n      return apply(\n        (Object[]) v);\n    }\n  }\n  );\n  final java.util.Iterator iterator = multiMap.arrays(comparator);\n  final java.util.ArrayList _list = new java.util.ArrayList(\n    multiMap.size());\n  Long a0w0 = (Long) null;\n  while (iterator.hasNext()) {\n    final Object[] _rows = (Object[]) iterator.next();\n    prevStart = -1;\n    prevEnd = 2147483647;\n    for (int i = 0; i < _rows.length; (++i)) {\n      final Object[] row = (Object[]) _rows[i];\n      if (i != prevEnd) {\n        int actualStart = i < prevEnd ? 0 : prevEnd + 1;\n        prevEnd = i;\n        a0w0 = Long.valueOf(((Number)org.apache.calcite.linq4j.tree.Primitive.of(long.class).numberValueRoundDown((i - 0 + 1))).longValue());\n      }\n      _list.add(new Object[] {\n        row[0],\n        row[1],\n        row[2],\n        row[3],\n        row[4],\n        row[5],\n        a0w0});\n    }\n  }\n  multiMap.clear();\n  final org.apache.calcite.linq4j.Enumerable _inputEnumerable = org.apache.calcite.linq4j.Linq4j.asEnumerable(_list);\n  final org.apache.calcite.linq4j.AbstractEnumerable child = new org.apache.calcite.linq4j.AbstractEnumerable(){\n    public org.apache.calcite.linq4j.Enumerator enumerator() {\n      return new org.apache.calcite.linq4j.Enumerator(){\n          public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable.enumerator();\n          public void reset() {\n            inputEnumerator.reset();\n          }\n\n          public boolean moveNext() {\n            while (inputEnumerator.moveNext()) {\n              if (org.apache.calcite.runtime.SqlFunctions.toLong(((Object[]) inputEnumerator.current())[6]) <= $L4J$C$_Number_org_apache_calcite_linq4j_tree_Primitive_of_long_class_358aa52b) {\n                return true;\n              }\n            }\n            return false;\n          }\n\n          public void close() {\n            inputEnumerator.close();\n          }\n\n          public Object current() {\n            final Object[] current = (Object[]) inputEnumerator.current();\n            final Object input_value = current[0];\n            final Object input_value0 = current[1];\n            final Object input_value1 = current[2];\n            final Object input_value2 = current[3];\n            final Object input_value3 = current[4];\n            final Object input_value4 = current[5];\n            return new Object[] {\n                input_value,\n                input_value0,\n                input_value1,\n                input_value2,\n                input_value3,\n                input_value4};\n          }\n\n          static final long $L4J$C$_Number_org_apache_calcite_linq4j_tree_Primitive_of_long_class_358aa52b = ((Number)org.apache.calcite.linq4j.tree.Primitive.of(long.class).numberValueRoundDown(1)).longValue();\n        };\n    }\n\n  };\n  return child.take(10000);\n}\n\n\npublic Class getElementType() {\n  return java.lang.Object[].class;\n}\n\n\n"
      }
    }

Example 4 YAML format (experimental)
-----------------------------------

.. note::
   YAML explain output is an experimental feature and not intended for
   production use. The interface and output may change without notice.

Return Explain response format in In ``yaml`` format.

Explain query::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl/_explain?format=yaml \
    ... -d '{"query" : "source=state_country | where age>30"}'
    calcite:
      logical: |
        LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])
          LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])
            LogicalFilter(condition=[>($5, 30)])
              CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
      physical: |
        CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":10000,"timeout":"1m","query":{"range":{"age":{"from":30,"to":null,"include_lower":false,"include_upper":true,"boost":1.0}}},"_source":{"includes":["name","country","state","month","year","age"],"excludes":[]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])
