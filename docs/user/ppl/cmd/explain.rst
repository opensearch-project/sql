=============
explain
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``explain`` command to explain the plan of query which is used very often for query translation and troubleshooting. ``explain`` command could be only used as the first command in the PPL query.


Syntax
============
explain <mode> queryStatement

* mode: optional. There are 4 explain modes: "simple", "standard", "cost", "extended". If mode is not provided, "standard" will be set by default.
 * standard: The default mode. Display logical and physical plan with pushdown information (DSL).
 * simple: Display the logical plan tree without attributes. Only works with Calcite.
 * cost: Display the standard information plus plan cost attributes. Only works with Calcite.
 * extended: Display the standard information plus generated code. Only works with Calcite.
* queryStatement: mandatory. A PPL query to explain.



Example 1: Explain a PPL query in v2 engine
==============================
When Calcite is disabled (plugins.calcite.enabled=false), explaining a PPL query will get its physical plan of v2 engine and pushdown information.

PPL query::

    PPL> explain source=state_country | where country = 'USA' OR country = 'England' | stats count() by country

Explain::

    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[count(), country]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": """OpenSearchQueryRequest(indexName=state_country, sourceBuilder={"from":0,"size":10000,"timeout":"1m","query":{"bool":{"should":[{"term":{"country":{"value":"USA","boost":1.0}}},{"term":{"country":{"value":"England","boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}},"sort":[{"_doc":{"order":"asc"}}],"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, needClean=true, searchDone=false, pitId=null, cursorKeepAlive=null, searchAfter=null, searchResponse=null)"""
            },
            "children": []
          }
        ]
      }
    }

Example 2: Explain a PPL query in v3 engine
===================================================

When Calcite is enabled (plugins.calcite.enabled=true), explaining a PPL query will get its logical and physical plan of v3 engine and pushdown information.

PPL query::

    PPL> explain source=state_country | where country = 'USA' OR country = 'England' | stats count() by country

Explain::

    {
      "calcite": {
        "logical": """LogicalProject(count()=[$1], country=[$0])
      LogicalAggregate(group=[{1}], count()=[COUNT()])
        LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))])
          CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
    """,
        "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0])
      CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#53:LogicalAggregate.NONE.[](input=RelSubset#43,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"sort":[{"_doc":{"order":"asc"}}],"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])
    """
      }
    }


Example 3: Explain a PPL query with simple mode
=========================================================

When Calcite is enabled (plugins.calcite.enabled=true), you can explain a PPL query will the "simple" mode.

PPL query::

    PPL> explain simple source=state_country | where country = 'USA' OR country = 'England' | stats count() by country

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

Example 4: Explain a PPL query with cost mode
=========================================================

When Calcite is enabled (plugins.calcite.enabled=true), you can explain a PPL query will the "cost" mode.

PPL query::

    PPL> explain cost source=state_country | where country = 'USA' OR country = 'England' | stats count() by country

Explain::

    {
      "calcite": {
        "logical": """LogicalProject(count()=[$1], country=[$0]): rowcount = 2.5, cumulative cost = {130.3125 rows, 206.0 cpu, 0.0 io}, id = 75
      LogicalAggregate(group=[{1}], count()=[COUNT()]): rowcount = 2.5, cumulative cost = {127.8125 rows, 201.0 cpu, 0.0 io}, id = 74
        LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))]): rowcount = 25.0, cumulative cost = {125.0 rows, 201.0 cpu, 0.0 io}, id = 73
          CalciteLogicalIndexScan(table=[[OpenSearch, state_country]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 72
    """,
        "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0]): rowcount = 100.0, cumulative cost = {200.0 rows, 501.0 cpu, 0.0 io}, id = 138
      CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#125:LogicalAggregate.NONE.[](input=RelSubset#115,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"sort":[{"_doc":{"order":"asc"}}],"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=10000, pageSize=null, startFrom=0)]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 133
    """
      }
    }

Example 5: Explain a PPL query with extended mode
=========================================================

When Calcite is enabled (plugins.calcite.enabled=true), you can explain a PPL query will the "extended" mode.

PPL query::

    PPL> explain extended source=state_country | where country = 'USA' OR country = 'England' | stats count() by country

Explain::

    {
      "calcite": {
        "logical": """LogicalProject(count()=[$1], country=[$0])
      LogicalAggregate(group=[{1}], count()=[COUNT()])
        LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))])
          CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
    """,
        "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0])
      CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#193:LogicalAggregate.NONE.[](input=RelSubset#183,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"sort":[{"_doc":{"order":"asc"}}],"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])
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

