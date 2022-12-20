# Pagination in v2 Engine

Pagination allows a SQL plugin client to retrieve arbitrarily large
results sets one subset at a time.

A cursor is a SQL abstraction for pagination. A client can open a cursor, retrieve a subset of data given a cursor and close a cursor.

Currently, SQL plugin does not provide SQL cursor syntax. However, the SQL REST endpoint can return result a page at a time. This feature is used by JDBC and ODBC drivers.


# Scope
The primary goal is to reach feature parity with the v1 engine. v1 engine supports pagination only for simple `SELECT` queries with optional `WHERE` or `ORDER BY` clauses.

# REST API
## Initial Query Request
```
POST /_plugins/_sql
{
    "query" : "...",
    "fetch_size": N
}
```

Response:
```
{
  "cursor": /* cursor_id */,
  "datarows": [
    // ...
    ],
  "schema" : [
    // ...
  ]
}
```
`query` is a DQL statement. `fetch_size` is a positive integer, indicating number of rows to return in each page.

If `query` is a DML statement then pagination does not apply, the `fetch_size` parameter is ignored and a cursor is not created. This is existing behaviour in v1 engine.

The client receives an (error response](#error-response) if:
- `fetch_size` is not a positive integer, or
-  evaluating `query` results in a server-side error. 

## Next Page Request
```
POST /_plugins/_sql
{
  "cursor": "<cursor_id>"
}
```
Similarly to v1 engine, the response object is the same as initial response if this is not the last page.

`cursor_id` will be different with each request.

If this is the last page, the `cursor` property is ommitted. The cursor is closed automatically.

The client will receive an [error response](#error-response) if executing this request results in an OpenSearch or SQL plug-in error. 

## Close Cursor Request
```
POST /_plugins/_sql/close
{
    "cursor" : "<cursor_id>"
}
```

Response:
```json
{
    "succeeded": true
}

```
The SQL engine always returns success on close even if the cursor is no longer alive as long as it can validate `<cursor_id>` as a valid cursor identifier.

It [responds with an error](#error-response) if `<cursor_id>` is not a valid cursor identifier.

## Cursor Keep Alive Timeout
Each cursor has a keep alive timer associated with it. When the timer runs out, the cursor is closed by OpenSearch.

This timer is reset every time a page is retrieved.

The client will receive an [error response](#error-response) if it sends a cursor request for an expired cursor. 

## Error Response
The client will receive an error response if any of the above REST calls result in an server-side error.

The response object has the following format:
```json5
{
    "error": {
        "details": <string>,
        "reason": <string>,
        "type": <string>
    },
    "status": <integer>
}
```

`details`, `reason`, and `type` properties are string values. The exact values will depend on the error state encountered.
`status` is an HTTP status code

# Adding pagination to v2 SQL Engine

Implementing pagination in the OpenSearch SQL plugin can be partitioned along several dimensions:
1. SQL query type,
1. Size of queried dataset,
1. SQL node load balancing and failover.

## SQL Query Type
The two main query types are filter-and-project and aggregation queries. 

V1 engine only supports cursors for filter-and-project queries.

Therefore, discussion of pagination in V2 engine will also be limited to filter-and-project queries.

Pagination of aggregation queries can be considered after V1 engine is deprecated.

## OpenSearch Data Retrieval Strategy

OpenSearch provides several data retrival APIs that are optimized for different use cases.

At this time, SQL plugin uses simple search API and scroll API.

Simple retrieval API returns at most `max_result_window` number of documents.  `max_result_window` is an index setting.

Scroll API requests returns all documents but can incur high memory costs on OpenSearch coordination node.

Efficient implementation of pagination needs to be aware of retrival API used. Each retrieval strategy will be considered separately.

The discussion below uses *under max_result_window* to refer to scenarios that can be implemented with simple retrieval API and *over max_result_window* for scenarios that require scroll API to implement.

## SQL Node Load Balancing
V1 SQL engine supports *sql node load balancing* -- a cursor request can be routed to any SQL node in a cluster. This is achieved by encoding all data necessary to retrieve the next page in the `cursor_id`.

This implmentation will use similar approach. 
# Implementation Plan

In all scenarios below, if v2 engine cannot paginate a query, the query will be passed to the v1 engine.

Each phase can be committed separately.

## Phase 1
Goal of this phase is to support pagination in v2 engine for simplest queries -- `SELECT * FROM index` with small datasets but in all scenarios.

1. Under max_result_window, no load balancing.
1. Under max_result_window, with load balancing.

## Phase 2
Goal of this phase is to support getting arbitrarily large datasets, done in two stages:
1. Over max_result_window, no load balancing. 
1. Over max_result_window, with load balancing.


## Phase 3
Support queries with `WHERE` and `ORDER` by clauses.
The focus of this phase will be on sharing query context for the paginated query between all SQL nodes in the cluster.

## Related work

[index-management](https://github.com/opensearch-project/index-management) and [asynchronous-search](https://github.com/opensearch-project/asynchronous-search) plugins are good examples of how to store query-specific context on the plugin side using OpenSearch indices. 

See [IndexManagementIndicies.kt](https://github.com/opensearch-project/index-management/blob/f2a9fa6ef05227f3c048b5e59a5852b18c1e98e1/src/main/kotlin/org/opensearch/indexmanagement/IndexManagementIndices.kt) and [AsynchronnousSearchPersistenceService.java](https://github.com/opensearch-project/asynchronous-search/blob/71c5ddc392ab97fe0a70376b37432155866b268a/src/main/java/org/opensearch/search/asynchronous/service/AsynchronousSearchPersistenceService.java).