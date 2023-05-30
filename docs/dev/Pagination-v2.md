# Pagination in v2 Engine

Pagination allows a SQL plugin client to retrieve arbitrarily large results sets one subset at a time.

A cursor is a SQL abstraction for pagination. A client can open a cursor, retrieve a subset of data given a cursor and close a cursor.

Currently, SQL plugin does not provide SQL cursor syntax. However, the SQL REST endpoint can return result a page at a time. This feature is used by JDBC and ODBC drivers.

# Scope
This document describes pagination in V2 sql engine for non-aggregate queries -- queries 
without `GROUP BY` clause or use of window functions.

# Demo
https://user-images.githubusercontent.com/88679692/224208630-8d38d833-abf8-4035-8d15-d5fb4382deca.mp4

# REST API
## Initial Query Request

Initial query request contains the search request and page size. Search query to OpenSearch is built during processing of this request. Neither the query nor page size can be changed while scrolling through pages based on this request.
The only difference between paged and non-paged requests is `fetch_size` parameter supplied in paged request.

```json
POST /_plugins/_sql
{
    "query" : "...",
    "fetch_size": N
}
```

Response:
```json
{
  "cursor": "<cursor_id>",
  "datarows": [
    ...
  ],
  "schema" : [
    ...
  ]
}
```
`query` is a DQL statement. `fetch_size` is a positive integer, indicating number of rows to return in each page.

If `query` is a DML statement then pagination does not apply, the `fetch_size` parameter is ignored and a cursor is not created. This is existing behaviour in v1 engine.

The client receives an [error response](#error-response) if:
- `fetch_size` is not a positive integer
-  evaluating `query` results in a server-side error
-  `fetch_size` is bigger than `max_window_size` cluster-wide parameter.

## Subsequent Query Request

Subsequent query request contains a cursor only.

```json
POST /_plugins/_sql
{
  "cursor": "<cursor_id>"
}
```
Similarly to v1 engine, the response object is the same as initial response if this is not the last page.

`cursor_id` will be different with each request.

## End of scrolling/paging
The last page in a response will not have a cursor id property.

## Cursor Keep Alive Timeout

Each cursor has a keep alive timer associated with it. When the timer runs out, the cursor is automatically closed by OpenSearch.

This timer is reset every time a page is retrieved.

The client will receive an [error response](#error-response) if it sends a cursor request for an expired cursor.

Keep alive timeout is [configurable](../user/admin/settings.rst#plugins.sql.cursor.keep_alive) by setting `plugins.sql.cursor.keep_alive` and has default value of 1 minute.

## Error Response

The client will receive an error response if any of the above REST calls result in a server-side error.

The response object has the following format:
```json
{
    "error": {
        "details": "<string>",
        "reason": "<string>",
        "type": "<string>"
    },
    "status": <integer>
}
```

`details`, `reason`, and `type` properties are string values. The exact values will depend on the error state encountered.
`status` is an HTTP status code

## OpenSearch Data Retrieval Strategy

OpenSearch provides several data retrieval APIs that are optimized for different use cases.

At this time, SQL plugin uses simple search API and scroll API.

Simple retrieval API returns at most `max_result_window` number of documents.  `max_result_window` is an index setting.

Scroll API requests returns all documents but can incur high memory costs on OpenSearch coordination node.

Efficient implementation of pagination needs to be aware of retrieval API used. Each retrieval strategy will be considered separately.

The discussion below uses *under max_result_window* to refer to scenarios that can be implemented with simple retrieval API and *over max_result_window* for scenarios that require scroll API to implement.

## SQL Node Load Balancing

V2 SQL engine supports *sql node load balancing* &mdash; a cursor request can be routed to any SQL node in a cluster. This is achieved by encoding all data necessary to retrieve the next page in the `cursor_id` property in the response.

## Feature Design
To support pagination, v2 SQL engine needs to:
1. in REST front-end:
    1. Route supported paginated query to v2 engine for
        1. Initial requests,
        2. Next page requests.
    2. Fallback to v1 engine for queries not supported by v2 engine.
    3. Create correct JSON response from execution of paginated physical plan by v2 engine.
2. during query planning:
    1. Differentiate between paginated and normal query plans.
    2. Push down pagination to table scan.
    3. Create a physical query plan from a cursor id.
3. during query execution:
   1. Serialize an executing query and generate a cursor id after returning `fetch_size` number of elements.
4. in OpenSearch data source: 
    1. Support pagination push down.
    2. Support other push down optimizations with pagination.

### Query Plan Changes

All three kinds of query requests &mdash; non-paged, initial page, or subsequent page  &mdash; are processed in the same way. Simplified workflow of query plan processing is shown below for reference.

```mermaid
stateDiagram-v2
  state "Request" as NonPaged {
    direction LR
    state "Parse Tree" as Parse
    state "Unresolved Query Plan" as Unresolved
    state "Abstract Query Plan" as Abstract
    state "Logical Query Plan" as Logical
    state "Optimized Query Plan" as Optimized
    state "Physical Query Plan" as Physical

    [*] --> Parse : ANTLR
    Parse --> Unresolved : AstBuilder
    Unresolved --> Abstract : QueryPlanner
    Abstract --> Logical : Planner
    Logical --> Optimized : Optimizer
    Optimized --> Physical : Implementor
  }
```


#### Unresolved Query Plan

Unresolved Query Plan for non-paged requests remains unchanged. 

To support initial query requests, the `QueryPlan` class has a new optional field `pageSize`.

```mermaid
classDiagram
  direction LR
  class QueryPlan {
    <<AbstractPlan>>
    -Optional~int~ pageSize
    -UnresolvedPlan plan
    -QueryService queryService
  }
  class UnresolvedQueryPlan {
    <<UnresolvedPlan>>
  }
  QueryPlan --* UnresolvedQueryPlan
```

When `QueryPlanFactory.create` is passed initial query request, it:
1. Adds an instance of `Paginate` unresolved plan as the root of the unresolved query plan.
2. Sets `pageSize` parameter in `QueryPlan`.

```mermaid
classDiagram
  direction LR
  class QueryPlan {
    <<AbstractPlan>>
    -Optional~int~ pageSize
    -UnresolvedPlan plan
    -QueryService queryService
  }
  class Paginate {
    <<UnresolvedPlan>>
    -int pageSize
    -UnresolvedPlan child
  }
  class UnresolvedQueryPlan {
    <<UnresolvedPlan>>
  }
  QueryPlan --* Paginate
  Paginate --* UnresolvedQueryPlan
```

When `QueryPlanFactory.create` is passed a subsequent query request, it:
1. Creates an instance of `FetchCursor` unresolved plan as the sole node in the unresolved query plan.

```mermaid
classDiagram
    direction LR
    class QueryPlan {
        <<AbstractPlan>>
        -Optional~int~ pageSize
        -UnresolvedPlan plan
        -QueryService queryService
    }
    class FetchCursor {
        <<UnresolvedPlan>>
        -String cursorId
    }
    QueryPlan --* FetchCursor
```

The examples below show Abstract Query Plan for the same query in different request types:

```mermaid
stateDiagram-v2
  state "Non Paged Request" as NonPaged {
    state "QueryPlan" as QueryPlanNP
    state "Project" as ProjectNP
    state "Limit" as LimitNP
    state "Filter" as FilterNP
    state "Aggregation" as AggregationNP
    state "Relation" as RelationNP

    QueryPlanNP --> ProjectNP
    ProjectNP --> LimitNP
    LimitNP --> FilterNP
    FilterNP --> AggregationNP
    AggregationNP --> RelationNP
  }

  state "Initial Query Request" as Paged {
    state "QueryPlan" as QueryPlanIP
    state "Project" as ProjectIP
    state "Limit" as LimitIP
    state "Filter" as FilterIP
    state "Aggregation" as AggregationIP
    state "Relation" as RelationIP

    Paginate --> QueryPlanIP
    QueryPlanIP --> ProjectIP
    ProjectIP --> LimitIP
    LimitIP --> FilterIP
    FilterIP --> AggregationIP
    AggregationIP --> RelationIP
  }

  state "Subsequent Query Request" As Sub {
    FetchCursor
  }
```

#### Logical Query Plan

There are no changes for non-paging requests.

Changes to logical query plan to support Initial Query Request:
1. `LogicalPaginate` is added to the top of the tree. It stores information about paging should be done in a private field `pageSize` being pushed down in the `Optimizer`.

```mermaid
classDiagram
  direction LR
  class LogicalPaginate {
    <<LogicalPlan>>
    int pageSize
  }
  class LogicalQueryPlan {
    <<LogicalPlan>>
  }
  class LogicalRelation {
    <<LogicalPlan>>
  }
  LogicalPaginate --* LogicalQueryPlan
  LogicalQueryPlan --* LogicalRelation
```

For subsequent page requests, `FetchCursor` unresolved plan is mapped to `LogicalFetchCursor` logical plan.

```mermaid
classDiagram
  direction LR
  class LogicalQueryPlan {
    <<LogicalPlan>>
  }
  class LogicalFetchCursor {
    <<LogicalPlan>>
    -String cursorId
  }
  LogicalQueryPlan --* LogicalFetchCursor
```

The examples below show logical query plan for the same query in different request types:

```mermaid
stateDiagram-v2
  state "Non Paged Request" as NonPaged {
    state "LogicalProject" as ProjectNP
    state "LogicalLimit" as LimitNP
    state "LogicalFilter" as FilterNP
    state "LogicalAggregation" as AggregationNP
    state "LogicalRelation" as RelationNP

    ProjectNP --> LimitNP
    LimitNP --> FilterNP
    FilterNP --> AggregationNP
    AggregationNP --> RelationNP
  }

  state "Initial Query Request" as Paged {
    state "LogicalProject" as ProjectIP
    state "LogicalLimit" as LimitIP
    state "LogicalFilter" as FilterIP
    state "LogicalAggregation" as AggregationIP
    state "LogicalRelation" as RelationIP

    LogicalPaginate --> ProjectIP
    ProjectIP --> LimitIP
    LimitIP --> FilterIP
    FilterIP --> AggregationIP
    AggregationIP --> RelationIP
  }

  state "Subsequent Query Request" As Sub {
    FetchCursor
  }
```


#### Optimized Logical Query Plan

Pagination is implemented by push down to OpenSearch. The following is only relevant for
initial paged requests. Non-paged request optimization was not changed and there is no optimization
to be done for subsequent page query plans.

Push down logical is implemented in  `OpenSearchIndexScanQueryBuilder.pushDownPageSize` method.
This method is called by `PushDownPageSize` rule during plan optimization.  `LogicalPaginate` is removed from the query plan during push down operation in `Optimizer`.

See [article about `TableScanBuilder`](query-optimizer-improvement.md#TableScanBuilder) for more details.

The examples below show optimized Logical Query Plan for the same query in different request types:

```mermaid
stateDiagram-v2
  state "Non Paged Request" as NonPaged {
    state "LogicalProject" as ProjectNP
    state "LogicalLimit" as LimitNP
    state "LogicalSort" as SortNP
    state "OpenSearchIndexScanQueryBuilder" as RelationNP

    ProjectNP --> LimitNP
    LimitNP --> SortNP
    SortNP --> RelationNP
  }

```

#### Physical Query Plan and Execution

Changes:
1. `OpenSearchIndexScanBuilder` is converted to `OpenSearchIndexScan` by `Implementor`.
2. `LogicalPlan.pageSize` is mapped to `OpenSearchIndexScan.maxResponseSize`. This is the limit to  the number of elements in a response.
2. Entire Physical Query Plan is created by `PlanSerializer` for Subsequent Query requests. The deserialized plan has the same structure as the Initial Query Request.
3. Implemented serialization and deserialization for `OpenSearchScrollRequest`.


The examples below show physical query plan for the same query in different request types:

```mermaid
stateDiagram-v2
  state "Non Paged Request" as NonPaged {
    state "ProjectOperator" as ProjectNP
    state "LimitOperator" as LimitNP
    state "SortOperator" as SortNP
    state "OpenSearchIndexScan" as RelationNP
    state "OpenSearchQueryRequest" as QRequestNP

    ProjectNP --> LimitNP
    LimitNP --> SortNP
    SortNP --> RelationNP
    RelationNP --> QRequestNP
  }

  state "Initial Query Request" as Paged {
    state "ProjectOperator" as ProjectIP
    state "LimitOperator" as LimitIP
    state "SortOperator" as SortIP
    state "OpenSearchIndexScan" as RelationIP
    state "OpenSearchQueryRequest" as QRequestIP

    ProjectIP --> LimitIP
    LimitIP --> SortIP
    SortIP --> RelationIP
    RelationIP --> QRequestIP
  }

  state "Subsequent Query Request" As Sub {
    state "ProjectOperator" as ProjectSP
    state "LimitOperator" as LimitSP
    state "SortOperator" as SortSP
    state "OpenSearchIndexScan" as RelationSP
    state "OpenSearchScrollRequest" as RequestSP

    ProjectSP --> LimitSP
    LimitSP --> SortSP
    SortSP --> RelationSP
    RelationSP --> RequestSP
  }
```

### Architecture Diagrams

New code workflows which added by Pagination feature are highlighted.

#### Non Paging Query Request

A non-paging request sequence diagram is shown below for comparison: 

```mermaid
sequenceDiagram
    participant SQLService
    participant QueryPlanFactory
    participant QueryService
    participant Planner
    participant CreateTableScanBuilder
    participant OpenSearchExecutionEngine

SQLService ->>+ QueryPlanFactory: execute
  QueryPlanFactory ->>+ QueryService: execute
    QueryService ->>+ Planner: optimize
      Planner ->>+ CreateTableScanBuilder: apply
        CreateTableScanBuilder -->>- Planner: index scan
      Planner -->>- QueryService: Logical Query Plan
    QueryService ->>+ OpenSearchExecutionEngine: execute
      OpenSearchExecutionEngine -->>- QueryService: execution completed
    QueryService -->>- QueryPlanFactory: execution completed
  QueryPlanFactory -->>- SQLService: execution completed
```

#### Initial Query Request

Processing of an Initial Query Request has few extra steps comparing versus processing a regular Query Request:
1. Query validation with `CanPaginateVisitor`. This is required to validate whether incoming query can be paged. This also activate legacy engine fallback mechanism.
2. `Serialization` is performed by `PlanSerializer` - it converts Physical Plan Tree into a cursor, which could be used query a next page.

```mermaid
sequenceDiagram
    participant SQLService
    participant QueryPlanFactory
    participant CanPaginateVisitor
    participant QueryService
    participant Planner
    participant CreatePagingScanBuilder
    participant OpenSearchExecutionEngine
    participant PlanSerializer

SQLService ->>+ QueryPlanFactory : execute
  rect rgb(91, 123, 155)
  QueryPlanFactory ->>+ CanPaginateVisitor : canConvertToCursor
    CanPaginateVisitor -->>- QueryPlanFactory : true
  end
  QueryPlanFactory ->>+ QueryService : execute
    QueryService ->>+ Planner : optimize
      rect rgb(91, 123, 155)
      Planner ->>+ CreateTableScanBuilder : apply
        CreateTableScanBuilder -->>- Planner : paged index scan
      end
      Planner -->>- QueryService : Logical Query Plan
    QueryService ->>+ OpenSearchExecutionEngine : execute
      rect rgb(91, 123, 155)
      Note over OpenSearchExecutionEngine, PlanSerializer : Serialization
      OpenSearchExecutionEngine ->>+ PlanSerializer : convertToCursor
        PlanSerializer -->>- OpenSearchExecutionEngine : cursor
      end
      OpenSearchExecutionEngine -->>- QueryService : execution completed
    QueryService -->>- QueryPlanFactory : execution completed
  QueryPlanFactory -->>- SQLService : execution completed
```

#### Subsequent Query Request

Subsequent pages are processed by a new workflow. The key point there:
1. `Deserialization` is performed by `PlanSerializer` to restore entire Physical Plan Tree encoded into the cursor.
2. Since query already contains the Physical Plan Tree, all tree processing steps are skipped.
3. `Serialization` is performed by `PlanSerializer` - it converts Physical Plan Tree into a cursor, which could be used query a next page.

```mermaid
sequenceDiagram
    participant QueryPlanFactory
    participant QueryService
    participant Analyzer
    participant Planner
    participant DefaultImplementor
    participant PlanSerializer
    participant OpenSearchExecutionEngine

QueryPlanFactory ->>+ QueryService : execute
  QueryService ->>+ Analyzer : analyze
    Analyzer -->>- QueryService : new LogicalFetchCursor
  QueryService ->>+ Planner : plan
    Planner ->>+ DefaultImplementor : implement
      rect rgb(91, 123, 155)
      DefaultImplementor ->>+ PlanSerializer : deserialize
        PlanSerializer -->>- DefaultImplementor: physical query plan
      end
      DefaultImplementor -->>- Planner : physical query plan
    Planner -->>- QueryService : physical query plan
  QueryService ->>+ OpenSearchExecutionEngine : execute
    OpenSearchExecutionEngine -->>- QueryService: execution completed
  QueryService -->>- QueryPlanFactory : execution completed
```

#### Legacy Engine Fallback

Since pagination in V2 engine supports fewer SQL commands than pagination in legacy engine, a fallback mechanism is created to keep V1 engine features still available for the end user. Pagination fallback is backed by a new exception type which allows legacy engine to intersect execution of a request.

```mermaid
sequenceDiagram
    participant RestSQLQueryAction
    participant Legacy Engine
    participant SQLService
    participant QueryPlanFactory
    participant CanPaginateVisitor

RestSQLQueryAction ->>+ SQLService : prepareRequest
  SQLService ->>+ QueryPlanFactory : execute
    rect rgb(91, 123, 155)
    note over SQLService, CanPaginateVisitor : V2 support check
    QueryPlanFactory ->>+ CanPaginateVisitor : canConvertToCursor
      CanPaginateVisitor -->>- QueryPlanFactory : false
    QueryPlanFactory -->>- RestSQLQueryAction : UnsupportedCursorRequestException
    deactivate SQLService
    end
      RestSQLQueryAction ->> Legacy Engine: accept
      Note over Legacy Engine : Processing in Legacy engine
        Legacy Engine -->> RestSQLQueryAction : complete
```

#### Serialization and Deserialization round trip

The SQL engine should be able to completely recover the Physical Query Plan to continue its execution to get the next page. Serialization mechanism is responsible for recovering the query plan. note: `ResourceMonitorPlan` isn't serialized, because a new object of this type would be created for the restored query plan before execution. 
Serialization and Deserialization are performed by Java object serialization API.

```mermaid
stateDiagram-v2
    direction LR
    state "Initial Query Request Query Plan" as FirstPage
    state FirstPage {
        state "ProjectOperator" as logState1_1
        state "..." as logState1_2
        state "ResourceMonitorPlan" as logState1_3
        state "OpenSearchIndexScan" as logState1_4
        state "OpenSearchScrollRequest" as logState1_5
        logState1_1 --> logState1_2
        logState1_2 --> logState1_3
        logState1_3 --> logState1_4
        logState1_4 --> logState1_5
    }

    state "Deserialized Query Plan" as SecondPageTree
    state SecondPageTree {
        state "ProjectOperator" as logState2_1
        state "..." as logState2_2
        state "OpenSearchIndexScan" as logState2_3
        state "OpenSearchScrollRequest" as logState2_4
        logState2_1 --> logState2_2
        logState2_2 --> logState2_3
        logState2_3 --> logState2_4
    }

    state "Subsequent Query Request Query Plan" as SecondPage
    state SecondPage {
        state "ProjectOperator" as logState3_1
        state "..." as logState3_2
        state "ResourceMonitorPlan" as logState3_3
        state "OpenSearchIndexScan" as logState3_4
        state "OpenSearchScrollRequest" as logState3_5
        logState3_1 --> logState3_2
        logState3_2 --> logState3_3
        logState3_3 --> logState3_4
        logState3_4 --> logState3_5
    }

  FirstPage --> SecondPageTree : Serialization and\nDeserialization
  SecondPageTree --> SecondPage : Execution\nPreparation
```

#### Serialization

All query plan nodes which are supported by pagination should implement [`SerializablePlan`](https://github.com/opensearch-project/sql/blob/f40bb6d68241e76728737d88026e4c8b1e6b3b8b/core/src/main/java/org/opensearch/sql/planner/SerializablePlan.java) interface. `getPlanForSerialization` method of this interface allows serialization mechanism to skip a tree node from serialization. OpenSearch search request objects are not serialized, but search context provided by the OpenSearch cluster is extracted from them.

```mermaid
sequenceDiagram
    participant PlanSerializer
    participant ProjectOperator
    participant ResourceMonitorPlan
    participant OpenSearchIndexScan
    participant OpenSearchScrollRequest

PlanSerializer ->>+ ProjectOperator : getPlanForSerialization
  ProjectOperator -->>- PlanSerializer : this
PlanSerializer ->>+ ProjectOperator : serialize
  Note over ProjectOperator : dump private fields
  ProjectOperator ->>+ ResourceMonitorPlan : getPlanForSerialization
    ResourceMonitorPlan -->>- ProjectOperator : delegate
  Note over ResourceMonitorPlan : ResourceMonitorPlan<br />is not serialized
  ProjectOperator ->>+ OpenSearchIndexScan : writeExternal
    OpenSearchIndexScan ->>+ OpenSearchScrollRequest : writeTo
      Note over OpenSearchScrollRequest : dump private fields
      OpenSearchScrollRequest -->>- OpenSearchIndexScan : serialized request
    Note over OpenSearchIndexScan : dump private fields
    OpenSearchIndexScan -->>- ProjectOperator : serialized
  ProjectOperator -->>- PlanSerializer : serialized
Note over PlanSerializer : Zip to reduce size
```

#### Deserialization

Deserialization restores previously serialized Physical Query Plan. The recovered plan is ready to execute and returns the next page of the search response. To complete the query plan restoration, SQL engine will build a new request to the OpenSearch node. This request doesn't contain a search query, but it contains a search context reference &mdash; `scrollID`. To create a new `OpenSearchScrollRequest` object it requires access to the instance of `OpenSearchStorageEngine`. Note: `OpenSearchStorageEngine` can't be serialized, and it exists as a singleton in the SQL plugin engine. `PlanSerializer` creates a customized deserialization binary object stream &mdash; `CursorDeserializationStream`. This stream provides an interface to access the `OpenSearchStorageEngine` object.

```mermaid
sequenceDiagram
    participant PlanSerializer
    participant CursorDeserializationStream
    participant ProjectOperator
    participant OpenSearchIndexScan
    participant OpenSearchScrollRequest

Note over PlanSerializer : Unzip
Note over PlanSerializer : Validate cursor integrity
PlanSerializer ->>+ CursorDeserializationStream : deserialize
  CursorDeserializationStream ->>+ ProjectOperator : create new
    Note over ProjectOperator: load private fields
    ProjectOperator -->> CursorDeserializationStream : deserialize input
  activate CursorDeserializationStream
  CursorDeserializationStream ->>+ OpenSearchIndexScan : create new
  deactivate CursorDeserializationStream
    OpenSearchIndexScan -->>+ CursorDeserializationStream : resolve engine
  CursorDeserializationStream ->>- OpenSearchIndexScan : OpenSearchStorageEngine
    Note over OpenSearchIndexScan : load private fields
    OpenSearchIndexScan ->>+ OpenSearchScrollRequest : create new
      OpenSearchScrollRequest -->>- OpenSearchIndexScan : created
    OpenSearchIndexScan -->>- ProjectOperator : deserialized
  ProjectOperator -->>- PlanSerializer : deserialized
  deactivate CursorDeserializationStream
```

#### Close Cursor

A user can forcibly close a cursor (scroll) at any moment of paging. Automatic close occurs when paging is complete and no more results left.
Close cursor protocol defined by following:
1. REST endpoint: `/_plugins/_sql/close`
2. Request type: `POST`
3. Request format:
```json
{
    "cursor" : "<cursor>"
}
```
4. Response format:
```json
{
    "succeeded": true
}
```
5. Failure or error: [error response](#error-response)
6. Use or sequential close of already closed cursor produces the same error as use of expired/auto-closed/non-existing cursor.

```mermaid
sequenceDiagram
SQLService ->>+ QueryPlanFactory : execute
  QueryPlanFactory ->>+ QueryService : execute
  QueryService ->>+ Analyzer : analyze
  Analyzer -->>- QueryService : new LogicalCloseCursor
  QueryService ->>+ Planner : plan
  Planner ->>+ DefaultImplementor : implement
  DefaultImplementor ->>+ PlanSerializer : deserialize
  PlanSerializer -->>- DefaultImplementor: physical query plan
  DefaultImplementor -->>- Planner : new CloseOperator
  Planner -->>- QueryService : CloseOperator
  QueryService ->>+ OpenSearchExecutionEngine : execute
  Note over OpenSearchExecutionEngine : Open is no-op, no request issued,<br />no results received and processed
  Note over OpenSearchExecutionEngine : Clean-up (clear scroll) on auto-close
  OpenSearchExecutionEngine -->>- QueryService: execution completed
  QueryService -->>- QueryPlanFactory : execution completed
  QueryPlanFactory -->>- SQLService : execution completed
```

```mermaid
stateDiagram-v2
    direction LR
    state "Abstract Query Plan" as Abstract {
      state "CommandPlan" as CommandPlan {
        state "Unresolved Query Plan" as Unresolved {
          state "CloseCursor" as CloseCursor
          state "FetchCursor" as FetchCursor

          CloseCursor --> FetchCursor
        }
      }
    }
    state "Logical Query Plan" as Logical {
      state "LogicalCloseCursor" as LogicalCloseCursor
      state "LogicalFetchCursor" as LogicalFetchCursor

      LogicalCloseCursor --> LogicalFetchCursor
    }
    state "Optimized Query Plan" as Optimized {
      state "LogicalCloseCursor" as LogicalCloseCursorO
      state "LogicalFetchCursor" as LogicalFetchCursorO

      LogicalCloseCursorO --> LogicalFetchCursorO
    }
    state "Physical Query Plan" as Physical {
      state "CursorCloseOperator" as CursorCloseOperator
      state "ProjectOperator" as ProjectOperator
      state "..." as ...
      state "OpenSearchIndexScan" as OpenSearchIndexScan

      CursorCloseOperator --> ProjectOperator
      ProjectOperator --> ...
      ... --> OpenSearchIndexScan
    }

    [*] --> Unresolved : QueryPlanner
    Unresolved --> Logical : Planner
    Logical --> Optimized : Optimizer
    Optimized --> Physical : Implementor
```

`CursorCloseOperator` provides a dummy (empty, since not used) `Schema`, does not perform `open` and always returns `false` by `hasNext`. Such behavior makes it a no-op operator which blocks underlying Physical Plan Tree from issuing any search request, but does not block auto-close provided by `AutoCloseable`. Default close action clears scroll context.
Regular paging doesn't execute scroll clear, because it checks whether paging is finished or not and raises a flag to prevent clear. This check performed when search response recevied, what never happen due to `CursorCloseOperator`.

```py
class OpenSearchScrollRequest:
  bool needClean = true

  def search:
    ...
    needClean = response.isEmpty()

  def clean:
    if needClean:
      clearScroll()
```

```py
class CursorCloseOperator(PhysicalPlan):
  PhysicalPlan tree
  def open:
    pass
    # no-op, don't propagate `open` of underlying plan tree

  def hasNext:
    return false
```

```py
class PhysicalPlan:
  def open:
    innerPlan.open()

  def close:
    innerPlan.close()
```
