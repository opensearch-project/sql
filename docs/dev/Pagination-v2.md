# Pagination in v2 Engine

Pagination allows a SQL plugin client to retrieve arbitrarily large results sets one subset at a time.

A cursor is a SQL abstraction for pagination. A client can open a cursor, retrieve a subset of data given a cursor and close a cursor.

Currently, SQL plugin does not provide SQL cursor syntax. However, the SQL REST endpoint can return result a page at a time. This feature is used by JDBC and ODBC drivers.

# Scope
Currenty, V2 engine supports pagination only for simple `SELECT * FROM <table>` queries without any other clauses like `WHERE` or `ORDER BY`.

# Demo
https://user-images.githubusercontent.com/88679692/224208630-8d38d833-abf8-4035-8d15-d5fb4382deca.mp4

# REST API
## Initial Query Request

Initial query request contains the search request and page size. Search query to OpenSearch is built during processing of this request. Neither the query nor page size can be change while scrolling through pages based on this request.
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

When scrolling is finished, SQL plugin returns a final cursor. This cursor leads to an empty page, which has no cursor and no data hits. Receiving that page means all data was properly queried, and the scrolling cursor has been closed.

The client will receive an [error response](#error-response) if executing this request results in an OpenSearch or SQL plug-in error.

## Cursor Keep Alive Timeout

Each cursor has a keep alive timer associated with it. When the timer runs out, the cursor is automatically closed by OpenSearch.

This timer is reset every time a page is retrieved.

The client will receive an [error response](#error-response) if it sends a cursor request for an expired cursor.

Keep alive timeout is [configurable](../user/admin/settings.rst#plugins.sql.cursor.keep_alive) by setting `plugins.sql.cursor.keep_alive` and has default value of 1 minute.

## Error Response

The client will receive an error response if any of the above REST calls result in an server-side error.

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

OpenSearch provides several data retrival APIs that are optimized for different use cases.

At this time, SQL plugin uses simple search API and scroll API.

Simple retrieval API returns at most `max_result_window` number of documents.  `max_result_window` is an index setting.

Scroll API requests returns all documents but can incur high memory costs on OpenSearch coordination node.

Efficient implementation of pagination needs to be aware of retrival API used. Each retrieval strategy will be considered separately.

The discussion below uses *under max_result_window* to refer to scenarios that can be implemented with simple retrieval API and *over max_result_window* for scenarios that require scroll API to implement.

## SQL Node Load Balancing

V2 SQL engine supports *sql node load balancing* -- a cursor request can be routed to any SQL node in a cluster. This is achieved by encoding all data necessary to retrieve the next page in the `cursor_id`.

## Feature Design

### Plan Tree changes

Different plan trees are built during request processing. Read more about their purpose and stages [here](query-optimizer-improvement.md#Examples). The section below describes the changes being introduced to these trees by the pagination feature.

Simplified workflow of plan trees is shown below. Initial Page Request is processed the same way as a non-paging request.

```mermaid
stateDiagram-v2
  state "Non Paged Request" as NonPaged {
    direction LR
    state "Parse Tree" as Parse
    state "Unresolved Plan Tree" as Unresolved
    state "Abstract Plan Tree" as Abstract
    state "Logical Plan Tree" as Logical
    state "Optimized Logical Plan Tree" as Optimized
    state "Physical Plan Tree" as Physical

    [*] --> Parse : ANTLR
    Parse --> Unresolved : AstBuilder
    Unresolved --> Abstract : QueryPlanner
    Abstract --> Logical : Planner
    Logical --> Optimized : Optimizer
    Optimized --> Physical : Implementor
  }
```
```mermaid
stateDiagram-v2
  state "Initial Page Request" as Paged {
    direction LR
    state "Parse Tree" as Parse
    state "Unresolved Plan Tree" as Unresolved
    state "Abstract Plan Tree" as Abstract
    state "Logical Plan Tree" as Logical
    state "Optimized Logical Plan Tree" as Optimized
    state "Physical Plan Tree" as Physical

    [*] --> Parse : ANTLR
    Parse --> Unresolved : AstBuilder
    Unresolved --> Abstract : QueryPlanner
    Abstract --> Logical : Planner
    Logical --> Optimized : Optimizer
    Optimized --> Physical : Implementor
  }
```
```mermaid
stateDiagram-v2
  state "Subsequent Page Request" as Paged {
    direction LR
    state "Abstract Plan Tree" as Abstract
    state "Physical Plan Tree" as Physical

    [*] --> Abstract : QueryPlanner
    Abstract --> Physical : Deserializer
  }
```

New plan tree workflow was added for Subsequent Page Requests. Since a final Physical Plan tree was already created for Initial request, subsequent requests should have the same tree. The tree is serialized into a `cursor` by `PlanSerializer` to be de-serialized on the subsequence Page Request. Query parsing and analysis is not performed for Subsequent Page Requests, since the Physical Plan tree is instead de-serialized from the `cursor`.

#### Abstract Plan tree

Abstract Plan Tree for non-paged requests remains unchanged. The `QueryPlan`, as a top entry, has a new optional field `pageSize`, which is not defined for non-paged requests.

```mermaid
classDiagram
  direction LR
  class QueryPlan {
    <<AbstractPlan>>
    -Optional~int~ pageSize
    -UnresolvedPlan plan
    -QueryService queryService
  }
  class UnresolvedPlanTree {
    <<UnresolvedPlan>>
  }
  QueryPlan --* UnresolvedPlanTree
```

Abstract plan tree for Initial Query Request has following changes:
1. New Plan node -- `Paginate` -- added into the tree.
2. `pageSize` parameter in `QueryPlan` is set, and `Paginate` is being added. It is converted to `LogicalPaginate` later.

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
  class UnresolvedPlanTree {
    <<UnresolvedPlan>>
  }
  QueryPlan --* Paginate
  Paginate --* UnresolvedPlanTree
```

Non-paging requests have the same plan tree, but `pageSize` value in `QueryPlan` is unset.

Abstract plan tree for Subsequent Query Request (for second and further pages) contains only one node -- `ContinuePaginatedPlan`.

```mermaid
classDiagram
  direction LR
  class ContinuePaginatedPlan {
    <<AbstractPlan>>
    -String cursor
    -PlanSerializer planSerializer
    -QueryService queryService
  }
```

`ContinuePaginatedPlan` translated to entire Physical Plan Tree by `PlanSerializer` on cursor deserialization. It bypasses Logical Plan tree stage, `Planner`, `Optimizer` and `Implementor`.

The examples below show Abstract Plan Tree for the same query in different request types:

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
    ContinuePaginatedPlan
  }
```

#### Logical Plan tree

Changes to plan tree for Initial Query Request with pagination:
1. `LogicalPaginate` is added to the top of the tree. It stores information about paging/scrolling should be done in a private field `pageSize` being pushed down in the `Optimizer`.

```mermaid
classDiagram
  direction LR
  class LogicalPaginate {
    <<LogicalPlan>>
    int pageSize
  }
  class LogicalPlanTree {
    <<LogicalPlan>>
  }
  class LogicalRelation {
    <<LogicalPlan>>
  }
  LogicalPaginate --* LogicalPlanTree
  LogicalPlanTree --* LogicalRelation
```

There are no changes for non-paging requests.

```mermaid
classDiagram
  direction LR
  class LogicalPlanTree {
    <<LogicalPlan>>
  }
  class LogicalRelation {
    <<LogicalPlan>>
  }
  LogicalPlanTree --* LogicalRelation
```

Note: This step is not executed for Subsequent Query Request.

The examples below show Logical Plan Tree for the same query in different request types:

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
```

#### Optimized Logical Plan tree

Changes:
1. For pagination request, a `OpenSearchPagedIndexScanBuilder` is inserted to the bottom of the tree instead of `OpenSearchIndexScanQueryBuilder`. Both are instances of `TableScanBuilder` which extends `LogicalPlan` interface.
2. `LogicalPaginate` is removed from the tree during push down operation in `Optimizer`.

See [article about `TableScanBuilder`](query-optimizer-improvement.md#TableScanBuilder) for more details.

```mermaid
classDiagram
  direction LR
  class LogicalProject {
    <<LogicalPlan>>
  }
  class OpenSearchPagedIndexScanBuilder {
    <<TableScanBuilder>>
  }

  LogicalProject --* OpenSearchPagedIndexScanBuilder
```

Note: No Logical Plan tree created for Subsequent Query Request.

The examples below show optimized Logical Plan Tree for the same query in different request types:

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

  state "Initial Paged Request" as Paged {
    state "LogicalProject" as ProjectIP
    state "LogicalLimit" as LimitIP
    state "LogicalSort" as SortIP
    state "OpenSearchPagedIndexScanBuilder" as RelationIP

    ProjectIP --> LimitIP
    LimitIP --> SortIP
    SortIP --> RelationIP
  }
```

#### Physical Plan tree

Changes:
1. `OpenSearchPagedIndexScanBuilder` is converted to `OpenSearchPagedIndexScan` by `Implementor`.
2. Entire Physical Plan tree is created by `PlanSerializer` for Subsequent Query requests. The deserialized tree has the same structure as the Initial Query Request.

```mermaid
classDiagram
  direction LR
  class ProjectOperator {
    <<PhysicalPlan>>
  }
  class OpenSearchPagedIndexScan {
    <<TableScanOperator>>
  }

  ProjectOperator --* OpenSearchPagedIndexScan
```

The examples below show Physical Plan Tree for the same query in different request types:

```mermaid
stateDiagram-v2
  state "Non Paged Request" as NonPaged {
    state "ProjectOperator" as ProjectNP
    state "LimitOperator" as LimitNP
    state "SortOperator" as SortNP
    state "OpenSearchIndexScan" as RelationNP

    ProjectNP --> LimitNP
    LimitNP --> SortNP
    SortNP --> RelationNP
  }

  state "Initial Query Request" as Paged {
    state "ProjectOperator" as ProjectIP
    state "LimitOperator" as LimitIP
    state "SortOperator" as SortIP
    state "OpenSearchPagedIndexScan" as RelationIP

    ProjectIP --> LimitIP
    LimitIP --> SortIP
    SortIP --> RelationIP
  }

  state "Subsequent Query Request" As Sub {
    state "ProjectOperator" as ProjectSP
    state "LimitOperator" as LimitSP
    state "SortOperator" as SortSP
    state "OpenSearchPagedIndexScan" as RelationSP

    ProjectSP --> LimitSP
    LimitSP --> SortSP
    SortSP --> RelationSP
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
      Planner -->>- QueryService: Logical Plan Tree
    QueryService ->>+ OpenSearchExecutionEngine: execute
      OpenSearchExecutionEngine -->>- QueryService: execution completed
    QueryService -->>- QueryPlanFactory: execution completed
  QueryPlanFactory -->>- SQLService: execution completed
```

#### Initial Query Request

Processing of an Initial Query Request has few extra steps comparing versus processing a regular Query Request:
1. Query validation with `CanPaginateVisitor`. This is required to validate whether incoming query can be paged. This also activate legacy engine fallback mechanism.
2. Creating a paged index scan with `CreatePagingTableScanBuilder` `Optimizer` rule. A Regular Query Request triggers `CreateTableScanBuilder` rule instead.
3. `Serialization` is performed by `PlanSerializer` - it converts Physical Plan Tree into a cursor, which could be used query a next page.
4. Traversal of Physical Plan Tree to get total hits, which is required to properly fill response to a user.

```mermaid
sequenceDiagram
    participant SQLService
    participant QueryPlanFactory
    participant CanPaginateVisitor
    participant QueryService
    participant Planner
    participant CreatePagingTableScanBuilder
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
      Planner ->>+ CreatePagingTableScanBuilder : apply
        CreatePagingTableScanBuilder -->>- Planner : paged index scan
      end
      Planner -->>- QueryService : Logical Plan Tree
    QueryService ->>+ OpenSearchExecutionEngine : execute
      rect rgb(91, 123, 155)
      Note over OpenSearchExecutionEngine, PlanSerializer : Serialization
      OpenSearchExecutionEngine ->>+ PlanSerializer : convertToCursor
        PlanSerializer -->>- OpenSearchExecutionEngine : cursor
      end
      rect rgb(91, 123, 155)
      Note over OpenSearchExecutionEngine : get total hits
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
4. Traversal of Physical Plan Tree to get total hits, which is required to properly fill response to a user.

```mermaid
sequenceDiagram
    participant SQLService
    participant QueryPlanFactory
    participant QueryService
    participant OpenSearchExecutionEngine
    participant PlanSerializer

SQLService ->>+ QueryPlanFactory : execute
  QueryPlanFactory ->>+ QueryService : execute
    rect rgb(91, 123, 155)
    note over QueryService, PlanSerializer : Deserialization
    QueryService ->>+ PlanSerializer: convertToPlan
      PlanSerializer -->>- QueryService: Physical plan tree
    end
    Note over QueryService : Planner, Optimizer and Implementor<br />are skipped
    QueryService ->>+ OpenSearchExecutionEngine : execute
      rect rgb(91, 123, 155)
      note over OpenSearchExecutionEngine, PlanSerializer : Serialization
      OpenSearchExecutionEngine ->>+ PlanSerializer : convertToCursor
        PlanSerializer -->>- OpenSearchExecutionEngine : cursor
      end
      rect rgb(91, 123, 155)
      Note over OpenSearchExecutionEngine : get total hits
      end
      OpenSearchExecutionEngine -->>- QueryService: execution completed
    QueryService -->>- QueryPlanFactory : execution completed
  QueryPlanFactory -->>- SQLService : execution completed
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

The SQL engine should be able to completely recover the Physical Plan tree to continue its execution to get the next page. Serialization mechanism is responsible for recovering the plan tree. note: `ResourceMonitorPlan` isn't serialized, because a new object of this type would be created for the restored plan tree before execution. 
Serialization and Deserialization are performed by Java object serialization API.

```mermaid
stateDiagram-v2
    direction LR
    state "Initial Query Request Plan Tree" as FirstPage
    state FirstPage {
        state "ProjectOperator" as logState1_1
        state "..." as logState1_2
        state "ResourceMonitorPlan" as logState1_3
        state "OpenSearchPagedIndexScan" as logState1_4
        state "OpenSearchScrollRequest" as logState1_5
        logState1_1 --> logState1_2
        logState1_2 --> logState1_3
        logState1_3 --> logState1_4
        logState1_4 --> logState1_5
    }

    state "Deserialized Plan Tree" as SecondPageTree
    state SecondPageTree {
        state "ProjectOperator" as logState2_1
        state "..." as logState2_2
        state "OpenSearchPagedIndexScan" as logState2_3
        state "ContinuePageRequest" as logState2_4
        logState2_1 --> logState2_2
        logState2_2 --> logState2_3
        logState2_3 --> logState2_4
    }

    state "Subsequent Query Request Plan Tree" as SecondPage
    state SecondPage {
        state "ProjectOperator" as logState3_1
        state "..." as logState3_2
        state "ResourceMonitorPlan" as logState3_3
        state "OpenSearchPagedIndexScan" as logState3_4
        state "ContinuePageRequest" as logState3_5
        logState3_1 --> logState3_2
        logState3_2 --> logState3_3
        logState3_3 --> logState3_4
        logState3_4 --> logState3_5
    }

  FirstPage --> SecondPageTree : Serialization and\nDeserialization
  SecondPageTree --> SecondPage : Execution\nPreparation
```

#### Serialization

All plan tree nodes which are supported by pagination should implement [`SerializablePlan`](https://github.com/opensearch-project/sql/blob/f40bb6d68241e76728737d88026e4c8b1e6b3b8b/core/src/main/java/org/opensearch/sql/planner/SerializablePlan.java) interface. `getPlanForSerialization` method of this interface allows serialization mechanism to skip a tree node from serialization. OpenSearch search request objects are not serialized, but search context provided by the OpenSearch cluster is extracted from them.

```mermaid
sequenceDiagram
    participant PlanSerializer
    participant ProjectOperator
    participant ResourceMonitorPlan
    participant OpenSearchPagedIndexScan
    participant OpenSearchScrollRequest
    participant ContinuePageRequest

PlanSerializer ->>+ ProjectOperator : getPlanForSerialization
  ProjectOperator -->>- PlanSerializer : this
PlanSerializer ->>+ ProjectOperator : serialize
  Note over ProjectOperator : dump private fields
  ProjectOperator ->>+ ResourceMonitorPlan : getPlanForSerialization
    ResourceMonitorPlan -->>- ProjectOperator : delegate
  Note over ResourceMonitorPlan : ResourceMonitorPlan<br />is not serialized
  ProjectOperator ->>+ OpenSearchPagedIndexScan : serialize
    alt First page
      OpenSearchPagedIndexScan ->>+ OpenSearchScrollRequest : toCursor
        OpenSearchScrollRequest -->>- OpenSearchPagedIndexScan : serialized request
    else Subsequent page
      OpenSearchPagedIndexScan ->>+ ContinuePageRequest : toCursor
        ContinuePageRequest -->>- OpenSearchPagedIndexScan : serialized request
    end
    Note over OpenSearchPagedIndexScan : dump private fields
    OpenSearchPagedIndexScan -->>- ProjectOperator : serialized
  ProjectOperator -->>- PlanSerializer : serialized
Note over PlanSerializer : Zip to reduce size
```

#### Deserialization

Deserialization restores previously serialized Physical Plan tree. The recovered tree is ready to execute and should return the next page of the search response. To complete the tree restoration, SQL engine should build a new request to the OpenSearch node. This request doesn't contain a search query, but it contains a search context reference -- `scrollID`. To create a new `ContinuePageRequest` object it is require to access to the instance of `OpenSearchStorageEngine`. `OpenSearchStorageEngine` can't be serialized and it exists as a singleton in the SQL plugin engine. `PlanSerializer` creates a customized deserialization binary object stream -- `CursorDeserializationStream`. This stream provides an interface to access the `OpenSearchStorageEngine` object.

```mermaid
sequenceDiagram
    participant PlanSerializer
    participant CursorDeserializationStream
    participant ProjectOperator
    participant OpenSearchPagedIndexScan
    participant ContinuePageRequest

Note over PlanSerializer : Unzip
Note over PlanSerializer : Validate cursor integrity
PlanSerializer ->>+ CursorDeserializationStream : deserialize
  CursorDeserializationStream ->>+ ProjectOperator : create new
    Note over ProjectOperator: load private fields
    ProjectOperator -->> CursorDeserializationStream : deserialize input
  activate CursorDeserializationStream
  CursorDeserializationStream ->>+ OpenSearchPagedIndexScan : create new
  deactivate CursorDeserializationStream
    OpenSearchPagedIndexScan -->>+ CursorDeserializationStream : resolve engine
  CursorDeserializationStream ->>- OpenSearchPagedIndexScan : OpenSearchStorageEngine
    Note over OpenSearchPagedIndexScan : load private fields
    OpenSearchPagedIndexScan ->>+ ContinuePageRequest : create new
      ContinuePageRequest -->>- OpenSearchPagedIndexScan : created
    OpenSearchPagedIndexScan -->>- ProjectOperator : deserialized
  ProjectOperator -->>- PlanSerializer : deserialized
  deactivate CursorDeserializationStream
```

#### Total Hits

Total Hits is the number of rows matching the search criteria; with `select *` queries it is equal to row (doc) number in the table (index).
Example:
Paging thru `SELECT * FROM calcs` (17 rows) with `fetch_size = 5` returns:

* Page 1: total hits = 17, result size = 5, cursor
* Page 2: total hits = 17, result size = 5, cursor
* Page 3: total hits = 17, result size = 5, cursor
* Page 4: total hits = 17, result size = 2, cursor
* Page 5: total hits = 0, result size = 0

Default implementation of `getTotalHits` in a Physical Plan iterate child plans down the tree and gets the maximum value or 0.

```mermaid
sequenceDiagram
    participant OpenSearchExecutionEngine
    participant ProjectOperator
    participant ResourceMonitorPlan
    participant OpenSearchPagedIndexScan

OpenSearchExecutionEngine ->>+ ProjectOperator: getTotalHits
  Note over ProjectOperator: default implementation
  ProjectOperator ->>+ ResourceMonitorPlan: getTotalHits
    Note over ResourceMonitorPlan: call to delegate
    ResourceMonitorPlan ->>+ OpenSearchPagedIndexScan: getTotalHits
      Note over OpenSearchPagedIndexScan: use stored value from the search response
      OpenSearchPagedIndexScan -->>- ResourceMonitorPlan: value
    ResourceMonitorPlan -->>- ProjectOperator: value
  ProjectOperator -->>- OpenSearchExecutionEngine: value
```
