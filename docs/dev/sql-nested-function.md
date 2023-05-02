## 1 Overview

The nested function in SQL and PPL maps to the nested query DSL in the OpenSearch query engine. A nested query is used to search nested object field types in an index. If an object matches the search, the nested query returns the root parent document. Nested inner objects are returned as inner hits in the query result.

When data is mapped as object type and stored in arrays, the inner objects are stored in a flattened form making it impossible to perform queries with isolation on the inner objects. Users may want to store their data using the `nested` object field type in order to avoid array data being stored in a flattened form and to query individual indexes of an array. Using the nested function with data stored as `nested` object field type allows users to query inner objects with isolation.

### 1.1 Problem Statement

**1. The V2 engine lacks legacy functionality for the nested function** - 
The `nested` function is not present in the V2 engine and must be brought forward from the legacy engine to support user queries for nested object field types in SQL.

**2. SQL provides a better user experience to query data than DSL** - 
The SQL plugin gives users the ability to interact with their data using SQL and PPL query languages rather than the OpenSearch DSL. To query `nested` object type data in SQL and PPL we need an interface that maps to the DSL `nested` queries in OpenSearch.

**3. The V2 engine has no support for the nested function in PPL** - 
The PPL query language is new to the V2 engine in the SQL plugin. Implementation for the nested function in SQL needs to be ported to the PPL query language for users to query their nested object field type data.

### 1.2 Use Cases

**Use case 1:** **OpenSearch Dashboards** - 
Users using OpenSearch Dashboards to query `nested` object field data in SQL and PPL.

- **Non-aggregation query:** The `nested` function can be used to select `nested` object fields or filter nested documents based on field values.
- **Aggregation query:** Users can do bucket aggregation on `nested` fields inside a `nested` object treating inner fields as separate documents.

## 2 Requirements

### 2.1 Functional Requirements
- Support nested function in SQL and PPL query languages.
  - Search query must be pushed down to OpenSearch.
- In SQL the nested function is supported in SELECT, WHERE, GROUP BY, ORDER BY, and HAVING clauses.
- Support JDBC format for nested aggregation queries(Not supported in legacy engine).
- Add support for nested function used as argument to other functions(Not supported in legacy engine).
- Nested function support in PPL.
- Nested function support in JDBC connector.
- Nested function support in ODBC connector.

### 2.2 Non-functional Requirements

### A. Reliability
After a `nested` query is pushed down to OpenSearch, post-processing is done with the response to flatten returned `nested` objects. When multiple array values are returned from `nested` object field types with differing path values, a cross join is executed on the returned rows. Post-processing Operator should be safe-guarded from out of memory error during result flattening. Efficiency should match or improve upon legacy implementation for response flattening.

### B. Extensibility
- The work of the `nested` function aims to be a building block for future work in PartiQL. Users can unnest `nested` collections using PartiQL syntax that follows the underlying logic of the `nested` function with only grammar changes.
- The nested function will require porting over to the PPL query language and a re-usable implementation in SQL should ease this porting.
- Post-processing for nested fields in SQL plugin should be generic to allow any struct or array fields passed in by additional data sources to follow implementation logic.

### 2.3 Tenets
- The `nested` function maps to the OpenSearch nested query in DSL. After extracting inner hits from OpenSearch response the plugin is responsible for in-memory flattening of returned documents.
- If the SQL plugin fails to form a valid `nested` DSL query to push down to OpenSearch the query execution cannot be completed.

### 2.4 Scope
The `nested` function in the legacy engine is supported in multiple clauses in SQL.  A user can use the nested function with the legacy engine in the SELECT, WHERE, GROUP BY, ORDER BY, and HAVING clauses of an SQL statement. The V2 engine has the additional query language PPL to support the nested function. The V2 engine implementation of the nested function likely won’t include all syntax options as the legacy engine to save development time, avoid additional edge cases, and create a more fluid user experience. For example when the `nested` function is used in the WHERE clause, a user can use both of the following syntax options:

- **nested(field | field, path) OPERATOR LITERAL**
- **nested(path, expression)**

Both options serve the same functionality in querying an expression of a `nested` field with a literal. The goal of supporting the nested function in the SQL plugin is to reach functional parity with the legacy engine, port over implementation to the PPL query language, add support in the JDBC and ODBC Connectors, and create a base for future PartiQL development. Any remaining bugs from legacy engine should be resolved and any lack of implementations such as JDBC format for nested aggregation should be implemented.

### 2.5 Out of Scope
- Not all WHERE clause syntax may be included in the V2 grammar TBD.

## Additional Info

### Release Schedule
- **Nested used in `SELECT` clause:** [SELECT Clause Design and Release Schedule](sql-nested-function-select-clause.md)
- **Nested used in `WHERE` clause:** TBD
- **Nested used in `GROUP BY` clause:** TBD
- **Nested used in `HAVING` clause:** TBD
- **Nested used in `ORDER BY` clause:** TBD
- **Nested Added to PPL query language:** TBD
- **Nested function support in JDBC connector:** TBD
- **Nested function support in ODBC connector:** TBD

