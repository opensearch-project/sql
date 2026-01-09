# Unified Query API

This module provides a high-level integration layer for the Calcite-based query engine, enabling external systems such as Apache Spark or command-line tools to parse and analyze queries without exposing low-level internals.

## Overview

This module provides components organized into two main areas aligned with the [Unified Query API architecture](https://github.com/opensearch-project/sql/issues/4782):

### Unified Language Specification

- **`UnifiedQueryPlanner`**: Accepts PPL (Piped Processing Language) queries and returns Calcite `RelNode` logical plans as intermediate representation.
- **`UnifiedQueryTranspiler`**: Converts Calcite logical plans (`RelNode`) into SQL strings for various target databases using different SQL dialects.

### Unified Execution Runtime

- **`UnifiedQueryCompiler`**: Compiles Calcite logical plans (`RelNode`) into executable JDBC `PreparedStatement` objects for separation of compilation and execution.

Together, these components enable complete workflows: parse PPL queries into logical plans, transpile those plans into target database SQL, or compile and execute queries directly for testing and conformance validation.

### Experimental API Design

**This API is currently experimental.** The design intentionally exposes Calcite abstractions (`Schema` for catalogs, `RelNode` as IR, `SqlDialect` for dialects) rather than creating custom wrapper interfaces. This is to avoid overdesign by leveraging the flexible Calcite interface in the short term. If a more abstracted API becomes necessary in the future, breaking changes may be introduced with the new abstraction layer.

## Usage

### UnifiedQueryContext

`UnifiedQueryContext` is a reusable abstraction shared across unified query components (planner, compiler, etc.). It bundles `CalcitePlanContext` and `Settings` into a single object, centralizing configuration for all unified query operations.

Create a context with catalog configuration, query type, and optional settings:

```java
UnifiedQueryContext context = UnifiedQueryContext.builder()
    .language(QueryType.PPL)
    .catalog("opensearch", opensearchSchema)
    .catalog("spark_catalog", sparkSchema)
    .defaultNamespace("opensearch")
    .cacheMetadata(true)
    .setting("plugins.query.size_limit", 200)
    .build();
```

### UnifiedQueryPlanner

Use `UnifiedQueryPlanner` to parse and analyze PPL queries into Calcite logical plans. The planner accepts a `UnifiedQueryContext` and can be reused for multiple queries.

```java
// Create planner with context
UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

// Plan multiple queries (context is reused)
RelNode plan1 = planner.plan("source = logs | where status = 200");
RelNode plan2 = planner.plan("source = metrics | stats avg(cpu)");
```

### UnifiedQueryTranspiler

Use `UnifiedQueryTranspiler` to convert Calcite logical plans into SQL strings for target databases. The transpiler supports various SQL dialects through Calcite's `SqlDialect` interface.

```java
UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
    .dialect(SparkSqlDialect.DEFAULT)
    .build();

String sql = transpiler.toSql(plan);
```

Supported SQL dialects include:
- `SparkSqlDialect.DEFAULT` - Apache Spark SQL
- `PostgresqlSqlDialect.DEFAULT` - PostgreSQL
- `MysqlSqlDialect.DEFAULT` - MySQL
- And other Calcite-supported dialects

### UnifiedQueryCompiler

Use `UnifiedQueryCompiler` to compile Calcite logical plans into executable JDBC statements. This separates compilation from execution and returns standard JDBC types.

```java
UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);

try (PreparedStatement statement = compiler.compile(plan)) {
    ResultSet rs = statement.executeQuery();
    while (rs.next()) {
        // Standard JDBC ResultSet access
    }
}
```

### Complete Workflow Examples

Combining all components for a complete PPL query workflow:

```java
// Step 1: Create reusable context (shared across all components)
try (UnifiedQueryContext context = UnifiedQueryContext.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build()) {

  // Step 2: Create planner with context
  UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

  // Step 3: Plan PPL query into logical plan
  RelNode plan = planner.plan("source = employees | where age > 30");

  // Option A: Transpile to target SQL
  UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
      .dialect(SparkSqlDialect.DEFAULT)
      .build();
  String sparkSql = transpiler.toSql(plan);
  // Result: SELECT * FROM `catalog`.`employees` WHERE `age` > 30

  // Option B: Compile and execute directly
  UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);
  try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
          // Process results with standard JDBC
      }
  }
}
```

## Development & Testing

A set of unit tests is provided to validate planner behavior.

To run tests:

```
./gradlew :api:test
```

## Integration Guide

This guide walks through how to integrate unified query planner into your application.

### Step 1: Add Dependency

The module is currently published as a snapshot to the AWS Sonatype Snapshots repository. To include it as a dependency in your project, add the following to your `pom.xml` or `build.gradle`:

```xml
<dependency>
  <groupId>org.opensearch.query</groupId>
  <artifactId>unified-query-api</artifactId>
  <version>YOUR_VERSION_HERE</version>
</dependency>
```

### Step 2: Implement a Calcite Schema

You must implement the Calcite `Schema` interface and register them using the fluent `catalog()` method on the builder.

```java
public class MySchema extends AbstractSchema {
  @Override
  protected Map<String, Table> getTableMap() {
    return Map.of(
      "test_table",
      new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return typeFactory.createStructType(
            List.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
            List.of("id"));
        }
      });
  }
}
```

## Future Work

- Expand support to SQL language.
- Extend planner to generate optimized physical plans using Calcite's optimization frameworks.
