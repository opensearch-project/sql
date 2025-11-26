# Unified Query API

This module provides a high-level integration layer for the Calcite-based query engine, enabling external systems such as Apache Spark or command-line tools to parse and analyze queries without exposing low-level internals.

## Overview

This module provides two primary components:

- **`UnifiedQueryPlanner`**: Accepts PPL (Piped Processing Language) queries and returns Calcite `RelNode` logical plans as intermediate representation.
- **`UnifiedQueryTranspiler`**: Converts Calcite logical plans (`RelNode`) into SQL strings for various target databases using different SQL dialects.

Together, these components enable a complete workflow: parse PPL queries into logical plans, then transpile those plans into target database SQL.

## Usage

### UnifiedQueryPlanner

Use the declarative, fluent builder API to initialize the `UnifiedQueryPlanner`.

```java
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("opensearch", schema)
    .defaultNamespace("opensearch")
    .cacheMetadata(true)
    .build();

RelNode plan = planner.plan("source = opensearch.test");
```

### UnifiedQueryTranspiler

Use `UnifiedQueryTranspiler` to convert Calcite logical plans into SQL strings for target databases. The transpiler supports various SQL dialects through Calcite's `SqlDialect` interface.

```java
UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
    .dialect(SparkSqlDialect.DEFAULT)
    .build();

String sql = transpiler.toSql(plan);
```

### Complete Workflow Example

Combining both components to transpile PPL queries into target database SQL:

```java
// Step 1: Initialize planner
UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
    .language(QueryType.PPL)
    .catalog("catalog", schema)
    .defaultNamespace("catalog")
    .build();

// Step 2: Parse PPL query into logical plan
RelNode plan = planner.plan("source = employees | where age > 30");

// Step 3: Initialize transpiler with target dialect
UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
    .dialect(SparkSqlDialect.DEFAULT)
    .build();

// Step 4: Transpile to target SQL
String sparkSql = transpiler.toSql(plan);
// Result: SELECT * FROM `catalog`.`employees` WHERE `age` > 30
```

Supported SQL dialects include:
- `SparkSqlDialect.DEFAULT` - Apache Spark SQL
- `PostgresqlSqlDialect.DEFAULT` - PostgreSQL
- `MysqlSqlDialect.DEFAULT` - MySQL
- And other Calcite-supported dialects

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
