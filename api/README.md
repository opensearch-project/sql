# Unified Query API

This module provides a high-level integration layer for the Calcite-based query engine, enabling external systems such as Apache Spark or command-line tools to parse and analyze queries without exposing low-level internals.

## Overview

The `UnifiedQueryPlanner` serves as the primary entry point for external consumers. It accepts PPL (Piped Processing Language) queries and returns Calcite `RelNode` logical plans as intermediate representation.

## Usage

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
