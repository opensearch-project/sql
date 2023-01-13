
# OpenSearch SQL/PPL Development Documentation

## Introduction

+ [Architecture](intro-architecture.md): a quick overview of architecture
+ [V2 Engine](intro-v2-engine.md): introduces why we developed new V2 engine
+ Concepts
+ Quickstart

---
## Programming Guides

+ Client
+ API
+ SDK

---
## Development Guides

### Language Processing

+ SQL
  + [Aggregate Window Function](sql-aggregate-window-function.md): provides window function support
+ PPL

### Query Processing

+ Query Analyzing
  + [Semantic Analysis](query-semantic-analysis.md): performs semantic analysis to ensure semantic correctness
  + [Type Conversion](query-type-conversion.md): implement implicit data type conversion
+ Query Planning
+ Query Execution
+ Query Acceleration

### Data Sources

+ OpenSearch
    + [Sub Queries](opensearch-nested-field-subquery.md): support sub queries on OpenSearch nested field
    + [Pagination](opensearch-pagination.md): pagination implementation by OpenSearch scroll API
+ Prometheus
+ Object Store

---
## Other Documentation

+ Test Framework
  + [Doc Test](testing-doctest.md): makes our doc live and runnable to ensure documentation correctness
  + [Comparison Test](testing-comparison-test.md): compares with other databases to ensure functional correctness
+ Operation Tools