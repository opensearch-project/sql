
# OpenSearch SQL/PPL Development Documentation

## Introduction

+ [Architecture](Architecture.md): a quick overview of architecture
+ Concepts
+ Quickstart

---
## Programming Guides

+ API
+ SDK

---
## Development Guides

### Language Processing

+ SQL
  + [Aggregate Window Function](AggregateWindowFunction.md): provides window function support
+ PPL

### Query Processing

+ Query Analyzing
  + [Semantic Analysis](SemanticAnalysis.md): performs semantic analysis to ensure semantic correctness
  + [Type Conversion](TypeConversion.md): implement implicit data type conversion
+ Query Planning
+ Query Execution
+ Query Acceleration

### Data Sources

+ OpenSearch
    + [Sub Queries](SubQuery.md): sub queries push down in OpenSearch
    + [Pagination](Pagination.md): pagination implementation by OpenSearch scroll API
+ Prometheus
+ Object Store

---
## Other Documentation

+ Test Framework
  + [Doc Test](Doctest.md): makes our doc live and runnable to ensure documentation correctness
  + [Comparison Test](Testing.md): compares with other databases to ensure functional correctness
+ Operation Tools