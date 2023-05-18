# Flint Index Reference Manual

## Overview

### What is Flint Index?

A Flint index is ...

![Overview](https://user-images.githubusercontent.com/46505291/235786891-556cfde2-189c-4f65-b24f-c36e9c59a96a.png)

### Feature Highlights

- Skipping Index
  - Partition index
  - MinMax index
  - ValueList index
  - BloomFilter index
- Covering Index

### Flint Index Specification

#### Metadata

```json
{
  "version": "0.1",
  "indexConfig": {
    "kind": "SkippingIndex",
    "properties": {
      "indexedColumns": ...
     }
  },
  "source": "alb_logs",
  "state": "active",
  "enabled": true
}
```

#### Field Data Type

For now, Flint uses OpenSearch field type directly.

#### File Format

Please see Index Store section for more details.

## User Guide

### SDK

`FlintClient` provides low-level Flint index management and data access API.

Index management API example:

```java
// Initialized Flint client for a specific storage
FlintClient flintClient = new FlintOpenSearchClient("localhost", 9200);

FlintMetadata metadata = new FlintMetadata(...)
flintClient.createIndex("alb_logs_skipping_index", metadata)

flintClient.getIndexMetadata("alb_logs_skipping_index")
```

Index data read and write example:

```java
TODO
```

### API

High level API is dependent on query engine implementation. Please see Query Engine Integration section for details.

### SQL

DDL statement:

```sql
CREATE SKIPPING INDEX
ON <object>
FOR COLUMNS ( column <index_type> [, ...] )
WHERE <filter_predicate>

DESCRIBE SKIPPING INDEX ON <object>

DROP SKIPPING INDEX ON <object>

<object> ::= [db_name].[schema_name].table_name
```

Skipping index type:

```sql
<index_type> ::= { <bloom_filter>, <min_max>, <value_list> }

<bloom_filter> ::= BLOOM_FILTER( bitCount, numOfHashFunctions ) #TBD
<min_max> ::= MIN_MAX
<value_list> ::= VALUE_LIST
```

Example:

```sql
CREATE SKIPPING INDEX ON alb_logs
FOR COLUMNS (
  client_ip BLOOM_FILTER,
  elb_status VALUE_LIST
)
WHERE time > '2023-04-01 00:00:00'

DESCRIBE SKIPPING INDEX ON alb_logs

DROP SKIPPING INDEX ON alb_logs
```

## Index Store

### OpenSearch

OpenSearch stores the Flint index in an OpenSearch index of the given name.
In the index mapping, the `_meta` and `properties`field stores meta and schema info of a Flint index.

```json
alb_logs_skipping_index
{
  "_meta": {
    "version": "0.1",
    "indexConfig": {
        "kind": "SkippingIndex",
        "properties": {
          "indexedColumns": [
            "elb_status_code": "value_list"
          ]
        }
    }
    ......
  },
  "properties": {
     "file_path": {
       "type": "keyword"
     },
     "elb_status_code": {
         "type": "integer"
     }
   }
}
```

## Query Engine Integration

### Apache Spark

#### Configurations

- `spark.flint.indexstore.location`: default is localhost
- `spark.flint.indexstore.port`: default is 9200

#### API

Here is an example for Flint Spark integration:

```scala
val flint = new FlintSpark(spark)
val index =
  new FlintSkippingIndex(
    tableName="alb_logs",
    indexedColumns=[
      BloomFilter("client_ip"),
      ValueList("elb_status")],
    filterPred="time > 2023-04-01 00:00:00")
flint.createIndex(index)
```

## Benchmarks

TODO