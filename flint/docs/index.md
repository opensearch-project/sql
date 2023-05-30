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
- Materialized View

### Flint Index Specification

#### Metadata

Currently, Flint metadata is only static configuration without version control and write-ahead log.

```json
{
  "version": "0.1",
  "indexConfig": {
    "kind": "skipping",
    "properties": {
      "indexedColumns": [{
        "kind": "...",
        "columnName": "...",
        "columnType": "..."
      }]
     }
  },
  "source": "alb_logs",
  "state": "active",
  "enabled": true
}
```

#### Field Data Type

For now, Flint Index doesn't define its own data type and uses OpenSearch field type instead.

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
FlintClient flintClient = new FlintOpenSearchClient("localhost", 9200);

// read example
FlintReader reader = flintClient.createReader("indexName", null)\
while(reader.hasNext) {
  reader.next()
}
reader.close()

// write example
FlintWriter writer = flintClient.createWriter("indexName")
writer.write("{\"create\":{}}")
writer.write("\n")
writer.write("{\"aInt\":1}")
writer.write("\n")
writer.flush()
writer.close()

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
{
  "_meta": {
    "version": "0.1",
    "indexConfig": {
        "kind": "skipping",
        "properties": {
          "indexedColumns": [
            {
              "kind": "partition",
              "columnName": "year",
              "columnType": "int"
            },
            {
              "kind": "value_list",
              "columnName": "elb_status_code",
              "columnType": "int"
            }
          ]
        }
    },
    "source": "alb_logs"
  },
  "properties": {
    "year": {
      "type": "integer"
    },
    "elb_status_code": {
      "type": "integer"
    },
    "file_path": {
      "type": "keyword"
    }
  }
}
```

## Query Engine Integration

### Apache Spark

#### Configurations

- `spark.datasource.flint.location`: default is localhost.
- `spark.datasource.flint.port`: default is 9200.
- `spark.datasource.flint.scheme`: default is http. valid values [http, https]
- `spark.datasource.flint.auth`: default is false. valid values [false, sigv4]
- `spark.datasource.flint.region`: default is us-west-2. only been used when auth=sigv4
- `spark.datasource.flint.write.id_name`: no default value.
- `spark.datasource.flint.write.batch_size`: default value is 1000.
- `spark.datasource.flint.write.refresh_policy`: default value is false. valid values [NONE(false), 
  IMMEDIATE(true), WAIT_UNTIL(wait_for)]
- `spark.datasource.flint.read.scroll_size`: default value is 100.

#### API

Here is an example for Flint Spark integration:

```scala
val flint = new FlintSpark(spark)

flint.skippingIndex()
    .onTable("alb_logs")
    .filterBy("time > 2023-04-01 00:00:00")
    .addPartitionIndex("year", "month", "day")
    .addValueListIndex("elb_status_code")
    .addBloomFilterIndex("client_ip")
    .create()

flint.refresh("flint_alb_logs_skipping_index", FULL)
```

#### Skipping Index Provider SPI

```scala
trait FlintSparkSkippingStrategy {
  TODO: outputSchema, getAggregators, rewritePredicate
}
```

#### Flint DataSource Read/Write

Here is an example for read index data from AWS OpenSearch domain.

```scala
val aos = Map(
  "host" -> "yourdomain.us-west-2.es.amazonaws.com", 
  "port" -> "-1", 
  "scheme" -> "https", 
  "auth" -> "sigv4", 
  "region" -> "us-west-2")

val df = new SQLContext(sc).read
        .format("flint")
        .options(aos)
        .schema("aInt int")
        .load("t001")
```

## Benchmarks

TODO
