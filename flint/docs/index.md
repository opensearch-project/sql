# Flint Index Reference Manual

## Overview

### What is Flint Index?

A Flint index is ...

![Overview](https://user-images.githubusercontent.com/46505291/235786891-556cfde2-189c-4f65-b24f-c36e9c59a96a.png)

### Feature Highlights

- Skipping Index
  - Partition index
  - MinMax index
  - ValueSet index
  - BloomFilter index
- Covering Index
- Materialized View

| Skipping Index | Create Index Statement (TBD)                                                                                                               | Index Building Logic                                                                                                                                                                                                                                                            | Query Rewrite Logic                                                                                                                                                                                                                                                                                                               |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Partition      | CREATE SKIPPING INDEX ON alb_logs<br>FOR COLUMNS (<br>  year PARTITION,<br>  month PARTITION,<br>  day PARTITION,<br>  hour PARTITION<br>) | INSERT INTO flint_alb_logs_skipping_index<br>SELECT<br>  FIRST(year) AS year,<br>  FIRST(month) AS month, FIRST(day) AS day,<br>  FIRST(hour) AS hour,<br>  input_file_name() AS file_path<br>FROM alb_logs<br>GROUP BY<br>  input_file_name()                                  | SELECT *<br>FROM alb_logs<br>WHERE year = 2023<br><br>=><br><br>SELECT *<br>FROM alb_logs (input_files = <br>  SELECT file_path<br>  FROM flint_alb_logs_skipping_index<br>  WHERE year = 2023 AND month = 4<br>)<br>WHERE year = 2023 AND month = 4                                                                              |
| ValueSet       | CREATE SKIPPING INDEX ON alb_logs<br>FOR COLUMNS (<br>  elb_status_code VALUE_LIST<br>)                                                    | INSERT INTO flint_alb_logs_skipping_index<br>SELECT<br>  COLLECT_SET(elb_status_code) AS elb_status_code<br>  input_file_name() AS file_path<br>FROM alb_logs<br>GROUP BY<br>  input_file_name()                                                                                | SELECT *<br>FROM alb_logs<br>WHERE elb_status_code = 404<br><br>=><br><br>SELECT *<br>FROM alb_logs (input_files = <br>  SELECT file_path<br>  FROM flint_alb_logs_skipping_index<br>  WHERE ARRAY_CONTAINS(elb_status_code, 404)<br>)<br>WHERE elb_status_code = 404                                                             |
| Min-Max        | CREATE SKIPPING INDEX ON alb_logs<br>FOR COLUMNS (<br>  request_processing_time MIN_MAX<br>)                                               | INSERT INTO flint_alb_logs_skipping_index<br>SELECT<br>  MIN(request_processing_time) AS request_processing_time_min,<br>  MAX(request_processing_time) AS request_processing_time_max,<br>  input_file_name() AS file_path<br>FROM alb_logs<br>GROUP BY<br>  input_file_name() | SELECT *<br>FROM alb_logs<br>WHERE request_processing_time = 100<br><br>=><br><br>SELECT *<br>FROM alb_logs (input_files = <br>  SELECT file_path<br>  FROM flint_alb_logs_skipping_index<br>  WHERE request_processing_time_min <= 100<br>    AND 100 <= request_processing_time_max<br>)<br>WHERE request_processing_time = 100 |

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
<index_type> ::= { <bloom_filter>, <min_max>, <value_set> }

<bloom_filter> ::= BLOOM_FILTER( bitCount, numOfHashFunctions ) #TBD
<min_max> ::= MIN_MAX
<value_set> ::= VALUE_SET
```

Example:

```sql
CREATE SKIPPING INDEX ON alb_logs
FOR COLUMNS (
  client_ip BLOOM_FILTER,
  elb_status_code VALUE_SET
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
              "kind": "Partition",
              "columnName": "year",
              "columnType": "int"
            },
            {
              "kind": "ValuesSet",
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
- `spark.flint.optimizer.enabled`: default is true.

#### API

Here is an example for Flint Spark integration:

```scala
val flint = new FlintSpark(spark)

flint.skippingIndex()
    .onTable("alb_logs")
    .filterBy("time > 2023-04-01 00:00:00")
    .addPartitions("year", "month", "day")
    .addValueSet("elb_status_code")
    .addBloomFilter("client_ip")
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
