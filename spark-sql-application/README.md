# Spark SQL Application

This application execute sql query and store the result in OpenSearch index in following format
```
"stepId":"<emr-step-id>",
"applicationId":"<spark-application-id>"
"schema": "json blob",
"result": "json blob"
```

## Prerequisites

+ Spark 3.3.1
+ Scala 2.12.15
+ flint-spark-integration

## Usage

To use this application, you can run Spark with Flint extension:

```
./bin/spark-submit \
    --class org.opensearch.sql.SQLJob \
    --jars <flint-spark-integration-jar> \
    sql-job.jar \
    <spark-sql-query> \
    <opensearch-index> \
    <opensearch-host> \
    <opensearch-port> \
    <opensearch-scheme> \
    <opensearch-auth> \
    <opensearch-region> \
```

## Result Specifications

Following example shows how the result is written to OpenSearch index after query execution.

Let's assume sql query result is
```
+------+------+
|Letter|Number|
+------+------+
|A     |1     |
|B     |2     |
|C     |3     |
+------+------+
```
OpenSearch index document will look like
```json
{
  "_index" : ".query_execution_result",
  "_id" : "A2WOsYgBMUoqCqlDJHrn",
  "_score" : 1.0,
  "_source" : {
    "result" : [
    "{'Letter':'A','Number':1}",
    "{'Letter':'B','Number':2}",
    "{'Letter':'C','Number':3}"
    ],
    "schema" : [
      "{'column_name':'Letter','data_type':'string'}",
      "{'column_name':'Number','data_type':'integer'}"
    ],
    "stepId" : "s-JZSB1139WIVU",
    "applicationId" : "application_1687726870985_0003"
  }
}
```

## Build

To build and run this application with Spark, you can run:

```
sbt clean publishLocal
```

## Test

To run tests, you can use:

```
sbt test
```

## Scalastyle

To check code with scalastyle, you can run:

```
sbt scalastyle
```

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](../CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

See the [LICENSE](../LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](../NOTICE) for details.