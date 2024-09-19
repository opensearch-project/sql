# async-query-core library

This directory contains async-query-core library, which implements the core logic of async-query and provide extension points to allow plugin different implementation of data storage, etc.
`async-query` module provides implementations for OpenSearch index based implementation.

## Type of queries
There are following types of queries, and the type is automatically identified by analysing the query. 
- BatchQuery: Execute single query in Spark
- InteractiveQuery: Establish session and execute queries in single Spark session
- IndexDMLQuery: Handles DROP/ALTER/VACUUM operation for Flint indices
- RefreshQuery: One time query request to refresh(update) Flint index
- StreamingQuery: Continuously update flint index in single Spark session

## Extension points
Following is the list of extension points where the consumer of the library needs to provide their own implementation.

- Data store interface
  - [AsyncQueryJobMetadataStorageService](src/main/java/org/opensearch/sql/spark/asyncquery/AsyncQueryJobMetadataStorageService.java)
  - [SessionStorageService](java/org/opensearch/sql/spark/execution/statestore/SessionStorageService.java)
  - [StatementStorageService](src/main/java/org/opensearch/sql/spark/execution/statestore/StatementStorageService.java)
  - [FlintIndexMetadataService](src/main/java/org/opensearch/sql/spark/flint/FlintIndexMetadataService.java)
  - [FlintIndexStateModelService](src/main/java/org/opensearch/sql/spark/flint/FlintIndexStateModelService.java)
  - [IndexDMLResultStorageService](src/main/java/org/opensearch/sql/spark/flint/IndexDMLResultStorageService.java)
- Other
  - [LeaseManager](src/main/java/org/opensearch/sql/spark/leasemanager/LeaseManager.java)
  - [JobExecutionResponseReader](src/main/java/org/opensearch/sql/spark/response/JobExecutionResponseReader.java)
  - [QueryIdProvider](src/main/java/org/opensearch/sql/spark/dispatcher/QueryIdProvider.java)
  - [SessionIdProvider](src/main/java/org/opensearch/sql/spark/execution/session/SessionIdProvider.java)
  - [SessionConfigSupplier](src/main/java/org/opensearch/sql/spark/execution/session/SessionConfigSupplier.java)
  - [EMRServerlessClientFactory](src/main/java/org/opensearch/sql/spark/client/EMRServerlessClientFactory.java)
  - [SparkExecutionEngineConfigSupplier](src/main/java/org/opensearch/sql/spark/config/SparkExecutionEngineConfigSupplier.java)
  - [DataSourceSparkParameterComposer](src/main/java/org/opensearch/sql/spark/parameter/DataSourceSparkParameterComposer.java)
  - [GeneralSparkParameterComposer](src/main/java/org/opensearch/sql/spark/parameter/GeneralSparkParameterComposer.java)
  - [SparkSubmitParameterModifier](src/main/java/org/opensearch/sql/spark/config/SparkSubmitParameterModifier.java) To be deprecated in favor of GeneralSparkParameterComposer

## Update Grammar files
This package uses ANTLR grammar files from `opensearch-spark` and `Spark` repositories.
To update the grammar files, update `build.gradle` file (in `downloadG4Files` task) as needed and run:
```
./gradlew async-query-core:downloadG4Files
```
