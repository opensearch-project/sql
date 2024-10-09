# Async Query Interface API Documentation

The Async Query Interface API enables the execution of queries on top of a Spark engine. This is specifically designed to work with the `S3Glue` data source connector and allows queries to be run asynchronously.

The Async Query Interface is especially useful for running complex, time-consuming queries without blocking the main thread. The API provides endpoints for query creation, result retrieval, and query cancellation.

---

## Table of Contents
1. [Configurations](#configurations)
2. [Async Query Creation](#async-query-creation)
3. [Query Result Retrieval](#query-result-retrieval)
4. [Query Cancellation](#query-cancellation)
5. [API Mock Tests](#api-mock-tests)

---

## Configurations

Before using the Async Query APIs, the Spark Execution Engine Config for Async Queries must be set under `plugins.query.executionengine.spark.config` in the cluster settings. For more details on this configuration, refer to the introductory section of this document.

---

## Async Query Creation

To create an async query, you can use the `POST /_plugins/_async_query` endpoint.

**Examples**

- Query Creation Sample Request
```
  POST _plugins/_async_query
  {"query":""CREATE EXTERNAL TABLE mys3.default.http_logs (
           `@timestamp` TIMESTAMP,
            clientip STRING,
            request STRING, 
            status INT, 
            size INT, 
            year INT, 
            month INT, 
            day INT) 
        USING json PARTITIONED BY(year, month, day) OPTIONS (path 's3://flint-data-dp-eu-west-1-beta/data/http_log/http_logs_partitioned_json_bz2/', compression 'bzip2')""
    ,"datasource":"mys3","lang":"sql"}
```


## Query Result Retrieval

To fetch the result of a previously created async query, use the `GET /_plugins/_async_query/{queryId}` endpoint.

**Examples**

- Query Result Sample Request
```
POST _plugins/_async_query
    {"query":"SELECT * FROM mys3.default.aws_elb","datasource":"mys3","lang":"sql"}
```

---

## Query Cancellation

To cancel a running or queued query, use the `DELETE /_plugins/_async_query/{queryId}` endpoint.

**Examples**

- Query Cancellation Sample Request

```
DELETE _plugins/_async_query/VjlvWTNBbE1Lem15czM=
```

---

_This API is designed to work seamlessly with AWS EMRServerless as the Spark execution engine._

---

## API Mock Tests

The [Swagger YAML file](swagger.yaml) describes the API. You can visualize the REST API using any of the numerous [online editors](https://editor.swagger.io/).

### Setup Mock Server

To test the API, you can use a Swagger-based mock library like Stoplight Prism.

#### Running the Swagger Mock Server

```bash
npm install -g @stoplight/prism-cli
```

Once installed, run the server using:

```bash
prism mock swagger.yaml
```

#### Running the Swagger Mock Server as Docker

Run the following one-liner Docker command:

```bash
docker run -p 9200:4010 -v "$(pwd)/docs/API:/api" --name integ-prism -t stoplight/prism mock -h 0.0.0.0 /api/swagger.yaml
```

After the server has started, you can initiate a CURL request to test any of the API endpoints.

![Swagger API Screenshot](../../img/swagger-api.png)
