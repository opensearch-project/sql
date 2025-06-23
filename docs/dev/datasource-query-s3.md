## 1.Overview

In this document, we will propose a solution in OpenSearch Observability to query log data stored in S3.

### 1.1.Problem Statements

Currently, OpenSearch Observability is collection of plugins and applications that let you visualize data-driven events by using Piped Processing Language to explore, discover, and query data stored in OpenSearch. The major requirements we heard from customer are

* **cost**, regarding to hardware cost of setup an OpenSearch cluster.
* **ingestion performance,** it is not easy to supporting high throughput raw log ingestion.
* **flexibility,** OpenSearch index required user know their query pattern before ingest data. Which is not flexible.

Can build a new solution for OpenSearch observably uses and leverage S3 as storage. The benefits are

* **cost efficiency**, comparing with OpenSearch, S3 is cheap.
* **high ingestion throughput**, S3 is by design to support high throughput write.
* **flexibility**, user do not need to worry about index mapping define and reindex. everything could be define at query tier.
* **scalability**, user do not need to optimize their OpenSearch cluster for write. S3 is auto scale.
* **data durability**, S3 provide 11s 9 of data durability.

With all these benefits, are there any concerns? The **ability to query S3 in OpenSearch and query performance** are the major concerns. In this doc, we will provide the solution to solve these two major concerns.

## 2.Terminology

* **Catalog**. OpenSearch access external data source through catalog. For example, S3 catalog. Each catalog has attributes, most important attributes is data source access credentials.
* **Table**: To access external data source. User should create external table to describe the schema and location. Table is the virtual concept which does not mapping to OpenSearch index.
* **Materialized View**: User could create view from existing tables. Each view is 1:1 mapping to OpenSearch index. There are two types of views
    * (1) Permanent view (default) which is fully managed by user.
    * (2) Transient View which is maintained by Maximus automatically. user could decide when to drop the transient view.

## 3.Requirements

### 3.1.Use Cases

#### Use Case - 1: pre-build and query metrics from log on s3

**_Create_**

* User could ingest the log or events directly to s3 with existing ingestion tools (e.g. [fluentbit](https://docs.fluentbit.io/manual/pipeline/outputs/s3)). The ingested log should be partitioned. e.g. _s3://my-raw-httplog/region/yyyy/mm/dd_.
* User configure s3 Catalog in OpenSearch. By default, s3 connector use [ProfileCredentialsProvider](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/profile/ProfileCredentialsProvider.html).

```
settings.s3.access_key: xxxxx
settings.s3.secret_key: xxxxx
```

* User create table in OpenSearch of their log data on s3. Maximus will create table httplog in the s3 catalog

```
CREATE EXTERNAL TABLE `s3`.`httplog`(
   @timestamp timestamp,
   clientip string,
   request string,
   state integer,
   size long
   )
ROW FORMAT SERDE
   'json'
   'grok', <timestamp> <className>
PARTITION BY
   ${region}/${year}/${month}/${day}/
LOCATION
   's3://my-raw-httplog/';
```

* User create the view *failEventsMetrics*. Note: User could only create view from schema-defined table in Maximus

```
CREATE MATERIALIZED VIEW failEventsMetrics (
  cnt    long,
  time   timestamp,
  status string
)
AS source=`s3`.`httpLog` status != 200 | status count() as cnt by span(5mins), status
WITH (
    REFRESH=AUTO
)
```

* Maximus will (1) create view *failEventsMetrics* in the default catalog. (2) create index *failEventsMetrics* in the OpenSearch cluster. (3) refresh *failEventsMetrics* index with logs on s3*.* _Notes: Create view return success when operation (1) and (2) success. Refresh view is an async task will be executed in background asynchronously._
* User could describe the view to monitor the status.

```
DESCRIBE/SHOW MATERIALIZED VIEW failEventsMetrics

# Return
status: INIT | IN_PROGRESS | READY
```

_**Query**_

* User could query the view.

```
source=failEventsMetrics time in last 7 days
```

* User could query the table, Thunder will rewrite table query as view query when optimizing the query.

```
source=`s3`.`httpLog` status != 200 | status count() as cnt by span(1hour), status
```

_**Drop**_


* User could drop the table. Maximus will delete httpLog metadata from catalog and associated view drop.

```
DROP TABLE `s3`.`httpLog`
```

* User could drop the view. Maximus will delete failEventsMetrics metadata from catalog and delete failEventsMetrics indices.

```
DROP MATERIALIZED VIEW failEventsMetrics
```

**Access Control**

* User could not configure any permission on table.
* User could not directly configure any permission on view. But user could configure permission on index associated with the view.
  ![image1](https://user-images.githubusercontent.com/2969395/182239505-a8541ec1-f46f-4b91-882a-8a4be36f5aea.png)

#### Use Case - 2: Ad-hoc s3 query in OpenSearch

**_Create_**

* Similar as previous example, User could ingest the log or events directly to s3 with existing ingestion tools. create catalog and table from data on s3.

_**Query**_

* User could query table without create view. At runtime, Maximus will create the transient view and populate the view with required data.

```
source=`s3`.`httpLog` status != 200 | status count() as cnt by span(5mins), status
```

_**List**_

* User could list all the view of a table no matter how the view is created. User could query/drop/describe the temp view create by Maximus, as same as user created view.

```
LIST VIEW on `s3`.`httpLog`

# return
httplog-tempView-xxxx
```

**Access Control**

* User could not configure any permission on table. During query time, Maximus will use the credentials to access s3 data. It is user’s ownership to configure the permission on s3 data.
* User could not directly configure any permission on view. But user could configure permission on index associated with the view.
  ![image2](https://user-images.githubusercontent.com/2969395/182239672-72b2cfc6-c22e-4279-b33e-67a85ee6a778.png)

### 3.2.Function Requirements

#### Query S3

* User could query time series data on S3 by using PPL in OpenSearch.
* For querying time series data in S3, user must create a **Catalog** for S3 with required setting.
    * access key and secret key
    * endpoint
* For querying time series data in S3, user must create a **Table** of time series data on S3.

#### View

* Support create materialized view from time series data on S3.
* Support fully materialized view refresh
* Support manually materialized view incrementally refresh
* Support automatically materialized view incrementally refresh
* Support hybrid scan
* Support drop materialized view
* Support show materialized view

#### Query Optimization

* Inject optimizer rule to rewrite the query with MV to avoid S3 scan

#### Automatic query acceleration

* Automatically select view candidate based on OpenSearch - S3 query execution metrics
* Store workload and selected view info for visualization and user intervention
* Automatically create/refresh/drop view.

#### S3 data format

* The time series data could be compressed with gzip.
* The time series data file format could be.

    * JSON
    * TSV

* If the time series data should be partitioned and have snapshot id. Query engine could support automatically incremental refresh and hybrid scan.

#### Resource Management

* Support circuit breaker based resource control when executing a query.
* Support task based resource manager

#### Fault Tolerant

* For fast query process, we scarify Fault Tolerant. Support query fast failure in case of hardware failure.

#### Setting

* Support configure of x% of disk automatically create view should used.

### 3.3.Non Function Requirements

#### Security:

There are three types of privileges that are related to materialized views

* Privileges directly on the materialized view itself
* Privileges on the objects (e.g. table) that the materialized view accesses.

*_materialized view itself_*

* User could not directly configure any access control on view. But user could configure any access control on index associated with the view. In the other words, materialized inherits all the access control on the index associated with view.
* When **automatically** **refresh view**, Maximus will use the backend role. It required the backend role has permission to index data.

*_objects (e.g. table) that the materialized view accesses_*

* As with non-materialized views, a user who wishes to access a materialized view needs privileges only on the view, not on the underlying object(s) that the view references.

*_table_*

* User could not configure any privileges on table. *_Note: because the table query could be rewrite as view query If the user do not have required permission to access the view. User could still get no permission exception._*

*_objects (e.g. table) that the table refer_*

* The s3 access control is only evaluated during s3 access. if the table access is rewrite as view access. The s3 access control will not take effect.

*_Encryption_*

* Materialized view data will be encrypted at rest.

#### Others

* Easy to use: the solution should be designed easy to use. It should just work out of the box and provide good performance with minimal setup.
* **Performance**: Use **http_log** dataset to benchmark with OpenSearch cluster.
* Scalability: The solution should be scale horizontally.
* **Serverless**: The solution should be designed easily deployed as Serverless infra.
* **Multi-tenant**: The solution should support multi tenant use case.
* **Metrics**: Todo
* **Log:** Todo

## 4.What is, and what is not

* We design and optimize for observability use cases only. Not OLTP and OLAP.
* We only support time series log data on S3. We do not support query arbitrary data on S3.