# Overview
Build federated PPL query engine to fetch data from multiple data sources.

## 1. Motivation
PPL(Piped Processing Language) serves as the de-facto query language for all the observability solutions(Event Explorer, Notebooks, Trace Analytics) built on OpenSearch Dashboards. In the current shape, PPL engine can only query from OpenSearch and this limits observability solutions to leverage data from other data sources. As part of this project, we want to build framework to support multiple data sources and initially implement support for metric data stores like Prometheus and AWS Cloudwatch. This federation will also help in injecting data from different sources to ML commands, correlate metrics and logs from different datasources.

## 2. Glossary

* *Observability :* The ability to understand whats happening inside your business/application using logs, traces, metrics and other data emitted from the application.


## 3.Tenets
* Query interface should be agnostic of underlying data store.
* Changes to PPL Query Language grammar should be simple and easy to onboard.
* Component design should be extensible for supporting new data stores.

## 4.Out of Scope for the Design.

* Join Queries across datasources is out of scope.
* Distributed Query Execution is out of scope and the current execution will happen on single coordination node.

## 5. Requirements

### 5.1 *Functional*

* As an OpenSearch user, I should be able to configure and add a new data source with available connectors.
* As an OpenSearch user, I should be able to query data from all the configured data sources using PPL.
* As an Opensearch contributor, I should be able to add connectors for different data sources.

### 5.2 *Non Functional*

* Query Execution Plan should be optimized and pushed down as much as possible to underlying data stores.
* Latency addition from query engine should be small when compared to that of underlying data stores.

## 6. High Level Design

![prometheus drawio(2)](https://user-images.githubusercontent.com/99925918/166970105-09855a1a-6db9-475f-9638-eb240022e399.svg)

At high level, our design is broken down into below major sections.

* Data Source representation.
    * This section speaks on the new constructs introduced in the query engine to support additional data sources.
* Query Engine Architecture Changes.
    * This section gives an overview of the architecture and changes required for supporting multiple data sources.
* PPL grammar changes for metric data and details for implementing Prometheus Connector.




### 6.1 Data source representation.

Below are the new constructs introduced to support additional datasources. These constructs helps in identfying and referring correct datasource for a particular query.

* *Connector :* Connector is a component that adapts the query engine to a datasource. For eg: Prometheus connector would adapt and help execute the queries to run on Prometheus data source.

* *Catalog :* we can describe a catalog as the union of a connector and the underlying instance of the datasource being referred. This gives us flexbility to refer different instances of a similar datastore with the same connector i.e. we can refer data from multiple prometheus instances using prometheus connector. The name for each catalog should be different.

Example Prometheus Catalog Definition
```
[{
    "name" : "prometheus_1",
    "connector": "prometheus", 
    "uri" : "http://localhost:9090",
    "authentication" : {
        "type" : "basicauth",
        "username" : "admin",
        "password" : "admin"
    }
}]
```

* *Table :* A table is a set of unordered rows which are organized into named columns with types. The mapping from source data to tables is defined by the connector.


* In order to query data from above Prometheus Catalog, Query would look like something below.
    * `source = prometheus_1.total_cpu_usage | stats avg(@value) by region`
* What will be the interface to add new catalogs?
    * In the initial phase, we are trying to adopt a simple approach of storing the above configuration to opensearch-keystore. Since this catalog configuration contains credential data we are taking this input from the user via keystore(secure-setting)
```
bin/opensearch-keystore add-file plugins.query.federation.catalog.config catalog.json
```

* Catalog is optional and it will be defaulted to opensearch instance in which the PPL query engine plugin is running.
    * `source = accounts`, this is a valid query and the engine defaults to opensearch instance to get the data from.


### 6.2  Query Engine Architecture.

There are no major changes in the query engine flow as the current state is extensible to support new datastores.

Changes required
* Current StorageEngine and Table injection beans default to Opensearch Connector implementation. This should be dynamic and based on the catalog mentioned in the source command.



![PPL Execution flow](https://user-images.githubusercontent.com/99925918/163804980-8e1f5c07-4776-4497-97d5-7b2eec1ffb8e.svg)

Interfaces to be implemented for supporting a new data source connector.

```java
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(String name);
}
```


```java
public interface Table {

  /**
   * Get the {@link ExprType} for each field in the table.
   */
  Map<String, ExprType> getFieldTypes();

  /**
   * Implement a {@link LogicalPlan} by {@link PhysicalPlan} in storage engine.
   *
   * @param plan logical plan
   * @return physical plan
   */
  PhysicalPlan implement(LogicalPlan plan);

  /**
   * Optimize the {@link LogicalPlan} by storage engine rule.
   * The default optimize solution is no optimization.
   *
   * @param plan logical plan.
   * @return logical plan.
   */
  default LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

}
```

```java
public interface ExecutionEngine {

  /**
   * Execute physical plan and call back response listener.
   *
   * @param plan     executable physical plan
   * @param listener response listener
   */
  void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener);

}
```

### 6.3 PPL Metric Grammar and Prometheus Connector.

This section talks about metric grammar for PPL. Till now, PPL catered only for event data and language grammar was built around that. With the introduction of support for new metric data stores, we want to analyse and add changes changes to support metric data. As called out in tenets, we want to build simple intuitive grammar through which developers can easily onboard.

*Key Characteristics of Metric Data*

* Metric data is a timeseries vector. Below are the four major attributes of any metric data.
    * Metric Name
    * Measured value
    * Dimensions
    * Timestamp
* Every time-series is uniquely identified by metric and a combination of value of dimensions.


Since we don't have column names similar to relational databases, `@timestamp` and `@value` are new internal keywords used to represent the time and measurement of a metric.


*Types of metric queries*
***These queries are inspired from widely used Prometheus data store.***


### Passthrough PromQL Support.
* PromQL is easy to use powerful language over metrics and we want to support promQL queries from catalogs with prometheus connectors.
* Function signature: source = ``promcatalog.nativeQuery(`promQLCommand`, startTime = "{{startTime}}", endTime="{{endTime}}", step="{{resolution}}")``



### PPL queries on prometheus.
### 1. Selecting Time Series


|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`node_cpu_seconds_total`	|source = promcatalog.`node_cpu_seconds_total`	| 	|
|`node_cpu_seconds_total[5m]`	|source = promcatalog.`node_cpu_seconds_total` \| where endtime = now() and starttime = now()-5m	| we can either use `startime and endtime` ? or `@timestamp < now() and @timstamp >now() - 5m`  |
|`node_cpu_seconds_total{cpu="0",mode="idle"}`	|source  = promcatalog.`node_cpu_seconds_total` \| where  cpu = '0' and mode = 'idle'	|This again got the same limitations as first query. Where should we stop the result set.	|
|`process_resident_memory_bytes offset 1d`	|source = promcatalog.`process_resident_memory_bytes` \| where starttime = now()-1d and endtime = 1d	|	|



###### Limitations and Solutions

* How do we limit the output size of timeseries select query? We can't output the entire history.
    * **[Solution 1]** Make `starttime` and `endtime` compulsory ?
        * This will create validations based on the catalog used and the grammar be agnostic of the underlying catalog.
    * **[Solution 2]** Have a hard limit on the result set length. Let user don’t specify time range and dig deep into the timeseries until it hits the hard limit. For eg: In case of prometheus, How can we execute this under hood, there is no limit operator in promQl. The only limit we could set is the time range limit.
    * **[Proposed Solution 3]** Have a default time range specified in the catalog configuration. If user specifies no filter on timerange in where clause, this default timerange will be applied similar to limiting eventData resultSet in the currrent day for OpenSearch.


### 2. Rates of increase for counters

|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`rate(demo_api_request_duration_seconds_count[5m])`	|source = promcatalog.`demo_api_request_duration_seconds_count` \| eval x = rate(@value, 5m)	|	|
|`irate(demo_api_request_duration_seconds_count[1m])`	|source = promcatalog.`demo_api_request_duration_seconds_count` \| eval x = irate(@value, 5m)	|	|
|`increase(demo_api_request_duration_seconds_count[1h])`	|source = promcatalog.`demo_api_request_duration_seconds_count` \| eval x = increase(@value, 5m)	|	|

* Should we output x along with @value in two columns or restrict to rate function output,
    * [timestamp, value, rate]  or [timestamp, x]
* If we push down the query to Prometheus we only get rate as output. Should we not push down and calculate on the coordination node?


### 3. Aggregating over multiple series

|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`sum(node_filesystem_size_bytes)`	|source = promcatalog.`node_filesystem_size_bytes `\| stats sum(@value) by span(@timestamp, 5m)	|	|
|`sum by(job, instance) (node_filesystem_size_bytes)`	|source = promcatalog.`node_filesystem_size_bytes `\| stats sum(@value) by instance, job span(@timestamp, 5m)	|	|
|`sum without(instance, job) (node_filesystem_size_bytes)`	|source = promcatalog.`node_filesystem_size_bytes `\| stats sum(@value) without instance, job	|We dont have without support. We need to build that, we can group by fields other than instance, job.	|

`sum()`, `min()`, `max()`, `avg()`, `stddev()`, `stdvar()`, `count()`, `count_values()`, `group()`, `bottomk()`, `topk()`, `quantile() Additional Aggregation Functions.
`

* On time series, we calculate aggregations with in small buckets over a large period of time.  For eg: calculate the average over 5min for the last one hour. PromQL gets the time parameters from API parameters, how can we get these for PPL. Can we make these compulsory but that would make the language specific to metric datastore which doesn’t apply for event data store.
* Can we have separate command mstats for metric datastore with compulsory span expression.

### 4.  Math Between Time Series [Vector Operation Arithmetic]

Arithmetic between series. Allowed operators : , `-`, `*`, `/`, `%`, `^`

|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`node_memory_MemFree_bytes + node_memory_Cached_bytes`	|source = promcatalog.`node_memory_MemFree_bytes \|` `vector_op`(+) promcatalog.`node_memory_Cached_bytes`	|Add all equally-labelled series from both sides:	|
|`node_memory_MemFree_bytes + on(instance, job) node_memory_Cached_bytes`	|source = promcatalog.`node_memory_MemFree_bytes \|` `vector_op`(+) on(instance, job) promcatalog.`node_memory_Cached_bytes`	|Add series, matching only on the `instance` and `job` labels:	|
|`node_memory_MemFree_bytes + ignoring(instance, job) node_memory_Cached_bytes`	|source = promcatalog.`node_memory_MemFree_bytes \|` `vector_op`(+) ignoring(instance, job) promcatalog.`node_memory_Cached_bytes`	|Add series, ignoring the `instance` and `job` labels for matching:	|
|`rate(demo_cpu_usage_seconds_total[1m]) / on(instance, job) group_left demo_num_cpus`	|source = `rate(promcatalog.demo_cpu_usage_seconds_total[1m]) ` \| `vector_op`(/) on(instance, job) group_left promcatalog.`node_memory_Cached_bytes`	|Explicitly allow many-to-one matching:	|
|`node_filesystem_avail_bytes * on(instance, job) group_left(version) node_exporter_build_info`	|source = promcatalog.`node_filesystem_avail_bytes` \| `vector_op`(*) on(instance, job) group_left(version) promcatalog.`node_exporter_build_info`	| Include the `version` label from "one" (right) side in the result:	|



* Event data joins can have below grammar.
    * `source = lefttable | join righttable on columnName`
    * `source = lefttable | join righttable on $left.leftColumn = $right.rightColumn`
* Metric data grammar. Since joins are mostly for vector arithmetic.
    * `source=leftTable | vector_op(operator) on|ignoring  group_left|group_right rightTable `
* What would be a better keyword `vector_op` or `mjoin` or `{new}`
* Does vector_op has any meaning for event data.
    * Can we restrict vector_op for metric data only?
    * If yes, How should we identify if something is of metric data type.
    * This can break the tenet of designing langauge grammar irrespective of the underlying datastore.



### 5. Filtering series by value [Vector Operation Comparitive]



|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`node_filesystem_avail_bytes > 10*1024*1024`	|source = promcatalog.`node_filesystem_avail_bytes` \|  `vector_op(>) 10*1024*1024`	|Only keep series with a sample value greater than a given number:	|
|`go_goroutines > go_threads`	|source = promcatalog.`node_memory_MemFree_bytes` \| `vector_op`(+) promcatalog.`node_memory_Cached_bytes`	|Only keep series from the left-hand side whose sample values are larger than their right-hand-side matches:	|
|`go_goroutines > bool go_threads`	|source = promcatalog.`go_goroutines` \| `vector_op`(> bool) promcatalog.`go_threads`	|Instead of filtering, return `0` or `1` for each compared series:	|
|`go_goroutines > bool on(job, instance) go_threads`	|source = promcatalog.`go_goroutines` \| `vector_op`(> bool) on(job,instance) promcatalog.`go_threads`	|Match only on specific labels:	|


* The above operations are similar to 4th section, except the operators are comparision operators instead of arithmetic,
* The operations are always between `a vector and scalar` or `a vector and a vector`


### 6. Set operations [Vector Operation Logical]

|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`up{job="prometheus"} or up{job="node"}`	|source = promcatalog.`up` \| where job="prometheus" \| `vector_op(or)  inner(`promcatalog.up \| where job="node") |Only keep series with a sample value greater than a given number:	|
|`node_network_mtu_bytes and (node_network_address_assign_type == 0)`	|source = promcatalog.`node_network_mtu_bytes` \|  `vector_op(and)  inner(`promcatalog.`node_network_address_assign_type` \| `vector_op(==) 0)`	|Include any label sets that are present both on the left and right side:	|
|`node_network_mtu_bytes unless (node_network_address_assign_type == 1)`	|source = promcatalog.`node_network_mtu_bytes` \| `vector_op(unless)  inner(`promcatalog`node_network_address_assign_type` \| `vector_op(==) 1) ` 	|Include any label sets from the left side that are not present in the right side:	|
|`node_network_mtu_bytes and on(device) (node_network_address_assign_type == 0)`	|source = `promcatalog.`node_network_mtu_bytes` \| `` vector_op(and)  on(device) inner(`promcatalog.`node_network_address_assign_type \| vector_op(==) 1) ` |Match only on specific labels:	|


### 7.Quantiles from histograms



|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`histogram_quantile(0.9, rate(demo_api_request_duration_seconds_bucket[5m]))`	|source = promcatalog.`demo_api_request_duration_seconds_bucket` \| eval x = `rate`(@value) \| eval k = `histogram_quantile`(le=0.9,x)	|90th percentile request latency over last 5 minutes, for every label dimension:	
|`histogram_quantile(0.9,sum by(le, path, method) (rate(demo_api_request_duration_seconds_bucket[5m])))`	| source = promcatalog.`demo_api_request_duration_seconds_bucket` \| eval x= `rate`(@value, 5m) \| stats `sum(x)` by (le,path,method) 	| 90th percentile request latency over last 5 minutes, for only the `path` and `method` dimensions.	|


* Can we apply nested functions?

### 8. Changes in gauges

|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`deriv(demo_disk_usage_bytes[1h])`	|source = promcatalog.`demo_disk_usage_bytes` \| eval x = deriv(@value, 1h)	|Per-second derivative using linear regression:	|
|`predict_linear(demo_disk_usage_bytes[4h], 3600)`	|source = promcatalog.`demo_disk_usage_bytes` \| eval x = `predict_linear`(@value, 4h, 3600)	|Predict value in 1 hour, based on last 4 hours:	|

Has the same drawbacks as earlier eval function commands.


### 9.Aggregating over time

|PromQL	|PPL	|Remarks	|
|---	|---	|---	|
|`avg_over_time(go_goroutines[5m])`	|source = promcatalog.`go_goroutines` \| eval k = `avg_over_time`(@value, 5m)	|Average within each series over a 5-minute period:	|
|`max_over_time(process_resident_memory_bytes[1d])`	|source = promcatalog.`process_resident_memory_bytes` \| eval k = `max_over_time`(@value, 1d)	|Get the maximum for each series over a one-day period:	|
|`count_over_time(process_resident_memory_bytes[5m])`	|source = promcatalog.`process_resident_memory_bytes` \| eval k = `count_over_time`(@value, 5m)	|Count the number of samples for each series over a 5-minute period:	|


## 7. Implementation

Tasks and Phase wise Division

|Goal	|Description	|Phase	|Area	|
|---	|---	|---	|---	|
|Catalog Configuration for external Metric Datastore	|Provides rudimentary way of capturing catalog connection information using opensearch-keystore.	|P0	|Backend (OS SQL Plugin)	|
|PPL Grammar to support basic prometheus metric operations.	|This involes support of PPL commands search, stats, where commands on Prometheus Data	|P0	|Backend (OS SQL Plugin)	|
|PromQL support	|PromQL support for querying prometheus	|P0	|Backend (OS SQL Plugin)	|
|Event Analytic Page enhancements	|This includes UI support in the existing event analytics page of for visualizations of metric data from Prometheus	|P0	|UI (OSD Observability Plugin)	|
|PPL Grammar to support advanced prometheus metric operations.	|This includes design and implementation of PPL grammar for advanced query operations like cross-metric commands, rate commands, histogram and summary commands.	|P1	|Backend (OS SQL Plugin)	|
|Metric analytics page 	|Build a new page explicitly for viewing metrics from all sources 	|P1	|UI (OSD Observability Plugin)	|


Quick Demo:

https://user-images.githubusercontent.com/99925918/164787195-86a17d34-cf75-4a40-943f-24d1cf7b9a51.mov