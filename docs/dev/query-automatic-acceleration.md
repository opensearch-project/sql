In a database engine, there are different ways to optimize query performance. For instance, rule-based/cost-based optimizer and distributed execution layer tries to find best execution plan by cost estimate and equivalent transformation of query plan. Here we're proposing an alternative approach which is to accelerate query execution by materialized view for time-space tradeoff.

### Architecture

Here is a reference architecture that illustrates components and the entire workflow which essentially is a *workload-driven feedback loop*:

1. *Input*: Query plan telemetry collected
2. *Generating feedback*: Feeding it into a workload-driven feedback generator
3. *Output*: Feedback for optimizer to rewrite query plan in future

Basically, *feedback* is referring to various materialized view prebuilt (either online or offline) which hints acceleration opportunity to query optimizer.

![AutoMV (1) (1)](https://user-images.githubusercontent.com/46505291/168863085-d5d39ab4-40bf-41c8-922c-4a45572e40dd.png)

There are 2 areas and paths moving forward for both of which lack open source solutions:

* OpenSearch engine acceleration: accelerate DSL or SQL/PPL engine execution
* MPP/Data Lake engine acceleration: accelerate Spark, Presto, Trino

### General Acceleration Workflow

#### 1.Workload Telemetry Collecting

Collect query plan telemetry generated in query execution and emit it as feedback generation input.

* *Query Plan Telemetry:* Specifically, query plan telemetry means statistics collected on each physical node (sub-query or sub-expression) when execution. Generally, the statistics include input/output size, column cardinality, running time etc. Eventually logical plan is rewritten to reuse materialized view, so the statistics in execution may need to be linked to logical plan before emitting telemetry data.
* *Challenge:* Efforts required in this stage depends on to what extent the query engine is observable and how easy telemetry can be collected.

#### 2.Workload Telemetry Preprocessing

Preprocess query plan telemetry into uniform workload representation.

* *Workload Representation*: uniform workload representation decouples subsequent stages from specific telemetry data format and store.

#### 3.View Selection

Analyze workload data and select sub-query as materialization candidate according to view selection algorithm.

* *Algorithm*
    * View selection algorithm can be heuristic rule, such as estimate high performance boost and low materialization cost, or by more complex learning algorithm.
    * Alternatively the selection can be manually done by customers with access to all workload statistics.
    * In between is giving acceleration suggestion by advisor and allow customer intervene to change the default acceleration strategy.
* *Select Timing*
    * *Online:* analyze and select view candidate at query processing time which benefits interactive/ad-hoc queries
    * *Offline:* shown as in figure above
* *Challenge*: Automatic workload analysis and view selection is challenging and may require machine learning capability. Simple heuristic rules mentioned above may be acceptable. Alternative options include view recommendation advisor or manual selection by customers.

#### 4.View Materialization and Maintenance

Materialize selected view and maintain the consistency between source data and materialized view data, by incrementally refreshing for example.

* *Materialized View:* is a database object that contains the results of a query. The result may be subset of a single table or multi-table join, or may be a summary using an aggregate function
    * *Query Result Cache*
        * *Full Result*: MV that stores entire result set and can only be reused by same deterministic query
        * *Intermediate Result*: MV that stores result for a subquery (similar as Covering Index if filtering operation only)
    * *Secondary Index*
        * *Data Skipping Index*: MV that stores column statistics in coarse-grained way, Small Materialized Aggregate, and thus skip those impossible to produce a match. Common SMA includes Min-Max, Bloom Filter, Value List.
        * *Covering Index*: MV that stores indexed column value(s) and included column value(s) so that index itself can answer the query and no need to access original data. Common index implementation includes B-tree family, Hash, Z-Ordering index.
    * *Approximate Data Structure*
* *Materialization Timing*
    * *Ingestion Time:* for a view defined and materialized at ingestion time, it can be “registered” to Selected View table in figure above (ex. by DDL CREATE MV). In this way the query acceleration framework can take care of query plan optimization
        * Parquet: min-max SMA
        * S3 query federation: user defined transformation as final resulting MV. More details in https://github.com/opensearch-project/sql/issues/595
        * Loki: labels as skipping hash index
        * DataSketch: approximate data structure
    * *Online (Query Processing Time):* add materialization overhead to first query in future
    * *Offline:* shown as in figure above
* *Challenge:* To ensure consistency, the materialized view needs to be in sync with source data. Without real-time notification to refresh or invalidate, hybrid scan or similar mechanism is required to reuse partial stale materialized view.

#### 5.Query Plan Optimization

At last, query optimizer checks the existing materialized view and replace original operator with scan on materialized view.

* *View Matching:* match sub-query with materialized view
* *Query Rewrite:* replace query plan node with materialized view scan operator