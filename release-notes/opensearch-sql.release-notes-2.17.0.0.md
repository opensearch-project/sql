Compatible with OpenSearch and OpenSearch Dashboards Version 2.17.0

### Features
* Flint query scheduler part1 - integrate job scheduler plugin ([#2889](https://github.com/opensearch-project/sql/pull/2889))
* Flint query scheduler part 2 ([#2975](https://github.com/opensearch-project/sql/pull/2975))
* Add feature flag for async query scheduler ([#2989](https://github.com/opensearch-project/sql/pull/2989))

### Enhancements
* Change the default value of plugins.query.size_limit to MAX_RESULT_WINDOW (10000) ([#2877](https://github.com/opensearch-project/sql/pull/2877))
* Support common format geo point ([#2896](https://github.com/opensearch-project/sql/pull/2896))
* Add TakeOrderedOperator ([#2906](https://github.com/opensearch-project/sql/pull/2906))
* IF function should support complex predicates in PPL ([#2970](https://github.com/opensearch-project/sql/pull/2970))
* Add flags for Iceberg and Lake Formation and Security Lake as a data source type ([#2978](https://github.com/opensearch-project/sql/pull/2978))
* Adds validation to allow only flint queries and sql SELECT queries to security lake type datasource ([#2977](https://github.com/opensearch-project/sql/pull/2977))
* Delegate Flint index vacuum operation to Spark ([#2995](https://github.com/opensearch-project/sql/pull/2995))

### Bug Fixes
* Restrict UDF functions ([#2884](https://github.com/opensearch-project/sql/pull/2884))
* Update SqlBaseParser ([#2890](https://github.com/opensearch-project/sql/pull/2890))
* Boolean function in PPL should be case insensitive ([#2842](https://github.com/opensearch-project/sql/pull/2842))
* Fix SparkExecutionEngineConfigClusterSetting deserialize issue ([#2972](https://github.com/opensearch-project/sql/pull/2972))
* Fix jobType for Batch and IndexDML query ([#2982](https://github.com/opensearch-project/sql/pull/2982))
* Fix handler for existing query ([#2983](https://github.com/opensearch-project/sql/pull/2983))

### Infrastructure
* Increment version to 2.17.0-SNAPSHOT ([#2892](https://github.com/opensearch-project/sql/pull/2892))
* Fix :integ-test:sqlBwcCluster#fullRestartClusterTask ([#2996](https://github.com/opensearch-project/sql/pull/2996))

### Refactoring
* Add RequestContext parameter to verifyDataSourceAccessAndGetRawMetada method ([#2872](https://github.com/opensearch-project/sql/pull/2872))
* Add AsyncQueryRequestContext to QueryIdProvider parameter ([#2887](https://github.com/opensearch-project/sql/pull/2887))
* Add AsyncQueryRequestContext to FlintIndexMetadataService/FlintIndexStateModelService ([#2885](https://github.com/opensearch-project/sql/pull/2885))
* Add mvQuery attribute in IndexQueryDetails ([#2951](https://github.com/opensearch-project/sql/pull/2951))
* Add AsyncQueryRequestContext to update/get in StatementStorageService ([#2953](https://github.com/opensearch-project/sql/pull/2953))
* Extract validation logic from FlintIndexMetadataServiceImpl ([#2954](https://github.com/opensearch-project/sql/pull/2954))
