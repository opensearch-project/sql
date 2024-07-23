**Compatible with OpenSearch and OpenSearch Dashboards Version 2.16.0**

### Enhancements

* Added Setting to Toggle Data Source Management Code Paths ([#2811](https://github.com/opensearch-project/sql/pull/2811))
* Span in PPL statsByClause could be specified after fields ([#2810](https://github.com/opensearch-project/sql/pull/2810))

### Bug Fixes

* Fix SparkSubmitParameterModifier issue ([#2839](https://github.com/opensearch-project/sql/pull/2839))
* Temp use of older nodejs version before moving to Almalinux8 ([#2816](https://github.com/opensearch-project/sql/pull/2816))
* Fix yaml errors causing checks not to be run ([#2823](https://github.com/opensearch-project/sql/pull/2823))
* Well format the raw response when query parameter "pretty" enabled ([#2829](https://github.com/opensearch-project/sql/pull/2829))

### Infrastructure

* Increment version to 2.16.0-SNAPSHOT ([#2743](https://github.com/opensearch-project/sql/pull/2743))
* Fix checkout action failure ([#2819](https://github.com/opensearch-project/sql/pull/2819))
* Fix MacOS workflow failure ([#2831](https://github.com/opensearch-project/sql/pull/2831))

### Refactoring

* Change DataSourceType from enum to class ([#2746](https://github.com/opensearch-project/sql/pull/2746))
* Fix code style issue ([#2745](https://github.com/opensearch-project/sql/pull/2745))
* Scaffold async-query-core and async-query module ([#2751](https://github.com/opensearch-project/sql/pull/2751))
* Move classes from spark to async-query-core and async-query ([#2750](https://github.com/opensearch-project/sql/pull/2750))
* Exclude integ-test, doctest and download task when built offline ([#2763](https://github.com/opensearch-project/sql/pull/2763))
* Abstract metrics to reduce dependency to legacy ([#2768](https://github.com/opensearch-project/sql/pull/2768))
* Remove AsyncQueryId ([#2769](https://github.com/opensearch-project/sql/pull/2769))
* Add README to async-query-core ([#2770](https://github.com/opensearch-project/sql/pull/2770))
* Separate build and validateAndBuild method in DataSourceMetadata ([#2752](https://github.com/opensearch-project/sql/pull/2752))
* Abstract FlintIndex client ([#2771](https://github.com/opensearch-project/sql/pull/2771))
* Fix statement to store requested langType ([#2779](https://github.com/opensearch-project/sql/pull/2779))
* Push down OpenSearch specific exception handling ([#2782](https://github.com/opensearch-project/sql/pull/2782))
* Implement integration test for async-query-core ([#2785](https://github.com/opensearch-project/sql/pull/2785))
* Fix SQLQueryUtils to extract multiple tables ([#2791](https://github.com/opensearch-project/sql/pull/2791))
* Eliminate dependency from async-query-core to legacy ([#2792](https://github.com/opensearch-project/sql/pull/2792))
* Pass accountId to EMRServerlessClientFactory.getClient ([#2822](https://github.com/opensearch-project/sql/pull/2822))
* Register system index descriptors through SystemIndexPlugin.getSystemIndexDescriptors ([#2817](https://github.com/opensearch-project/sql/pull/2817))
* Introduce SparkParameterComposerCollection ([#2824](https://github.com/opensearch-project/sql/pull/2824))
