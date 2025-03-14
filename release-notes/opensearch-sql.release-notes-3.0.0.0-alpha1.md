Compatible with OpenSearch and OpenSearch Dashboards Version 3.0.0.0-alpha1

### Breaking Changes

* [Release 3.0] Bump gradle 8.10.2 / JDK23 / 3.0.0.0-alpha1 on SQL plugin ([#3319](https://github.com/opensearch-project/sql/pull/3319))
* [v3.0.0] Remove SparkSQL support ([#3306](https://github.com/opensearch-project/sql/pull/3306))
* [v3.0.0] Remove opendistro settings and endpoints ([#3326](https://github.com/opensearch-project/sql/pull/3326))
* [v3.0.0] Deprecate SQL Delete statement ([#3337](https://github.com/opensearch-project/sql/pull/3337))
* [v3.0.0] Deprecate scroll API usage ([#3346](https://github.com/opensearch-project/sql/pull/3346))
* [v3.0.0] Deprecate OpenSearch DSL format ([#3367](https://github.com/opensearch-project/sql/pull/3367))

### Features

* PPL: Add `json` function and `cast(x as json)` function ([#3243](https://github.com/opensearch-project/sql/pull/3243))

### Enhancements

* Add other functions to SQL query validator ([#3304](https://github.com/opensearch-project/sql/pull/3304))
* Improved patterns command with new algorithm ([#3263](https://github.com/opensearch-project/sql/pull/3263))
* Clean up syntax error reporting ([#3278](https://github.com/opensearch-project/sql/pull/3278))

### Maintenance

* Build: Centralise dependencies version - Pt1 ([#3294](https://github.com/opensearch-project/sql/pull/3294))
* Remove dependency from async-query-core to datasources ([#2891](https://github.com/opensearch-project/sql/pull/2891))
