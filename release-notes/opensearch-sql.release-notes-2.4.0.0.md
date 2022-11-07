Compatible with OpenSearch and OpenSearch Dashboards Version 2.4.0

### Features

#### Data Source Management

* Catalog Implementation ([#819](https://github.com/opensearch-project/sql/pull/819))
* Catalog to Datasource changes ([#1027](https://github.com/opensearch-project/sql/pull/1027))

#### Prometheus Support

* Prometheus Connector Initial Code ([#878](https://github.com/opensearch-project/sql/pull/878))
* Restricted catalog name to [a-zA-Z0-9_-] characters ([#876](https://github.com/opensearch-project/sql/pull/876))
* Table function for supporting prometheus query_range function ([#875](https://github.com/opensearch-project/sql/pull/875))
* List tables/metrics using information_schema in source command. ([#914](https://github.com/opensearch-project/sql/pull/914))
* [Backport 2.4] Prometheus select metric and stats queries. ([#1020](https://github.com/opensearch-project/sql/pull/1020))

#### Log Pattern Command

* Add patterns and grok command ([#813](https://github.com/opensearch-project/sql/pull/813))

#### ML Command

* Add category_field to AD command in PPL ([#952](https://github.com/opensearch-project/sql/pull/952))
* A Generic ML Command in PPL ([#971](https://github.com/opensearch-project/sql/pull/971))

### Enhancements

* Add datetime functions `FROM_UNIXTIME` and `UNIX_TIMESTAMP` ([#835](https://github.com/opensearch-project/sql/pull/835))
* Adding `CONVERT_TZ` and `DATETIME` functions to SQL and PPL  ([#848](https://github.com/opensearch-project/sql/pull/848))
* Add Support for Highlight Wildcard in SQL ([#827](https://github.com/opensearch-project/sql/pull/827))
* Update SQL CLI to use AWS session token. ([#918](https://github.com/opensearch-project/sql/pull/918))
* Add `typeof` function. ([#867](https://github.com/opensearch-project/sql/pull/867))
* Show catalogs ([#925](https://github.com/opensearch-project/sql/pull/925))
* Add functions `PERIOD_ADD` and `PERIOD_DIFF`. ([#933](https://github.com/opensearch-project/sql/pull/933))
* Add take() aggregation function in PPL ([#949](https://github.com/opensearch-project/sql/pull/949))
* Describe Table with catalog name. ([#989](https://github.com/opensearch-project/sql/pull/989))
* Catalog Enhancements ([#988](https://github.com/opensearch-project/sql/pull/988))
* Rework on error reporting to make it more verbose and human-friendly. ([#839](https://github.com/opensearch-project/sql/pull/839))

### Bug Fixes

* Fix EqualsAndHashCode Annotation Warning Messages ([#847](https://github.com/opensearch-project/sql/pull/847))
* Remove duplicated png file ([#865](https://github.com/opensearch-project/sql/pull/865))
* Fix NPE with multiple queries containing DOT(.) in index name. ([#870](https://github.com/opensearch-project/sql/pull/870))
* Update JDBC driver version ([#941](https://github.com/opensearch-project/sql/pull/941))
* Fix result order of parse with other run time fields ([#934](https://github.com/opensearch-project/sql/pull/934))
* AD timefield name issue ([#919](https://github.com/opensearch-project/sql/pull/919))
* [Backport 2.4] Add function name as identifier in antlr ([#1018](https://github.com/opensearch-project/sql/pull/1018))
* [Backport 2.4] Fix incorrect results returned by `min`, `max` and `avg` ([#1022](https://github.com/opensearch-project/sql/pull/1022))

### Infrastructure

* Fix failing ODBC workflow ([#828](https://github.com/opensearch-project/sql/pull/828))
* Reorganize GitHub workflows. ([#837](https://github.com/opensearch-project/sql/pull/837))
* Update com.fasterxml.jackson to 2.13.4 to match opensearch repo. ([#858](https://github.com/opensearch-project/sql/pull/858))
* Trigger build on pull request synchronize action. ([#873](https://github.com/opensearch-project/sql/pull/873))
* Update Jetty Dependency ([#872](https://github.com/opensearch-project/sql/pull/872))
* Fix manual CI workflow and add `name` option. ([#904](https://github.com/opensearch-project/sql/pull/904))
* add groupId to pluginzip publication ([#906](https://github.com/opensearch-project/sql/pull/906))
* Enable ci for windows and macos ([#907](https://github.com/opensearch-project/sql/pull/907))
* Update group to groupId ([#908](https://github.com/opensearch-project/sql/pull/908))
* Enable ignored and disabled tests ([#926](https://github.com/opensearch-project/sql/pull/926))
* Update version of `jackson-databind` for `sql-jdbc` only ([#943](https://github.com/opensearch-project/sql/pull/943))
* Add security policy for ml-commons library ([#945](https://github.com/opensearch-project/sql/pull/945))
* Change condition to always upload coverage for linux workbench ([#967](https://github.com/opensearch-project/sql/pull/967))
* Bump ansi-regex for workbench ([#975](https://github.com/opensearch-project/sql/pull/975))
* Removed json-smart in the JDBC driver ([#978](https://github.com/opensearch-project/sql/pull/978))
* Update MacOS Version for ODBC Driver ([#987](https://github.com/opensearch-project/sql/pull/987))
* Update Jackson Databind version to 2.13.4.2 ([#992](https://github.com/opensearch-project/sql/pull/992))
* [Backport 2.4] Bump sql-cli version to 1.1.0 ([#1024](https://github.com/opensearch-project/sql/pull/1024))

### Documentation

* Add Forum link in SQL plugin README.md ([#809](https://github.com/opensearch-project/sql/pull/809))
* Fix indentation of patterns example ([#880](https://github.com/opensearch-project/sql/pull/880))
* Update docs - missing changes for #754. ([#884](https://github.com/opensearch-project/sql/pull/884))
* Fix broken links ([#911](https://github.com/opensearch-project/sql/pull/911))
* Adding docs related to catalog. ([#963](https://github.com/opensearch-project/sql/pull/963))
* SHOW CATALOGS documentation and integ tests ([#977](https://github.com/opensearch-project/sql/pull/977))
* [Backport 2.4] Add document for ml command. ([#1017](https://github.com/opensearch-project/sql/pull/1017))

