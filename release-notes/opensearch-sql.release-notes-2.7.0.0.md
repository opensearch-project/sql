Compatible with OpenSearch and OpenSearch Dashboards Version 2.7.0

### Features

* Create datasource API (#1458) ([#1479](https://github.com/opensearch-project/sql/pull/1479))
* REST API for GET,PUT and DELETE ([#1502](https://github.com/opensearch-project/sql/pull/1502))

### Enhancements

* [main]Changes in DataSourceService and DataSourceMetadataStorage interface ([#1414](https://github.com/opensearch-project/sql/pull/1414))
* Added SINH function to V2 engine ([#1437](https://github.com/opensearch-project/sql/pull/1437))
* Added RINT function to V2 engine  ([#1439](https://github.com/opensearch-project/sql/pull/1439))
* Add `sec_to_time` Function To OpenSearch SQL ([#1438](https://github.com/opensearch-project/sql/pull/1438))
* Add `WEEKDAY` Function to SQL Plugin ([#1440](https://github.com/opensearch-project/sql/pull/1440))
* Add `YEARWEEK` Function To OpenSearch SQL ([#1445](https://github.com/opensearch-project/sql/pull/1445))
* Add `EXTRACT` Function To OpenSearch SQL Plugin ([#1443](https://github.com/opensearch-project/sql/pull/1443))
* Add `STR_TO_DATE` Function To The SQL Plugin ([#1444](https://github.com/opensearch-project/sql/pull/1444))
* Add The `TO_SECONDS` Function To The SQL Plugin ([#1447](https://github.com/opensearch-project/sql/pull/1447))
* Added Arithmetic functions to V2 engine ([#1448](https://github.com/opensearch-project/sql/pull/1448))
* Added SIGNUM function to V2 engine ([#1442](https://github.com/opensearch-project/sql/pull/1442))
* Add `TIMESTAMPADD` Function To OpenSearch SQL Plugin ([#1453](https://github.com/opensearch-project/sql/pull/1453))
* Add `Timestampdiff` Function To OpenSearch SQL ([#1472](https://github.com/opensearch-project/sql/pull/1472))
* Add Nested Support in Select Clause (#1490) ([#1518](https://github.com/opensearch-project/sql/pull/1518))
* Fix null response from pow/power and added missing integration testing ([#1459](https://github.com/opensearch-project/sql/pull/1459))


### Bug Fixes

* Integ Test Refactoring ([#1383](https://github.com/opensearch-project/sql/pull/1383))
* Exclude OpenSearch system index when IT cleanup ([#1381](https://github.com/opensearch-project/sql/pull/1381))
* Ensure Nested Function Falls Back to Legacy Engine Where Not Supported ([#1549](https://github.com/opensearch-project/sql/pull/1549))

### Documentation

* Documentation and other papercuts for datasource api launch ([#1534](https://github.com/opensearch-project/sql/pull/1534))

### Maintenance

* Refactor AWSSigV4 auth to support different AWSCredentialProviders ([#1389](https://github.com/opensearch-project/sql/pull/1389))
* [AUTO] Increment version to 2.7.0-SNAPSHOT ([#1368](https://github.com/opensearch-project/sql/pull/1368))
* Update usage of Strings.toString ([#1404](https://github.com/opensearch-project/sql/pull/1404))
* Deprecated Spring IoC and using Guice instead (#1177) ([#1410](https://github.com/opensearch-project/sql/pull/1410))
* Bump backport version. ([#1009](https://github.com/opensearch-project/sql/pull/1009))
* Resolve table function based on StorageEngine provided function resolver ([#1424](https://github.com/opensearch-project/sql/pull/1424))
* Rework on `OpenSearchDataType`: parse, store and use mapping information ([#1455](https://github.com/opensearch-project/sql/pull/1455))
* Update to account for XContent refactor in 2.x ([#1485](https://github.com/opensearch-project/sql/pull/1485))
* Replace non-ASCII characters in code and docs. ([#1486](https://github.com/opensearch-project/sql/pull/1486))
* Add publish snapshots to maven via GHA ([#1496](https://github.com/opensearch-project/sql/pull/1496))
* Refactoring datasource changes to a new module. ([#1511](https://github.com/opensearch-project/sql/pull/1511))
* #639: allow metadata fields and score opensearch function  (#228) ([#1509](https://github.com/opensearch-project/sql/pull/1509))
