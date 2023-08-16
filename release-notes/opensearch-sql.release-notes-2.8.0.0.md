Compatible with OpenSearch and OpenSearch Dashboards Version 2.8.0

### Features

* Support for pagination in v2 engine of SELECT * FROM <table> queries ([#1666](https://github.com/opensearch-project/sql/pull/1666))
* Support Alternate Datetime Formats ([#1664](https://github.com/opensearch-project/sql/pull/1664))
* Create new anonymizer for new engine ([#1665](https://github.com/opensearch-project/sql/pull/1665))
* Add Support for Nested Function Use In WHERE Clause Predicate Expresion ([#1657](https://github.com/opensearch-project/sql/pull/1657))
* Cross cluster search in PPL ([#1512](https://github.com/opensearch-project/sql/pull/1512))
* Added COSH to V2 engine ([#1428](https://github.com/opensearch-project/sql/pull/1428))
* REST API for GET,PUT and DELETE ([#1482](https://github.com/opensearch-project/sql/pull/1482))

### Enhancements

* Minor clean up of datetime and other classes ([#1310](https://github.com/opensearch-project/sql/pull/1310))
* Add integration JDBC tests for cursor/fetch_size feature ([#1315](https://github.com/opensearch-project/sql/pull/1315))
* Refactoring datasource changes to a new module. ([#1504](https://github.com/opensearch-project/sql/pull/1504))

### Bug Fixes

* Fixing bug where Nested functions used in WHERE, GROUP BY, HAVING, and ORDER BY clauses don't fallback to legacy engine. ([#1549](https://github.com/opensearch-project/sql/pull/1549))

### Documentation

* Add Nested Documentation for 2.7 Related Features ([#1620](https://github.com/opensearch-project/sql/pull/1620))
* Update usage example doc for PPL cross-cluster search ([#1610](https://github.com/opensearch-project/sql/pull/1610))
* Documentation and other papercuts for datasource api launch ([#1530](https://github.com/opensearch-project/sql/pull/1530))

### Maintenance

* Fix IT - address breaking changes from upstream. ([#1659](https://github.com/opensearch-project/sql/pull/1659))
* Increment version to 2.8.0-SNAPSHOT ([#1552](https://github.com/opensearch-project/sql/pull/1552))
* Backport maintainer list update to `2.x`. ([#1650](https://github.com/opensearch-project/sql/pull/1650))
* Backport jackson and gradle update from #1580 to 2.x ([#1596](https://github.com/opensearch-project/sql/pull/1596))
* adding reflections as a dependency ([#1559](https://github.com/opensearch-project/sql/pull/1596))
* Bump org.json dependency version ([#1586](https://github.com/opensearch-project/sql/pull/1586))
* Integ Test Fix ([#1541](https://github.com/opensearch-project/sql/pull/1541))