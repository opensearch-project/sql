Compatible with OpenSearch and OpenSearch Dashboards Version 2.9.0

### Features

* Enable Table Function and PromQL function ([#1719](https://github.com/opensearch-project/sql/pull/1719))
* Add Spark connector ([#1780](https://github.com/opensearch-project/sql/pull/1780))

### Enhancements

* Pagination: Support WHERE clause, column list in SELECT clause and for functions and expressions in the query ([#1500](https://github.com/opensearch-project/sql/pull/1500))
* Pagination: Support ORDER BY clauses and queries without FROM clause ([#1599](https://github.com/opensearch-project/sql/pull/1599))
* Remove backticks on by field in stats ([#1728](https://github.com/opensearch-project/sql/pull/1728))
* Support Array and ExprValue Parsing With Inner Hits ([#1737](https://github.com/opensearch-project/sql/pull/1737))
* Add Support for Nested Function in Order By Clause ([#1789](https://github.com/opensearch-project/sql/pull/1789))
* Add Support for Field Star in Nested Function ([#1773](https://github.com/opensearch-project/sql/pull/1773))
* Guarantee datasource read api is strong consistent read (compatibility with segment replication) ([#1815](https://github.com/opensearch-project/sql/pull/1815))
* Added new datetime functions and aliases to PPL ([#1807](https://github.com/opensearch-project/sql/pull/1807))
* Support user-defined and incomplete date formats ([#1821](https://github.com/opensearch-project/sql/pull/1821))
* Add _routing to SQL includes list ([#1771](https://github.com/opensearch-project/sql/pull/1771))
* Disable read of plugins.query.datasources.encryption.masterkey from cluster settings GET API ([#1825](https://github.com/opensearch-project/sql/pull/1825))
* Add EMR client to spark connector ([#1790](https://github.com/opensearch-project/sql/pull/1790))
* Improved error codes in case of data source API security exception ([#1753](https://github.com/opensearch-project/sql/pull/1753))
* Remove Default master encryption key from settings ([#1851](https://github.com/opensearch-project/sql/pull/1851))

### Bug Fixes

* Fixed bug of byte/short not handling 0 denominator in divide/modulus equations ([#1716](https://github.com/opensearch-project/sql/pull/1716))
* Fix CSV/RAW output header being application/json rather than plain/text ([#1779](https://github.com/opensearch-project/sql/pull/1779))

### Documentation

* Updated documentation of round function return type ([#1725](https://github.com/opensearch-project/sql/pull/1725))
* Updated `protocol.rst` with new wording for error message ([#1662](https://github.com/opensearch-project/sql/pull/1662))
* Updated documentation for temporal data types ([#1826](https://github.com/opensearch-project/sql/pull/1826))

### Infrastructure

* stopPrometheus task in doctest build.gradle now runs upon project failure in startOpenSearch ([#1747](https://github.com/opensearch-project/sql/pull/1747))
* Bump guava to 32.0.1 ([#1829](https://github.com/opensearch-project/sql/pull/1829))
* Disable CrossClusterSearchIT test ([#1814](https://github.com/opensearch-project/sql/pull/1814))
* Fix flakytest when tests.locale=tr ([#1827](https://github.com/opensearch-project/sql/pull/1827))

### Refactoring

* Simplify OpenSearchIndexScanBuilder ([#1738](https://github.com/opensearch-project/sql/pull/1738))
