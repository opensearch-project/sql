Compatible with OpenSearch and OpenSearch Dashboards Version 2.14.0

### Enhancements
* Add iceberg support to EMR serverless jobs. ([#2602](https://github.com/opensearch-project/sql/pull/2602))
* Use EMR serverless bundled iceberg JAR. ([#2646](https://github.com/opensearch-project/sql/pull/2646))

### Bug Fixes
* Align vacuum statement semantics with Flint Spark ([#2606](https://github.com/opensearch-project/sql/pull/2606))
* Handle EMRS exception as 400 ([#2612](https://github.com/opensearch-project/sql/pull/2612))
* Fix pagination for many columns (#2440) ([#2441](https://github.com/opensearch-project/sql/pull/2441))
* Fix semicolon parsing for async query ([#2631](https://github.com/opensearch-project/sql/pull/2631))
* Throw OpensearchSecurityException in case of datasource authorization ([#2626](https://github.com/opensearch-project/sql/pull/2626))

### Maintenance
* Refactoring of SparkQueryDispatcher  ([#2615](https://github.com/opensearch-project/sql/pull/2615))

### Infrastructure
* Increment version to 2.14.0-SNAPSHOT ([#2585](https://github.com/opensearch-project/sql/pull/2585))