## 2021-06-01 Version 1.0.0.0

Compatible with OpenSearch and OpenSearch Dashboards Version 1.0.0

### Enhancements

* Support querying a data stream ([#56](https://github.com/opensearch-project/sql/pull/56))

### OpenSearch Migration

* Remove debug logging in ODBC driver ([#27](https://github.com/opensearch-project/sql/pull/27))
* Update workbench nav category to opensearch ([#28](https://github.com/opensearch-project/sql/pull/28))
* fix opendistro related renaming for sql-cli ([#29](https://github.com/opensearch-project/sql/pull/29))
* Fix issue of workbench not outputting errors ([#32](https://github.com/opensearch-project/sql/pull/32))
* Update issue template with multiple labels ([#34](https://github.com/opensearch-project/sql/pull/34))
* SQL/PPL and JDBC package renaming ([#54](https://github.com/opensearch-project/sql/pull/54))
* Upgrade dependencies to address high severity CVE-2021-20270 ([#61](https://github.com/opensearch-project/sql/pull/61))
* ODBC folder, file and code renaming ([#62](https://github.com/opensearch-project/sql/pull/62))
* Update workbench documentation links, complete renaming ([#67](https://github.com/opensearch-project/sql/pull/67))
* Update sqli-cli documentation links to OpenSearch ([#72](https://github.com/opensearch-project/sql/pull/72))
* Remove opensearch.sql.engine.new.enabled setting ([#70](https://github.com/opensearch-project/sql/pull/70))
* SQL/PPL API endpoint backward compatibility ([#66](https://github.com/opensearch-project/sql/pull/66))
* Remove opensearch.sql.query.analysis.* related settings ([#76](https://github.com/opensearch-project/sql/pull/76))
* Remove opensearch.sql.query.response.format setting ([#77](https://github.com/opensearch-project/sql/pull/77))
* Migrate #1097: Adding support to NOT REGEXP_QUERY ([#79](https://github.com/opensearch-project/sql/pull/79))
* Migrate #1083: Support long literals in SQL/PPL ([#80](https://github.com/opensearch-project/sql/pull/80))
* Change strategy to test connectivity between ODBC driver and SQL plugin ([#69](https://github.com/opensearch-project/sql/pull/69))
* Remove cursor enabling and fetch size setting ([#75](https://github.com/opensearch-project/sql/pull/75))
* Disable DELETE clause by defaut and add opensearch.sql.delete.enabled setting ([#81](https://github.com/opensearch-project/sql/pull/81))
* Support Plugin Settings Backwards Compatibility ([#82](https://github.com/opensearch-project/sql/pull/82))
* Updated icon and background images in ODBC installers ([#84](https://github.com/opensearch-project/sql/pull/84))
* Build SQL/PPL against OpenSearch rc1 and rename artifacts ([#83](https://github.com/opensearch-project/sql/pull/83))
* Support text functions ASCII, LEFT, LOCATE, REPLACE in new engine ([#88](https://github.com/opensearch-project/sql/pull/88))
* Update PowerBI custom connector .mez file for ODBC driver ([#90](https://github.com/opensearch-project/sql/pull/90))
* Rename remaining beta1 references in sql-cli/workbench ([#91](https://github.com/opensearch-project/sql/pull/91))
* Build SQL/PPL against OpenSearch 1.0 branch ([#94](https://github.com/opensearch-project/sql/pull/94))
* Bug Fix: Enable legacy settings in new setting action ([#97](https://github.com/opensearch-project/sql/pull/97))
* Bump OpenSearch Dashboards version to 1.0 in Workbench ([#98](https://github.com/opensearch-project/sql/pull/98))
* Add Integtest.sh for OpenSearch integtest setups ([#128](https://github.com/opensearch-project/sql/pull/128))
* Merge develop into main  ([#142](https://github.com/opensearch-project/sql/pull/142))
* Build against OpenSearch 1.0.0 and bump artifact version to 1.0.0.0 ([#146](https://github.com/opensearch-project/sql/pull/146))

### Documentation

* Migrate SQL/PPL, JDBC, ODBC docs to OpenSearch ([#68](https://github.com/opensearch-project/sql/pull/68))
* Level up README markdown ([#148](https://github.com/opensearch-project/sql/pull/148))

### Infrastructure

* Bump glob-parent from 5.1.1 to 5.1.2 in /workbench ([#125](https://github.com/opensearch-project/sql/pull/125))
