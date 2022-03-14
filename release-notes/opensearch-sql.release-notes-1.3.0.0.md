### Version 1.3.0.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards Version 1.3.0

### Features
* Add parse command to PPL ([#411](https://github.com/opensearch-project/sql/pull/411))
* PPL integration with AD and ml-commons ([#468](https://github.com/opensearch-project/sql/pull/468))

### Enhancements
* Support ISO 8601 Format in Date Format. ([#460](https://github.com/opensearch-project/sql/pull/460))
* Add Certificate Validation option ([#449](https://github.com/opensearch-project/sql/pull/449))
* Span expression should always be first in by list if exist ([#437](https://github.com/opensearch-project/sql/pull/437))
* Support multiple indices in PPL and SQL ([#408](https://github.com/opensearch-project/sql/pull/408))
* Support combination of group field and span in stats command ([#417](https://github.com/opensearch-project/sql/pull/417))
* Support In clause in SQL and PPL ([#420](https://github.com/opensearch-project/sql/pull/420))
* Add cast function to PPL ([#433](https://github.com/opensearch-project/sql/pull/433))
* [Enhancement] optimize sort rewrite logic ([#434](https://github.com/opensearch-project/sql/pull/434))

### Bug Fixes
* Fix certificate validation for ODBC driver ([#479](https://github.com/opensearch-project/sql/pull/479))
* Update dependency opensearch-ml-client group name ([#477](https://github.com/opensearch-project/sql/pull/477))
* Treating ExpressionEvaluationException as client Error. ([#459](https://github.com/opensearch-project/sql/pull/459))
* Version Bump: H2 1.x -> 2.x ([#444](https://github.com/opensearch-project/sql/pull/444))
* Version Bump: springframework and jackson ([#443](https://github.com/opensearch-project/sql/pull/443))
* Bug Fix, disable html escape when formatting response ([#412](https://github.com/opensearch-project/sql/pull/412))
* Jackson-databind bump to 2.12.6 ([#410](https://github.com/opensearch-project/sql/pull/410))
* Parse none type field as null instead of throw exception ([#406](https://github.com/opensearch-project/sql/pull/406))

### Documentation
* Add parse docs to PPL commands index ([#486](https://github.com/opensearch-project/sql/pull/486))
* Add limitation section in PPL docs ([#456](https://github.com/opensearch-project/sql/pull/456))
* Add how to setup aws credentials for ODBC Tableau ([#394](https://github.com/opensearch-project/sql/pull/394))

### Maintenance
* Add JDK 8 to CI Matrix  ([#483](https://github.com/opensearch-project/sql/pull/483))
* Add CI Matrix for JDK 11 and 14 ([#451](https://github.com/opensearch-project/sql/pull/451))
* Update backport and add auto-delete workflows ([#446](https://github.com/opensearch-project/sql/pull/446))
* Add auto backport functionality for SQL ([#445](https://github.com/opensearch-project/sql/pull/445))
* Version bump to 1.3 ([#419](https://github.com/opensearch-project/sql/pull/419))
* Revert to windows 2019 for odbc CI ([#413](https://github.com/opensearch-project/sql/pull/413))

### Infrastructure
* Disable flaky test in JdbcTestIT. ([#475](https://github.com/opensearch-project/sql/pull/475))