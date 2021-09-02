## 2021-09-02 Version 1.1.0.0

Compatible with OpenSearch and OpenSearch Dashboards Version 1.1.0

### Enhancements

* Support implicit type conversion from string to boolean ([#166](https://github.com/opensearch-project/sql/pull/166))
* Support distinct count aggregation ([#167](https://github.com/opensearch-project/sql/pull/167))
* Support implicit type conversion from string to temporal ([#171](https://github.com/opensearch-project/sql/pull/171))
* Workbench: auto dump cypress test data, support security ([#199](https://github.com/opensearch-project/sql/pull/199))

### Bug Fixes

* Fix for SQL-ODBC AWS Init and Shutdown Behaviour ([#163](https://github.com/opensearch-project/sql/pull/163))
* Fix import path for cypress constant ([#201](https://github.com/opensearch-project/sql/pull/201))


### Infrastructure

* Add Integtest.sh for OpenSearch integtest setups (workbench) ([#157](https://github.com/opensearch-project/sql/pull/157))
* Bump path-parse from 1.0.6 to 1.0.7 in /workbench ([#178](https://github.com/opensearch-project/sql/pull/178))
* Use externally-defined OpenSearch version when specified. ([#179](https://github.com/opensearch-project/sql/pull/179))
* Use OpenSearch 1.1 and build snapshot by default in CI. ([#181](https://github.com/opensearch-project/sql/pull/181))
* Workbench: remove curl commands in integtest.sh ([#200](https://github.com/opensearch-project/sql/pull/200))
