Compatible with OpenSearch and OpenSearch Dashboards Version 2.6.0

### Enhancements

* Extend comparison methods to accept different datetime types ([#1196](https://github.com/opensearch-project/sql/pull/1196))
* Enable concat() string function to support multiple string arguments ([#1279](https://github.com/opensearch-project/sql/pull/1279))
* Add more keywords as identifier in PPL ([#1319](https://github.com/opensearch-project/sql/pull/1319))
* Update DATE_ADD/ADDDATE and DATE_SUB/SUBDATE functions ([#1182](https://github.com/opensearch-project/sql/pull/1182))
* Escape character support for string literals ([#1206](https://github.com/opensearch-project/sql/pull/1206))
* Updated EXPM1() and Tests to New Engine ([#1334](https://github.com/opensearch-project/sql/pull/1334))
* Update TIMESTAMP function implementation and signatures ([#1254](https://github.com/opensearch-project/sql/pull/1254))
* Add GET_FORMAT Function To OpenSearch SQL Plugin ([#1299](https://github.com/opensearch-project/sql/pull/1299))
* Add TIME_FORMAT() Function To SQL Plugin ([#1301](https://github.com/opensearch-project/sql/pull/1301))
* Support More Formats For GET_FORMAT Function ([#1343](https://github.com/opensearch-project/sql/pull/1343))
* Add last_day Function To OpenSearch SQL Plugin ([#1344](https://github.com/opensearch-project/sql/pull/1344))
* Add WeekOfYear Function To OpenSearch ([#1345](https://github.com/opensearch-project/sql/pull/1345))

### Bug Fixes

* Allow literal in aggregation ([#1288](https://github.com/opensearch-project/sql/pull/1288))
* Datetime aggregation fixes ([#1061](https://github.com/opensearch-project/sql/pull/1061))
* Modified returning NaN to NULL ([#1341](https://github.com/opensearch-project/sql/pull/1341))
* Fix index not found reported as server error bug ([#1353](https://github.com/opensearch-project/sql/pull/1353))

### Infrastructure

* Upgrade sqlite to 3.32.3.3 ([#1283](https://github.com/opensearch-project/sql/pull/1283))
* Add publish snapshots to maven via GHA ([#1359](https://github.com/opensearch-project/sql/pull/1359))
* Added untriaged issue workflow ([#1338](https://github.com/opensearch-project/sql/pull/1338))
* Create custom integ test file for sql plugin ([#1330](https://github.com/opensearch-project/sql/pull/1330))
* Fix IT according to OpenSearch changes ([#1326](https://github.com/opensearch-project/sql/pull/1326))
* Fix ArgumentCaptor can't capture varargs ([#1320](https://github.com/opensearch-project/sql/pull/1320))
* Added Correctness Tests For Date And Time Functions ([#1298](https://github.com/opensearch-project/sql/pull/1298))
* Update usage of Strings.toString ([#1309](https://github.com/opensearch-project/sql/pull/1309))
* Update link checker CI workflow. ([#1304](https://github.com/opensearch-project/sql/pull/1304))
* Add micro benchmark by JMH ([#1278](https://github.com/opensearch-project/sql/pull/1278))
* Move PiTest to a new workflow. ([#1285](https://github.com/opensearch-project/sql/pull/1285))
* Adding mutation testing to build gradle with PiTest ([#1204](https://github.com/opensearch-project/sql/pull/1204))

### Documentation
* Reorganize development docs ([#1200](https://github.com/opensearch-project/sql/pull/1200))
* Remove obsolete links from README ([#1303](https://github.com/opensearch-project/sql/pull/1303))
