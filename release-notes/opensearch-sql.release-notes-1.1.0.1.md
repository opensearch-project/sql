## 2021-10-28 Version 1.1.0.1

Compatible with OpenSearch and OpenSearch Dashboards Version 1.1.0

### Enhancements
* Support match function as filter in SQL and PPL ([204](https://github.com/opensearch-project/sql/pull/240))
* Renamed plugin helper config file to consistent name with OSD ([#228](https://github.com/opensearch-project/sql/pull/228))
* Support ODBC compatibility with ODFE SQL ([#238](https://github.com/opensearch-project/sql/pull/238))
* Support span aggregation in query engine ([#220](https://github.com/opensearch-project/sql/pull/220))

### Bug Fixes
* Fix PPL request concurrency handling issue ([#207](https://github.com/opensearch-project/sql/pull/207))
* Changed the ODBC mac installer build platform to MacOS 10.15 from latest ([#230](https://github.com/opensearch-project/sql/pull/230))

### Infrastructure
* Removed integtest.sh. ([#208](https://github.com/opensearch-project/sql/pull/208))
* Update build to use public Maven repo ([#225](https://github.com/opensearch-project/sql/pull/225))
* Address security vulnerability CVE-2021-3795 ([#231](https://github.com/opensearch-project/sql/pull/231))
* Addressed security vulnerability CVE-2021-3807 ([#233](https://github.com/opensearch-project/sql/pull/233))
* Enable DCO Workflow Check ([#242](https://github.com/opensearch-project/sql/pull/242))

