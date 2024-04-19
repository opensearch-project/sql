Compatible with OpenSearch and OpenSearch Dashboards Version 2.13.0

### Enhancements
* Datasource disable feature ([#2539](https://github.com/opensearch-project/sql/pull/2539))
* Handle ALTER Index Queries ([#2554](https://github.com/opensearch-project/sql/pull/2554))
* Implement vacuum index operation ([#2557](https://github.com/opensearch-project/sql/pull/2557))
* Stop Streaming Jobs When datasource is disabled/deleted ([#2559](https://github.com/opensearch-project/sql/pull/2559))

### Bug Fixes
* Fix issue in testSourceMetricCommandWithTimestamp integ test with different timezones and locales ([#2522](https://github.com/opensearch-project/sql/pull/2522))
* Refactor query param ([#2519](https://github.com/opensearch-project/sql/pull/2519))
* bump ipaddress to 5.4.2 ([#2544](https://github.com/opensearch-project/sql/pull/2544))
* Restrict the scope of cancel API ([#2548](https://github.com/opensearch-project/sql/pull/2548))
* Change async query default setting ([#2561](https://github.com/opensearch-project/sql/pull/2561))
* Percent encode opensearch index name ([#2564](https://github.com/opensearch-project/sql/pull/2564))
* [Bugfix] Wrap the query with double quotes ([#2565](https://github.com/opensearch-project/sql/pull/2565))
* FlintStreamingJobCleanerTask missing event listener ([#2574](https://github.com/opensearch-project/sql/pull/2574))

### Infrastructure
* bump bwc version ([#2546](https://github.com/opensearch-project/sql/pull/2546))
* [Backport main] Add release notes for 1.3.15 ([#2538](https://github.com/opensearch-project/sql/pull/2538))
* Upgrade opensearch-spark jars to 0.3.0 ([#2568](https://github.com/opensearch-project/sql/pull/2568))

### Refactoring
* Change emr job names based on the query type ([#2543](https://github.com/opensearch-project/sql/pull/2543))
