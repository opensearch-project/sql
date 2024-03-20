Compatible with OpenSearch and OpenSearch Dashboards Version 2.13.0

### Features

### Enhancements
* Datasource disable feature by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2539
* Handle ALTER Index Queries. by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2554
* Implement vacuum index operation by @dai-chen in https://github.com/opensearch-project/sql/pull/2557
* Stop Streaming Jobs When datasource is disabled/deleted. by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2559

### Bug Fixes
* Fix issue in testSourceMetricCommandWithTimestamp integ test with different timezones and locales. by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2522
* Refactor query param by @noCharger in https://github.com/opensearch-project/sql/pull/2519
* Restrict the scope of cancel API by @penghuo in https://github.com/opensearch-project/sql/pull/2548
* Change async query default setting by @penghuo in https://github.com/opensearch-project/sql/pull/2561
* Percent encode opensearch index name by @seankao-az in https://github.com/opensearch-project/sql/pull/2564
* [Bugfix] Wrap the query with double quotes by @noCharger in https://github.com/opensearch-project/sql/pull/2565
* FlintStreamingJobCleanerTask missing event listener by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2574

### Documentation

### Infrastructure
* bump bwc version by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2546
* [Backport main] Add release notes for 1.3.15 by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/2538
* Upgrade opensearch-spark jars to 0.3.0 by @noCharger in https://github.com/opensearch-project/sql/pull/2568

### Refactoring
* Change emr job names based on the query type by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2543

### Security
* bump ipaddress to 5.4.2 by @joshuali925 in https://github.com/opensearch-project/sql/pull/2544

---
**Full Changelog**: https://github.com/opensearch-project/sql/compare/2.12.0.0...2.13.0.0