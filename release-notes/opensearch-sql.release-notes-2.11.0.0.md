Compatible with OpenSearch and OpenSearch Dashboards Version 2.10.0

### Features

### Enhancements
* [Backport 2.x] Enable PPL lang and add datasource to async query API in https://github.com/opensearch-project/sql/pull/2195
* [Backport 2.x] Refactor Flint Auth in https://github.com/opensearch-project/sql/pull/2201
* [Backport 2.x] Add conf for spark structured streaming job in https://github.com/opensearch-project/sql/pull/2203
* [Backport 2.x] Submit long running job only when auto_refresh = false in https://github.com/opensearch-project/sql/pull/2209
* [Backport 2.x] Bug Fix, handle DESC TABLE response in https://github.com/opensearch-project/sql/pull/2213
* [Backport 2.x] Drop Index Implementation in https://github.com/opensearch-project/sql/pull/2217
* [Backport 2.x] Enable PPL Queries in https://github.com/opensearch-project/sql/pull/2223
* [Backport 2.11] Read extra Spark submit parameters from cluster settings in https://github.com/opensearch-project/sql/pull/2236
* [Backport 2.11] Spark Execution Engine Config Refactor in https://github.com/opensearch-project/sql/pull/2266
* [Backport 2.11] Provide auth.type and auth.role_arn paramters in GET Datasource API response. in https://github.com/opensearch-project/sql/pull/2283
* [Backport 2.x] Add support for `date_nanos` and tests. (#337) in https://github.com/opensearch-project/sql/pull/2020
* [Backport 2.x] Applied formatting improvements to Antlr files based on spotless changes (#2017) by @MitchellGale in https://github.com/opensearch-project/sql/pull/2023
* [Backport 2.x] Revert "Guarantee datasource read api is strong consistent read (#1815)" in https://github.com/opensearch-project/sql/pull/2031
* [Backport 2.x] Add _primary preference only for segment replication enabled indices in https://github.com/opensearch-project/sql/pull/2045
* [Backport 2.x] Changed allowlist config to denylist ip config for datasource uri hosts in https://github.com/opensearch-project/sql/pull/2058

### Bug Fixes
* [Backport 2.x] fix broken link for connectors doc in https://github.com/opensearch-project/sql/pull/2199
* [Backport 2.x] Fix response codes returned by JSON formatting them in https://github.com/opensearch-project/sql/pull/2200
* [Backport 2.x] Bug fix, datasource API should be case sensitive in https://github.com/opensearch-project/sql/pull/2202
* [Backport 2.11] Minor fix in dropping covering index in https://github.com/opensearch-project/sql/pull/2240
* [Backport 2.11] Fix Unit tests for FlintIndexReader in https://github.com/opensearch-project/sql/pull/2242
* [Backport 2.11] Bug Fix , delete OpenSearch index when DROP INDEX in https://github.com/opensearch-project/sql/pull/2252
* [Backport 2.11] Correctly Set query status in https://github.com/opensearch-project/sql/pull/2232
* [Backport 2.x] Exclude generated files from spotless  in https://github.com/opensearch-project/sql/pull/2024
* [Backport 2.x] Fix mockito core conflict. in https://github.com/opensearch-project/sql/pull/2131
* [Backport 2.x] Fix `ASCII` function and groom UT for text functions. (#301) in https://github.com/opensearch-project/sql/pull/2029
* [Backport 2.x] Fixed response codes For Requests With security exception. in https://github.com/opensearch-project/sql/pull/2036

### Documentation
* [Backport 2.x] Datasource description in https://github.com/opensearch-project/sql/pull/2138
* [Backport 2.11] Add documentation for S3GlueConnector. in https://github.com/opensearch-project/sql/pull/2234

### Infrastructure
* [Backport 2.x] bump aws-encryption-sdk-java to 1.71 in https://github.com/opensearch-project/sql/pull/2057
* [Backport 2.x] Run IT tests with security plugin (#335) #1986 by @MitchellGale in https://github.com/opensearch-project/sql/pull/2022

### Refactoring
* [Backport 2.x] Merging Async Query APIs feature branch into main. in https://github.com/opensearch-project/sql/pull/2163
* [Backport 2.x] Removed Domain Validation in https://github.com/opensearch-project/sql/pull/2136
* [Backport 2.x] Check for existence of security plugin in https://github.com/opensearch-project/sql/pull/2069
* [Backport 2.x] Always use snapshot version for security plugin download in https://github.com/opensearch-project/sql/pull/2061
* [Backport 2.x] Add customized result index in data source etc in https://github.com/opensearch-project/sql/pull/2220

### Security
* [2.x] bump okhttp to 4.10.0 (#2043) by @joshuali925 in https://github.com/opensearch-project/sql/pull/2044
* [2.x] bump okio to 3.4.0 by @joshuali925 in https://github.com/opensearch-project/sql/pull/2047

---
**Full Changelog**: https://github.com/opensearch-project/sql/compare/2.3.0.0...v.2.11.0.0