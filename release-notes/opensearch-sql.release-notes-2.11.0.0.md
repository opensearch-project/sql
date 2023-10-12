Compatible with OpenSearch and OpenSearch Dashboards Version 2.11.0

### Features

### Enhancements
*  Enable PPL lang and add datasource to async query API in https://github.com/opensearch-project/sql/pull/2195
*  Refactor Flint Auth in https://github.com/opensearch-project/sql/pull/2201
*  Add conf for spark structured streaming job in https://github.com/opensearch-project/sql/pull/2203
*  Submit long running job only when auto_refresh = false in https://github.com/opensearch-project/sql/pull/2209
*  Bug Fix, handle DESC TABLE response in https://github.com/opensearch-project/sql/pull/2213
*  Drop Index Implementation in https://github.com/opensearch-project/sql/pull/2217
*  Enable PPL Queries in https://github.com/opensearch-project/sql/pull/2223
*  Read extra Spark submit parameters from cluster settings in https://github.com/opensearch-project/sql/pull/2236
*  Spark Execution Engine Config Refactor in https://github.com/opensearch-project/sql/pull/2266
*  Provide auth.type and auth.role_arn paramters in GET Datasource API response. in https://github.com/opensearch-project/sql/pull/2283
*  Add support for `date_nanos` and tests. (#337) in https://github.com/opensearch-project/sql/pull/2020
*  Applied formatting improvements to Antlr files based on spotless changes (#2017) by @MitchellGale in https://github.com/opensearch-project/sql/pull/2023
*  Revert "Guarantee datasource read api is strong consistent read (#1815)" in https://github.com/opensearch-project/sql/pull/2031
*  Add _primary preference only for segment replication enabled indices in https://github.com/opensearch-project/sql/pull/2045
*  Changed allowlist config to denylist ip config for datasource uri hosts in https://github.com/opensearch-project/sql/pull/2058

### Bug Fixes
*  fix broken link for connectors doc in https://github.com/opensearch-project/sql/pull/2199
*  Fix response codes returned by JSON formatting them in https://github.com/opensearch-project/sql/pull/2200
*  Bug fix, datasource API should be case sensitive in https://github.com/opensearch-project/sql/pull/2202
*  Minor fix in dropping covering index in https://github.com/opensearch-project/sql/pull/2240
*  Fix Unit tests for FlintIndexReader in https://github.com/opensearch-project/sql/pull/2242
*  Bug Fix , delete OpenSearch index when DROP INDEX in https://github.com/opensearch-project/sql/pull/2252
*  Correctly Set query status in https://github.com/opensearch-project/sql/pull/2232
*  Exclude generated files from spotless  in https://github.com/opensearch-project/sql/pull/2024
*  Fix mockito core conflict. in https://github.com/opensearch-project/sql/pull/2131
*  Fix `ASCII` function and groom UT for text functions. (#301) in https://github.com/opensearch-project/sql/pull/2029
*  Fixed response codes For Requests With security exception. in https://github.com/opensearch-project/sql/pull/2036

### Documentation
*  Datasource description in https://github.com/opensearch-project/sql/pull/2138
*  Add documentation for S3GlueConnector. in https://github.com/opensearch-project/sql/pull/2234

### Infrastructure
*  bump aws-encryption-sdk-java to 1.71 in https://github.com/opensearch-project/sql/pull/2057
*  Run IT tests with security plugin (#335) #1986 by @MitchellGale in https://github.com/opensearch-project/sql/pull/2022

### Refactoring
*  Merging Async Query APIs feature branch into main. in https://github.com/opensearch-project/sql/pull/2163
*  Removed Domain Validation in https://github.com/opensearch-project/sql/pull/2136
*  Check for existence of security plugin in https://github.com/opensearch-project/sql/pull/2069
*  Always use snapshot version for security plugin download in https://github.com/opensearch-project/sql/pull/2061
*  Add customized result index in data source etc in https://github.com/opensearch-project/sql/pull/2220

### Security
*  bump okhttp to 4.10.0 (#2043) by @joshuali925 in https://github.com/opensearch-project/sql/pull/2044
*  bump okio to 3.4.0 by @joshuali925 in https://github.com/opensearch-project/sql/pull/2047

---
**Full Changelog**: https://github.com/opensearch-project/sql/compare/2.3.0.0...v.2.11.0.0