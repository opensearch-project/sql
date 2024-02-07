Compatible with OpenSearch and OpenSearch Dashboards Version 2.12.0

### Features


### Enhancements
* add InteractiveSession and SessionManager by @penghuo in https://github.com/opensearch-project/sql/pull/2290
* Add Statement by @penghuo in https://github.com/opensearch-project/sql/pull/2294
* Add sessionId parameters for create async query API by @penghuo in https://github.com/opensearch-project/sql/pull/2312
* Implement patch API for datasources by @derek-ho in https://github.com/opensearch-project/sql/pull/2273
* Integration with REPL Spark job by @penghuo in https://github.com/opensearch-project/sql/pull/2327
* Add missing tags and MV support by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2336
* Bug Fix, support cancel query in running state by @penghuo in https://github.com/opensearch-project/sql/pull/2351
* Add Session limitation by @penghuo in https://github.com/opensearch-project/sql/pull/2354
* Handle Describe,Refresh and Show Queries Properly by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2357
* Add where clause support in create statement by @dai-chen in https://github.com/opensearch-project/sql/pull/2366
* Add Flint Index Purging Logic by @kaituo in https://github.com/opensearch-project/sql/pull/2372
* add concurrent limit on datasource and sessions by @penghuo in https://github.com/opensearch-project/sql/pull/2390
* Redefine Drop Index as logical delete by @penghuo in https://github.com/opensearch-project/sql/pull/2386
* Added session, statement, emrjob metrics to sql stats api by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2398
* Add more metrics and handle emr exception message by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2422
* Add cluster name in spark submit params by @noCharger in https://github.com/opensearch-project/sql/pull/2467
* Add setting plugins.query.executionengine.async_query.enabled by @penghuo in https://github.com/opensearch-project/sql/pull/2510

### Bug Fixes
* Fix bug, using basic instead of basicauth by @penghuo in https://github.com/opensearch-project/sql/pull/2342
* create new session if current session not ready by @penghuo in https://github.com/opensearch-project/sql/pull/2363
* Create new session if client provided session is invalid by @penghuo in https://github.com/opensearch-project/sql/pull/2368
* Enable session by default by @penghuo in https://github.com/opensearch-project/sql/pull/2373
* Return 429 for ConcurrencyLimitExceededException by @penghuo in https://github.com/opensearch-project/sql/pull/2428
* Async query get result bug fix by @dai-chen in https://github.com/opensearch-project/sql/pull/2443
* Validate session with flint datasource passed in async job request by @kaituo in https://github.com/opensearch-project/sql/pull/2448
* Temporary fixes for build errors by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2476
* Add SparkDataType as wrapper for unmapped spark data type by @penghuo in https://github.com/opensearch-project/sql/pull/2492
* Fix wrong 503 error response code by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2493

### Documentation
* [DOC] Configure the Spark metrics properties while creating a s3 Glue Connector by @noCharger in https://github.com/opensearch-project/sql/pull/2504

### Infrastructure
* Onboard jenkins prod docker images in github actions by @peterzhuamazon in https://github.com/opensearch-project/sql/pull/2404
* Add publishToMavenLocal to publish plugins in this script by @zane-neo in https://github.com/opensearch-project/sql/pull/2461
* Update to Gradle 8.4 by @reta in https://github.com/opensearch-project/sql/pull/2433
* Add JDK-21 to GA worklflows by @reta in https://github.com/opensearch-project/sql/pull/2481

### Refactoring
* Refactoring in Unit Tests by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2308
* deprecated job-metadata-index by @penghuo in https://github.com/opensearch-project/sql/pull/2339
* Refactoring for tags usage in test files. by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2383
* Add seder to TransportPPLQueryResponse by @zane-neo in https://github.com/opensearch-project/sql/pull/2452
* Move pplenabled to transport by @zane-neo in https://github.com/opensearch-project/sql/pull/2451
* Async Executor Service Depedencies Refactor by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2488

### Security
* Upgrade JSON to 20231013 to fix CVE-2023-5072 by @derek-ho in https://github.com/opensearch-project/sql/pull/2307
* Block execution engine settings in sql query settings API and add more unit tests by @vamsi-amazon in https://github.com/opensearch-project/sql/pull/2407
* upgrade okhttp to 4.12.0 by @joshuali925 in https://github.com/opensearch-project/sql/pull/2405
* Bump aws-java-sdk-core version to 1.12.651 by @penghuo in https://github.com/opensearch-project/sql/pull/2503

## New Contributors
* @dreamer-89 made their first contribution in https://github.com/opensearch-project/sql/pull/2013
* @kaituo made their first contribution in https://github.com/opensearch-project/sql/pull/2212
* @zane-neo made their first contribution in https://github.com/opensearch-project/sql/pull/2452
* @noCharger made their first contribution in https://github.com/opensearch-project/sql/pull/2467

---
**Full Changelog**: https://github.com/opensearch-project/sql/compare/2.11.0.0...2.12.0.0