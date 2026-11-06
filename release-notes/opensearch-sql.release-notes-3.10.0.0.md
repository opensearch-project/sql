## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Support `flat_object` field type in PPL queries ([#5830](https://github.com/opensearch-project/sql/pull/5830))
* Add asynchronous PPL query submission and result retrieval ([#5832](https://github.com/opensearch-project/sql/pull/5832))
* Add cancellation and deletion support for async PPL queries ([#5846](https://github.com/opensearch-project/sql/pull/5846))
* Add language-neutral query job lifecycle framework ([#5818](https://github.com/opensearch-project/sql/pull/5818))

### Enhancements

* Fail PPL queries with HTTP 429 when the SQL worker pool is saturated instead of silently dropping them ([#5915](https://github.com/opensearch-project/sql/pull/5915))
* Support Unicode string literals in the Calcite PPL engine ([#5774](https://github.com/opensearch-project/sql/pull/5774))
* Support data streams and implicit time fields in index pruning ([#5908](https://github.com/opensearch-project/sql/pull/5908))
* Prune indices by request-level time bounds before the schema is resolved ([#5766](https://github.com/opensearch-project/sql/pull/5766))
* Raise default structural limits for the deserialization filter ([#5826](https://github.com/opensearch-project/sql/pull/5826))
* Report progress for asynchronous Calcite PPL queries ([#5910](https://github.com/opensearch-project/sql/pull/5910))
* Bound PPL `parse`/`rex` regex matching work with a configurable match limit ([#5921](https://github.com/opensearch-project/sql/pull/5921))
* Make the `flat_object` read depth cap configurable ([#5861](https://github.com/opensearch-project/sql/pull/5861))
* Optimize nested field removal to O(n), reducing query time from 40s to 600ms on large schemas ([#5913](https://github.com/opensearch-project/sql/pull/5913))
* Restore `SUM(narrow_int + literal)` aggregation pushdown for improved performance ([#5827](https://github.com/opensearch-project/sql/pull/5827))

### Bug Fixes

* Fix HTTP 500 from `coalesce`, `mvappend`, `array`, and `percentile_approx` on `short`, `byte`, and `float` values ([#5874](https://github.com/opensearch-project/sql/pull/5874))
* Fix HTTP 500 from `sort` and `stats` on `object`, `nested`, `geo_point`, and `flat_object` fields ([#5858](https://github.com/opensearch-project/sql/pull/5858))
* Fix HTTP 500 and empty results from `binary` fields in Calcite pushdowns ([#5767](https://github.com/opensearch-project/sql/pull/5767))
* Fix HTTP 500 from `in`, `between`, `case`, and `coalesce` on `object` and `nested` fields ([#5870](https://github.com/opensearch-project/sql/pull/5870))
* Fix comma-separated sources failing with request-level time bounds ([#5852](https://github.com/opensearch-project/sql/pull/5852))
* Fix status code mislabeling in legacy engine responses ([#5850](https://github.com/opensearch-project/sql/pull/5850))
* Fix `count`/`dc` failing after a `fields` command that narrows the row ([#5783](https://github.com/opensearch-project/sql/pull/5783))
* Fix `expand` on non-column values reporting a `ClassCastException` instead of a user-friendly error ([#5841](https://github.com/opensearch-project/sql/pull/5841))
* Fix empty PPL query returning HTTP 500 instead of 400 ([#5868](https://github.com/opensearch-project/sql/pull/5868))
* Fix empty SQL query returning HTTP 500 instead of 400 ([#5880](https://github.com/opensearch-project/sql/pull/5880))
* Keep metadata fields hidden per input of `append`, `appendpipe`, `appendcol`, `union`, and `lookup` ([#5884](https://github.com/opensearch-project/sql/pull/5884))
* Fix node crash when PPL alerting monitor triggers due to missing `opensearch` module class ([#5924](https://github.com/opensearch-project/sql/pull/5924))
* Register background search tasks as children of parent to prevent zombie tasks on cancellation ([#5888](https://github.com/opensearch-project/sql/pull/5888))
* Resolve cross-cluster index schemas through the remote cluster's GetMappings API ([#5844](https://github.com/opensearch-project/sql/pull/5844))

### Infrastructure

* Add OpenTelemetry logs and traces data stream fixtures for mapping conflict integration tests ([#5831](https://github.com/opensearch-project/sql/pull/5831))
* Fix index pruning YAML test flakiness ([#5813](https://github.com/opensearch-project/sql/pull/5813))
* Report unit test coverage across modules using aggregated JaCoCo reports ([#5891](https://github.com/opensearch-project/sql/pull/5891))
* Split out CI assemble and lint steps for better caching and clarity ([#5365](https://github.com/opensearch-project/sql/pull/5365))
* Add `runWithSecurity` Gradle task for local development ([#5878](https://github.com/opensearch-project/sql/pull/5878))
* Run `AnalyticsEngineSecurityIT`/`ProfileIT` in CI and fix drifted AE plugin download URLs ([#5782](https://github.com/opensearch-project/sql/pull/5782))

### Documentation

* Improve `lookup` command documentation and fix doc loading bug ([#5824](https://github.com/opensearch-project/sql/pull/5824))

### Maintenance

* Bump Guava from 33.3.0-jre to 33.7.2-jre to address CVE-2026-102554 ([#5918](https://github.com/opensearch-project/sql/pull/5918))
* Fix flaky `CalciteFlattenCommandIT.testFlattenNullField` under non-Latin locales ([#5793](https://github.com/opensearch-project/sql/pull/5793))
* Remove the opt-in partial-result mode for mapping conflicts ([#5814](https://github.com/opensearch-project/sql/pull/5814))
