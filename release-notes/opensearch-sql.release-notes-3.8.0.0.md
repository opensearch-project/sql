## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Features

* Add PPL `xyseries` command for pivoting row-oriented grouped results into wide tables ([#5343](https://github.com/opensearch-project/sql/pull/5343))
* Add PPL `timewrap` command for time-period comparison over timechart output ([#5241](https://github.com/opensearch-project/sql/pull/5241))
* Add PPL `foreach` command for iterating over field lists, multivalue fields, and JSON arrays ([#5613](https://github.com/opensearch-project/sql/pull/5613))
* Add PPL `makeresults` command for generating in-memory rows without an index scan ([#5622](https://github.com/opensearch-project/sql/pull/5622))

### Enhancements

* Anonymize `xyseries` command and mark it as experimental in documentation ([#5643](https://github.com/opensearch-project/sql/pull/5643))
* Suggest similar field names in 'field not found' error messages ([#5402](https://github.com/opensearch-project/sql/pull/5402))
* Support `constant_keyword` field type in PPL, treating it as a string ([#5639](https://github.com/opensearch-project/sql/pull/5639))
* Decouple Calcite PPL planning from ExprType, operating on RelDataType directly ([#5633](https://github.com/opensearch-project/sql/pull/5633))
* Support bare-field join criteria shorthand (`join on <field>`) in PPL ([#5517](https://github.com/opensearch-project/sql/pull/5517))
* Classify unsupported-feature errors as client errors (4xx) on the SQL path ([#5569](https://github.com/opensearch-project/sql/pull/5569))
* Reject unsupported output formats on the analytics-engine route with a 4xx error ([#5570](https://github.com/opensearch-project/sql/pull/5570))
* Widen narrow integer operands in PPL arithmetic to prevent silent overflow ([#5603](https://github.com/opensearch-project/sql/pull/5603))
* Add configurable expression depth limit during AST building to prevent stack overflow ([#5602](https://github.com/opensearch-project/sql/pull/5602))
* Add `json_tree` machine-readable explain format accessible via `_explain?format=json_tree` ([#5576](https://github.com/opensearch-project/sql/pull/5576))
* Onboard new backport-pr reusable GitHub workflow ([#5586](https://github.com/opensearch-project/sql/pull/5586))
* Return all columns including struct and nested fields when using `head` command ([#5518](https://github.com/opensearch-project/sql/pull/5518))
* Bring `CalcitePPLBasicIT` to parity on the analytics-engine route ([#5542](https://github.com/opensearch-project/sql/pull/5542))
* Bring `CalciteWhereCommandIT` to parity on the analytics-engine route ([#5546](https://github.com/opensearch-project/sql/pull/5546))
* Stabilize order-dependent PPL ITs with explicit sort for multi-shard analytics runs ([#5537](https://github.com/opensearch-project/sql/pull/5537))
* Align `DateTimeComparisonIT` today's date computation to UTC for analytics-engine compatibility ([#5543](https://github.com/opensearch-project/sql/pull/5543))
* Fix NPE on `case()` with incompatible branch types, returning a clean 400 error ([#5575](https://github.com/opensearch-project/sql/pull/5575))
* Fix NPE when `rex` sits inside `appendcol` subsearch for the analytics engine ([#5574](https://github.com/opensearch-project/sql/pull/5574))

### Bug Fixes

* Fix `ClassCastException` in PPL multisearch on indexes with `@timestamp` alias field ([#5577](https://github.com/opensearch-project/sql/pull/5577))
* Fix PPL `foreach` JSON array type coercion to handle non-numeric elements gracefully ([#5637](https://github.com/opensearch-project/sql/pull/5637))
* Detect long (BIGINT) arithmetic overflow instead of silently wrapping ([#5604](https://github.com/opensearch-project/sql/pull/5604))
* Preserve SQL-layer profiling alongside the analytics-engine profile ([#5571](https://github.com/opensearch-project/sql/pull/5571))
* Propagate request-task cancellation into the analytics PPL route ([#5563](https://github.com/opensearch-project/sql/pull/5563))
* Return 4xx instead of 500 for unsupported window functions ([#5587](https://github.com/opensearch-project/sql/pull/5587))
* Fix `SHOW`/`DESCRIBE` statement routing under `cluster.pluggable.dataformat` setting ([#5528](https://github.com/opensearch-project/sql/pull/5528))
* Handle opaque `NullPointerException` for unresolvable alias-type field path with a clear error ([#5536](https://github.com/opensearch-project/sql/pull/5536))
* Fix invalid field or index error misclassified as internal 500 failures ([#5532](https://github.com/opensearch-project/sql/pull/5532))
* Fix `GROUP BY` expression resolution in `SELECT`/`HAVING`/`ORDER BY` ([#5548](https://github.com/opensearch-project/sql/pull/5548))
* Fix SQL window functions with `ORDER BY`/`LIMIT` on unified query path ([#5592](https://github.com/opensearch-project/sql/pull/5592))
* Fix dedup field name mapping to handle alias collision when rename and eval resolve to the same source field ([#5593](https://github.com/opensearch-project/sql/pull/5593))
* Allow partial pushdown for semi-scripted predicates so pushable filters are not blocked by unsupported ones ([#5565](https://github.com/opensearch-project/sql/pull/5565))
* Gracefully handle malformed documents in result scanning instead of crashing ([#5618](https://github.com/opensearch-project/sql/pull/5618))
* Honor PPL `fetch_size` on the analytics-engine route ([#5567](https://github.com/opensearch-project/sql/pull/5567))
* Strip analytics-engine-unsupported fields from test data and exclude affected ITs ([#5541](https://github.com/opensearch-project/sql/pull/5541))
* Repair two pre-existing IT failures on main (error type assertion and explain flake) ([#5545](https://github.com/opensearch-project/sql/pull/5545))
* Revert PPL `rest` command ([#5635](https://github.com/opensearch-project/sql/pull/5635))

### Infrastructure

* Bring `CalciteBinCommandIT` and `CalciteMultisearchCommandIT` to parity on the analytics-engine route ([#5551](https://github.com/opensearch-project/sql/pull/5551))
* Bring `CalcitePPLEnhancedCoalesceIT` to parity on the analytics-engine route ([#5552](https://github.com/opensearch-project/sql/pull/5552))
* Bring `CalcitePPLJoinIT` to parity on the analytics-engine route ([#5554](https://github.com/opensearch-project/sql/pull/5554))
* Stabilize `CalcitePPLConditionBuiltinFunctionIT` on the analytics-engine route ([#5556](https://github.com/opensearch-project/sql/pull/5556))
* Stabilize `CalciteStreamstatsCommandIT` on the analytics-engine route ([#5582](https://github.com/opensearch-project/sql/pull/5582))
* Stabilize PPL ITs on the analytics-engine route (array/map-path/datatype/basic) ([#5562](https://github.com/opensearch-project/sql/pull/5562))
* Stabilize PPL ITs on the analytics-engine route (case/string/full-text/like/appendpipe) ([#5561](https://github.com/opensearch-project/sql/pull/5561))
* Stabilize PPL ITs on the analytics-engine route (percentile/float/datetime/json/dedup/union/rename/chart) ([#5564](https://github.com/opensearch-project/sql/pull/5564))
* Stabilize PPL ITs on the analytics-engine route (sort/streamstats/IP-UDT/metadata/strip-verifier) ([#5566](https://github.com/opensearch-project/sql/pull/5566))
* Stabilize subquery PPL ITs on the analytics-engine route ([#5555](https://github.com/opensearch-project/sql/pull/5555))
* Recover concrete schema type for ANY-typed columns on the analytics route (fixes eval max/min) ([#5557](https://github.com/opensearch-project/sql/pull/5557))
* Fix SQL IT test queries, assertions, and data for engine-agnostic compatibility ([#5584](https://github.com/opensearch-project/sql/pull/5584))
* Gate analytics-engine incompatible IT tests with capability matrix annotations ([#5585](https://github.com/opensearch-project/sql/pull/5585))
* Decouple IT from execution backend with capability-based gating ([#5560](https://github.com/opensearch-project/sql/pull/5560))
* Fix doctest job-scheduler dependency resolution for 3.8.0 ([#5540](https://github.com/opensearch-project/sql/pull/5540))
* Bump Apache Calcite 1.41.0 → 1.42.0 (CVE-2026-46718) ([#5619](https://github.com/opensearch-project/sql/pull/5619))
* Bump `get-ci-image-tag.yml` ref to SHA-pinned opensearch-build commit to unblock CI ([#5583](https://github.com/opensearch-project/sql/pull/5583))
* Case test patches for missed optimizations ([#5531](https://github.com/opensearch-project/sql/pull/5531))
* Use engine-zone today in `DateTimeFunctionIT` now()-based assertions ([#5553](https://github.com/opensearch-project/sql/pull/5553))
* Update datetime tests to stay within analytics-engine epoch bounds ([#5534](https://github.com/opensearch-project/sql/pull/5534))

### Maintenance

* Fix flaky TPC-H Q15 floating-point assertion ([#5629](https://github.com/opensearch-project/sql/pull/5629))
* Fix lychee link checker ([#5451](https://github.com/opensearch-project/sql/pull/5451))
