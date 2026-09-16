## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Features

* Add SQL `histogram` and `date_histogram` bucket functions ([#5700](https://github.com/opensearch-project/sql/pull/5700))
* Add a generic, extensible REST endpoint provider SPI for the PPL `rest` command ([#5656](https://github.com/opensearch-project/sql/pull/5656))
* Add `include_metadata` request parameter for PPL queries to include `_id`, `_index`, `_score`, and other metadata fields ([#5412](https://github.com/opensearch-project/sql/pull/5412))
* Add `percentfield` and `showperc` options to `top` and `rare` PPL commands ([#5642](https://github.com/opensearch-project/sql/pull/5642))
* Support `RANK()` and `DENSE_RANK()` window functions in unified SQL ([#5720](https://github.com/opensearch-project/sql/pull/5720))
* Support `UNION` (distinct) for unified SQL on the analytics-engine route ([#5741](https://github.com/opensearch-project/sql/pull/5741))
* Add a dedicated thread pool for complex/slow PPL queries to keep the cluster responsive ([#5628](https://github.com/opensearch-project/sql/pull/5628))

### Enhancements

* Add opt-in partial-result mode for aggregations on text/keyword mapping conflicts, returning fast partial answers with warnings instead of slow full scans ([#5657](https://github.com/opensearch-project/sql/pull/5657))
* Avoid PIT context exhaustion by pruning indices that cannot match the query's time range before opening a PIT ([#5727](https://github.com/opensearch-project/sql/pull/5727))
* Enable index pruning (`plugins.query.pruning.enabled`) by default ([#5759](https://github.com/opensearch-project/sql/pull/5759))
* Surface PIT-context exhaustion with an actionable error message naming the `search.max_open_pit_context` setting and remediation steps ([#5631](https://github.com/opensearch-project/sql/pull/5631))
* Map Calcite `ROW` type to `STRUCT` so analytics-engine object fields report the correct type instead of `unknown` ([#5737](https://github.com/opensearch-project/sql/pull/5737))
* Type nested field access as the field's own type instead of the whole row, fixing `Unsupported conversion for Relational Data type: ROW` on the analytics engine ([#5764](https://github.com/opensearch-project/sql/pull/5764))
* Override the `profile` endpoint with an `analyze` endpoint providing operator tree and rule-based query optimization recommendations ([#5568](https://github.com/opensearch-project/sql/pull/5568))
* Clean up the `analyze` endpoint response and add rule-based recommendations ([#5658](https://github.com/opensearch-project/sql/pull/5658))
* Register missing `TAN` function for the Calcite and analytics-engine paths ([#5717](https://github.com/opensearch-project/sql/pull/5717))
* Add structural limits (`maxdepth`, `maxrefs`, `maxbytes`) for the deserialization filter, configurable via dynamic cluster settings ([#5721](https://github.com/opensearch-project/sql/pull/5721))
* Forward cluster planning settings (e.g. `plugins.query.size_limit`, pattern settings) to the analytics-engine unified query path ([#5611](https://github.com/opensearch-project/sql/pull/5611))
* Reject object and array fields in `timechart`/`chart` split and `cast` expressions with a clear 400 error instead of a 500 plan dump ([#5751](https://github.com/opensearch-project/sql/pull/5751))
* Add PPL OpenTelemetry tracing integration across the Calcite query execution pipeline ([#5708](https://github.com/opensearch-project/sql/pull/5708))
* Push down aggregation on text fields without a `.keyword` sub-field using a Calcite script that reads from `_source` ([#5646](https://github.com/opensearch-project/sql/pull/5646))

### Bug Fixes

* Accept plain Calcite types against UDT operand signatures, fixing false type-mismatch errors for `list()`, `values()`, and similar aggregations on date/time/IP/binary fields ([#5675](https://github.com/opensearch-project/sql/pull/5675))
* Carry the `partial_result` per-request override off `ThreadContext` so it survives the security plugin's thread handoff ([#5758](https://github.com/opensearch-project/sql/pull/5758))
* Detect `BIGINT` overflow in `SUM` and fix wrong `AVG` results on the Calcite engine ([#5612](https://github.com/opensearch-project/sql/pull/5612))
* Fix `IllegalStateException` in `UnifiedQueryPlanner.preserveCollation` on plans with composite (multi-key) collations ([#5650](https://github.com/opensearch-project/sql/pull/5650))
* Fix PPL `search` command ignoring wildcards and over-matching quoted values ([#5697](https://github.com/opensearch-project/sql/pull/5697))
* Fix `dedup` 500 error on cross-index object/scalar mapping conflicts by gracefully degrading scalar-under-object values to null ([#5732](https://github.com/opensearch-project/sql/pull/5732))
* Fix partial-result warnings gate lost across the security plugin's thread handoff ([#5743](https://github.com/opensearch-project/sql/pull/5743))
* Fix zero-offset limit consuming the first deduplicated row ([#5701](https://github.com/opensearch-project/sql/pull/5701))
* Force `snakeyaml-engine` version to resolve dependency conflict ([#5762](https://github.com/opensearch-project/sql/pull/5762))
* Fix PPL `LIKE` function so `\\` correctly escapes the escape character ([#5653](https://github.com/opensearch-project/sql/pull/5653))
* Resolve dotted source paths in pushed-down Calcite scripts so object subfields no longer silently return null ([#5724](https://github.com/opensearch-project/sql/pull/5724))
* Shadow stale mapped leaf columns when `spath` or `eval` overrides an object parent field ([#5726](https://github.com/opensearch-project/sql/pull/5726))
* Fix `dedup` 500 on the analytics-engine route by skipping the dedup-simplify rule before the engine handoff ([#5695](https://github.com/opensearch-project/sql/pull/5695))
* Fix `mvindex()` failure when `plugins.calcite.pushdown.enabled=true` ([#5689](https://github.com/opensearch-project/sql/pull/5689))
* Avoid running the `analyze` measurement path for `profile`-only queries, fixing ~800% performance regression ([#5688](https://github.com/opensearch-project/sql/pull/5688))

### Infrastructure

* Publish the `ppl-rest-spi` snapshot artifact so external consumers can resolve `unified-query-opensearch` dependencies ([#5676](https://github.com/opensearch-project/sql/pull/5676))
* Exclude suites from `integTestRemote` that require analytics-engine plugins, Prometheus, or datasource encryption keys ([#5709](https://github.com/opensearch-project/sql/pull/5709))
* Exclude `HighlightFunctionIT` from license-header checks to work around RAT 0.18 charset misdetection ([#5733](https://github.com/opensearch-project/sql/pull/5733))

### Documentation

* Add DataFusion backend support column to the PPL command reference ([#5679](https://github.com/opensearch-project/sql/pull/5679))
* Remove redundant pointer to bucket functions from the function list ([#5714](https://github.com/opensearch-project/sql/pull/5714))

### Maintenance

* Update the expensive-sort analyze rule to account for `CalciteEnumerableTopK` and add integration tests ([#5710](https://github.com/opensearch-project/sql/pull/5710))
* Migrate to Jackson 3.x APIs ([#5703](https://github.com/opensearch-project/sql/pull/5703))
* Preserve plugin-managed indices during integration test cleanup to avoid repeated recreation of audit and system indices ([#5630](https://github.com/opensearch-project/sql/pull/5630))
