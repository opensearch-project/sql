## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Features

* Add Union command in PPL with type coercion and UNION ALL semantics ([#5240](https://github.com/opensearch-project/sql/pull/5240))
* Support SQL Vector Search via `vectorSearch()` table function with k-NN pushdown and filtering modes ([#5394](https://github.com/opensearch-project/sql/pull/5394))

### Enhancements

* Add query-type whitelist to block non-query statements (DML/DDL) in unified SQL execution path ([#5330](https://github.com/opensearch-project/sql/pull/5330))
* Add time conversion functions (`ctime`, `mktime`, `mstime`, `dur2sec`) and `timeformat` parameter for the PPL convert command ([#5210](https://github.com/opensearch-project/sql/pull/5210))
* Add `tests.analytics.parquet_indices` toggle for analytics-engine integration test coverage ([#5436](https://github.com/opensearch-project/sql/pull/5436))
* Define unified SQL language spec with composable extensions for OpenSearch SQL syntax ([#5360](https://github.com/opensearch-project/sql/pull/5360))
* Register `LENGTH`, `REGEXP_REPLACE`, `DATE_TRUNC` in unified function spec for ClickBench query support ([#5419](https://github.com/opensearch-project/sql/pull/5419))
* Register missing SQL V2 relevance functions and aliases in Calcite function table ([#5440](https://github.com/opensearch-project/sql/pull/5440))
* Route unified SQL path through V2 ANTLR parser with `CalciteRelNodeVisitor` ([#5438](https://github.com/opensearch-project/sql/pull/5438))
* Resolve SQL unified query gaps for SELECT clauses, LIMIT/OFFSET, derived tables, and window functions ([#5450](https://github.com/opensearch-project/sql/pull/5450))
* Extend V2 SQL parser with `IN`/`EXISTS` subquery support for unified query path ([#5448](https://github.com/opensearch-project/sql/pull/5448))
* Extend V2 SQL parser with `JOIN` (INNER/LEFT/RIGHT/CROSS) for unified query path ([#5446](https://github.com/opensearch-project/sql/pull/5446))
* Add UDT support for IP and Binary types in analytics-engine response schema ([#5463](https://github.com/opensearch-project/sql/pull/5463))
* Add coercion rules and placeholder UDF to handle VARBINARY-to-VARCHAR comparisons for IP/binary fields ([#5443](https://github.com/opensearch-project/sql/pull/5443))
* Close gaps from top/rare analytics-engine wiring by forwarding `PPL_SYNTAX_LEGACY_PREFERRED` and adding stable tie-break ([#5433](https://github.com/opensearch-project/sql/pull/5433))
* Use `leastRestrictive` for `mvappend` element-type widening to fix analytics-engine Substrait serialization ([#5424](https://github.com/opensearch-project/sql/pull/5424))
* Lower `isempty`/`isblank` to `CHAR_LENGTH = 0` instead of multiset `IS_EMPTY` for analytics-engine compatibility ([#5439](https://github.com/opensearch-project/sql/pull/5439))
* Add `IS [NOT] NULL` predicate syntax support in PPL ([#5278](https://github.com/opensearch-project/sql/pull/5278))
* Allow `limit=N` keyword syntax for `head` and `top` commands ([#4249](https://github.com/opensearch-project/sql/pull/4249))
* Register `DISTINCT_COUNT_APPROX` logical marker in PPLFuncImpTable for unified/analytics-engine paths ([#5466](https://github.com/opensearch-project/sql/pull/5466))
* Fix singleton stack-corruption NPE in `DatetimeUdtNormalizeRule` by instantiating rules per `plan()` call ([#5458](https://github.com/opensearch-project/sql/pull/5458))
* Improve exception handling in `UnifiedQueryPlanner` with proper error classification and logging ([#5465](https://github.com/opensearch-project/sql/pull/5465))
* Create parquet-backed test indices for `spath` command analytics-engine route ([#5441](https://github.com/opensearch-project/sql/pull/5441))
* Improve error messages for invalid index mapping by formatting index patterns and including underlying error details ([#5370](https://github.com/opensearch-project/sql/pull/5370))
* Initial implementation of report-builder interface for richer error context in responses ([#5266](https://github.com/opensearch-project/sql/pull/5266))
* Validate materialized view subqueries against SQL grammar deny list ([#5485](https://github.com/opensearch-project/sql/pull/5485))

### Bug Fixes

* Fix `COALESCE(null, int)` returning string type by using `SqlTypeName.NULL` for substituted literals ([#5382](https://github.com/opensearch-project/sql/pull/5382))
* Fix `NOT IN` including null/missing rows by adding exists filter for three-valued logic compliance ([#5337](https://github.com/opensearch-project/sql/pull/5337))
* Fix `NOT LIKE` incorrectly including rows with null/missing fields ([#5338](https://github.com/opensearch-project/sql/pull/5338))
* Fix PPL mixed text/keyword field type across wildcard indices causing silently dropped documents ([#5358](https://github.com/opensearch-project/sql/pull/5358))
* Fix `bin`/`chart` NPE with null values by declaring nullable return type and adding nullsLast to sorts ([#5334](https://github.com/opensearch-project/sql/pull/5334))
* Fix chained `streamstats` with window causing NPE by replacing correlate-based plan with self-join ([#5359](https://github.com/opensearch-project/sql/pull/5359))
* Fix `eval` overwriting MAP root field when assigning multiple dotted-path names ([#5351](https://github.com/opensearch-project/sql/pull/5351))
* Fix `json_set` crash and `json_delete` no-op with `$.key` paths by preventing double-prefixing ([#5339](https://github.com/opensearch-project/sql/pull/5339))
* Fix multiple `appendpipe` error while revisiting parent AST by using relBuilder stack directly ([#5322](https://github.com/opensearch-project/sql/pull/5322))
* Fix `rename` with wildcard applying on hidden/metadata fields ([#5350](https://github.com/opensearch-project/sql/pull/5350))
* Fix sort order not preserved through `dedup` in Calcite engine ([#5353](https://github.com/opensearch-project/sql/pull/5353))
* Fix `transpose` command name collision with 'value' field ([#5352](https://github.com/opensearch-project/sql/pull/5352))
* Scope SQL cursor continuation to original query indices under Fine-Grained Access Control ([#5399](https://github.com/opensearch-project/sql/pull/5399))
* Normalize datetime types for unified query API with UDT rewrite and output cast rules ([#5408](https://github.com/opensearch-project/sql/pull/5408))
* Fix SQL query routing to analytics engine after V2 parser change ([#5456](https://github.com/opensearch-project/sql/pull/5456))
* Fix `SemanticCheckException` not thrown for invalid field in nested function ([#5239](https://github.com/opensearch-project/sql/pull/5239))
* Handle Prometheus `/api/v1/metadata` responses without `data` field for Cortex-backed datasources ([#5437](https://github.com/opensearch-project/sql/pull/5437))
* Use `ObjectInputFilter` allowlist for deserialization in cursor and script pushdown serializers ([#5469](https://github.com/opensearch-project/sql/pull/5469))

### Infrastructure

* Add integration tests for analytics engine index-level authorization ([#5462](https://github.com/opensearch-project/sql/pull/5462))
* Add issues write permission to untriaged label workflow ([#5457](https://github.com/opensearch-project/sql/pull/5457))
* Add tiebreaker to stats sort-on-measure IT queries for deterministic results across engines ([#5435](https://github.com/opensearch-project/sql/pull/5435))
* Land analytics-engine PPL integration into main with query routing, explain, profiling, and async execution ([#5430](https://github.com/opensearch-project/sql/pull/5430))
* Fix distribution build by pinning `analytics-api` dependency to `3.7.0-SNAPSHOT` ([#5455](https://github.com/opensearch-project/sql/pull/5455))
* Pin GitHub Actions to commit SHAs for supply chain security ([#5464](https://github.com/opensearch-project/sql/pull/5464))
* Bump Gradle wrapper to 9.4.1 and workaround `@Ignore` test failure ([#5414](https://github.com/opensearch-project/sql/pull/5414))
* Fix link checker CI failure by excluding LinkedIn URLs ([#5461](https://github.com/opensearch-project/sql/pull/5461))
* Integration test cases for field-level security ([#5008](https://github.com/opensearch-project/sql/pull/5008))
* Skip `vectorSearch()` missing-plugin integration test when the k-NN plugin is installed, fixing the distribution integ-test run since distributions bundle k-NN ([#5492](https://github.com/opensearch-project/sql/pull/5492))

### Documentation

* Update SQL docs for querying multiple indices with backtick-enclosed list syntax ([#5340](https://github.com/opensearch-project/sql/pull/5340))
* Update PPL command doc examples to use OpenTelemetry sample data ([#5303](https://github.com/opensearch-project/sql/pull/5303))

### Maintenance

* Add PPL bugfix skill for Claude Code with automated triage, TDD-style fix, and PR creation ([#5307](https://github.com/opensearch-project/sql/pull/5307))
* Refactor the dedupe workflow by extracting a reusable workflow to opensearch-build ([#5319](https://github.com/opensearch-project/sql/pull/5319))
* Skip bot-created issues in dedupe detect workflow ([#5328](https://github.com/opensearch-project/sql/pull/5328))
* Update dedupe workflow to have correct name ([#5327](https://github.com/opensearch-project/sql/pull/5327))
* Version bump to OpenSearch 3.7 with Jackson 2 → 3 parser API and `_source` excludes serialization fixes ([#5361](https://github.com/opensearch-project/sql/pull/5361))
