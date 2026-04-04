## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features

* Add Calcite native SQL planning path in UnifiedQueryPlanner for ANSI SQL queries ([#5257](https://github.com/opensearch-project/sql/pull/5257))
* Support bi-directional graph traversal command `graphlookup` with BFS algorithm ([#5138](https://github.com/opensearch-project/sql/pull/5138))
* Support `graphlookup` with literal values as start for top-level PPL queries without requiring `source=` ([#5253](https://github.com/opensearch-project/sql/pull/5253))
* Change `graphlookup` struct output format from list to map to preserve column name information ([#5227](https://github.com/opensearch-project/sql/pull/5227))
* Fix UDT type loss through UNION in multisearch by preserving types in `leastRestrictive()` ([#5154](https://github.com/opensearch-project/sql/pull/5154))
* Add `mvexpand` command to expand multivalue fields into separate rows with optional limit parameter ([#5144](https://github.com/opensearch-project/sql/pull/5144))
* Add PPL search result highlighting via the `highlight` API parameter with support for simple and rich formats ([#5234](https://github.com/opensearch-project/sql/pull/5234))
* Add grammar bundle generation API for PPL autocomplete support in OpenSearch Dashboards ([#5162](https://github.com/opensearch-project/sql/pull/5162))
* Add `fieldformat` command to set display format of field values using eval expressions ([#5080](https://github.com/opensearch-project/sql/pull/5080))
* Implement PPL `convert` command with five conversion functions: auto, num, rmcomma, rmunit, and none ([#5157](https://github.com/opensearch-project/sql/pull/5157))

### Enhancements

* Add auto-extract mode for `spath` command returning flattened key-value map when path is omitted ([#5140](https://github.com/opensearch-project/sql/pull/5140))
* Add `nomv` command to convert multivalue fields into single values delimited by newline ([#5130](https://github.com/opensearch-project/sql/pull/5130))
* Add query cancellation support via `_tasks/_cancel` API for PPL queries ([#5254](https://github.com/opensearch-project/sql/pull/5254))
* Optimize `reverse` command performance by eliminating ROW_NUMBER() window function with context-aware three-tier logic ([#4775](https://github.com/opensearch-project/sql/pull/4775))
* Improve resource monitor error messages to include memory usage details and configuration guidance ([#5129](https://github.com/opensearch-project/sql/pull/5129))
* Support `LAST`, `FIRST`, and `TAKE` aggregations on TEXT type and script expressions with LIMIT pushdown to TopHits ([#5091](https://github.com/opensearch-project/sql/pull/5091))
* Make SQL plugin aware of FIPS-140-3 build parameter for BouncyCastle dependency management ([#5155](https://github.com/opensearch-project/sql/pull/5155))
* Support PPL queries with trailing pipes and empty pipes for more forgiving syntax ([#5161](https://github.com/opensearch-project/sql/pull/5161))
* Support creating and updating Prometheus recording and alerting rules via direct query resources API ([#5228](https://github.com/opensearch-project/sql/pull/5228))
* Update `graphlookup` syntax to support directional edge operators (`-->` and `<->`) ([#5209](https://github.com/opensearch-project/sql/pull/5209))
* Add CloudWatch-style `contains` operator for case-insensitive substring matching in PPL where clauses ([#5219](https://github.com/opensearch-project/sql/pull/5219))
* Bump ANTLR version from 4.7.1 to 4.13.2 across all modules for ATN serialization v4 support ([#5159](https://github.com/opensearch-project/sql/pull/5159))

### Bug Fixes

* Preserve `head`/TopK semantics during sort-expression pushdown to prevent returning more rows than requested ([#5135](https://github.com/opensearch-project/sql/pull/5135))
* Return null instead of error for double overflow to Infinity in arithmetic operations ([#5202](https://github.com/opensearch-project/sql/pull/5202))
* Return actual null from `JSON_EXTRACT` for missing or null paths instead of the string "null" ([#5196](https://github.com/opensearch-project/sql/pull/5196))
* Fix MAP path resolution for `top/rare`, `join`, `lookup`, and `streamstats` commands ([#5206](https://github.com/opensearch-project/sql/pull/5206))
* Fix MAP path resolution for symbol-based PPL commands by pre-materializing dotted paths ([#5198](https://github.com/opensearch-project/sql/pull/5198))
* Fix PIT (Point in Time) resource leaks in v2 query engine for explain, early close, and cursor close scenarios ([#5221](https://github.com/opensearch-project/sql/pull/5221))
* Fix fallback error handling to show original Calcite error instead of V2's generic message ([#5133](https://github.com/opensearch-project/sql/pull/5133))
* Fix memory leak caused by ExecutionEngine recreated per query appending to global function registry ([#5222](https://github.com/opensearch-project/sql/pull/5222))
* Fix path navigation on map columns for `spath` command by removing unconditional alias wrapping ([#5149](https://github.com/opensearch-project/sql/pull/5149))
* Fix pitest dependency resolution by decoupling plugin version from runtime version ([#5143](https://github.com/opensearch-project/sql/pull/5143))
* Fix typo: rename `renameClasue` to `renameClause` in ANTLR grammar files ([#5252](https://github.com/opensearch-project/sql/pull/5252))
* Fix `isnotnull()` not being pushed down when combined with multiple `!=` conditions ([#5238](https://github.com/opensearch-project/sql/pull/5238))

### Infrastructure

* Add `gradle.properties` file to build SQL plugin with FIPS-140-3 crypto standard by default ([#5231](https://github.com/opensearch-project/sql/pull/5231))

### Maintenance

* Add ahkcs (Kai Huang) as maintainer ([#5223](https://github.com/opensearch-project/sql/pull/5223))
* Add songkant-aws (SongKan Tang) as maintainer ([#5244](https://github.com/opensearch-project/sql/pull/5244))
* Revert dynamic column support for `spath` command in preparation for map-based extract-all redesign ([#5139](https://github.com/opensearch-project/sql/pull/5139))
* Fix bc-fips jar hell by marking BouncyCastle dependency as compileOnly ([#5158](https://github.com/opensearch-project/sql/pull/5158))
* Add CLAUDE.md with project overview, build commands, architecture, and development guidelines ([#5259](https://github.com/opensearch-project/sql/pull/5259))
