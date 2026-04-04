## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features

* Support bi-directional graph traversal command `graphlookup` ([#5138](https://github.com/opensearch-project/sql/pull/5138))
* Support `graphlookup` with literal value as its start, enabling top-level PPL command usage without `source=` ([#5253](https://github.com/opensearch-project/sql/pull/5253))
* Add `mvexpand` command to expand multivalue fields into separate rows with optional limit parameter ([#5144](https://github.com/opensearch-project/sql/pull/5144))
* Add `fieldformat` command to set display format of field values using eval expressions ([#5080](https://github.com/opensearch-project/sql/pull/5080))
* Implement PPL `convert` command with 5 conversion functions: auto, num, rmcomma, rmunit, none ([#5157](https://github.com/opensearch-project/sql/pull/5157))
* Add grammar bundle generation API for PPL language features to support client-side autocomplete ([#5162](https://github.com/opensearch-project/sql/pull/5162))
* Add PPL search result highlighting via the `highlight` API parameter with support for simple and rich OpenSearch Dashboards formats ([#5234](https://github.com/opensearch-project/sql/pull/5234))
* Change `graphlookup` output format from list to map to preserve column name information in struct results ([#5227](https://github.com/opensearch-project/sql/pull/5227))
* Fix multisearch UDT type loss through UNION by preserving UDT types in `leastRestrictive()` ([#5154](https://github.com/opensearch-project/sql/pull/5154))

### Enhancements

* Add Calcite native SQL planning path in `UnifiedQueryPlanner` using Calcite's parser pipeline ([#5257](https://github.com/opensearch-project/sql/pull/5257))
* Add auto-extract mode for `spath` command returning flattened key-value map when path is omitted ([#5140](https://github.com/opensearch-project/sql/pull/5140))
* Add `nomv` command to convert multivalue fields into single values delimited by newline ([#5130](https://github.com/opensearch-project/sql/pull/5130))
* Add query cancellation support via `_tasks/_cancel` API for PPL queries ([#5254](https://github.com/opensearch-project/sql/pull/5254))
* Optimize `reverse` command performance by eliminating expensive ROW_NUMBER() window function with context-aware three-tier logic ([#4775](https://github.com/opensearch-project/sql/pull/4775))
* Improve resource monitor error messages to include memory usage details and configuration guidance ([#5129](https://github.com/opensearch-project/sql/pull/5129))
* Support `LAST`/`FIRST`/`TAKE` aggregations on TEXT type, script pushdown, alias handling, and LIMIT pushdown to TopHits ([#5091](https://github.com/opensearch-project/sql/pull/5091))
* Update `graphlookup` syntax to support directional edge notation (`-->` and `<->`) ([#5209](https://github.com/opensearch-project/sql/pull/5209))
* Support creating and updating Prometheus rules and Alertmanager silences via direct query resources API ([#5228](https://github.com/opensearch-project/sql/pull/5228))
* Add `contains` operator to PPL `where` clauses for case-insensitive substring matching ([#5219](https://github.com/opensearch-project/sql/pull/5219))
* Bump ANTLR version from 4.7.1 to 4.13.2 across all modules ([#5159](https://github.com/opensearch-project/sql/pull/5159))
* Support PPL queries with trailing pipes and empty pipes in command sequences ([#5161](https://github.com/opensearch-project/sql/pull/5161))
* Make SQL plugin aware of FIPS build parameter for proper BouncyCastle dependency scoping ([#5155](https://github.com/opensearch-project/sql/pull/5155))

### Bug Fixes

* Preserve head/TopK semantics for sort-expression pushdown to prevent returning more rows than requested ([#5135](https://github.com/opensearch-project/sql/pull/5135))
* Return null instead of error for double overflow to Infinity in arithmetic operations ([#5202](https://github.com/opensearch-project/sql/pull/5202))
* Return actual null from `JSON_EXTRACT` for missing or null paths instead of the string "null" ([#5196](https://github.com/opensearch-project/sql/pull/5196))
* Fix MAP path resolution for `top/rare`, `join`, `lookup`, and `streamstats` commands ([#5206](https://github.com/opensearch-project/sql/pull/5206))
* Fix MAP path resolution for symbol-based PPL commands by pre-materializing dotted paths ([#5198](https://github.com/opensearch-project/sql/pull/5198))
* Fix PIT (Point in Time) resource leaks in v2 query engine for explain, early close, and cursor close scenarios ([#5221](https://github.com/opensearch-project/sql/pull/5221))
* Fix fallback error handling to show original Calcite error instead of V2's generic message ([#5133](https://github.com/opensearch-project/sql/pull/5133))
* Fix memory leak caused by `ExecutionEngine` recreated per query appending to global function registry ([#5222](https://github.com/opensearch-project/sql/pull/5222))
* Fix path navigation on map columns for `spath` command by removing unconditional alias wrapping ([#5149](https://github.com/opensearch-project/sql/pull/5149))
* Fix pitest dependency resolution by decoupling plugin version from runtime version ([#5143](https://github.com/opensearch-project/sql/pull/5143))
* Fix typo: rename `renameClasue` to `renameClause` in ANTLR grammar files ([#5252](https://github.com/opensearch-project/sql/pull/5252))
* Fix `isnotnull()` not being pushed down when combined with multiple `!=` conditions ([#5238](https://github.com/opensearch-project/sql/pull/5238))

### Infrastructure

* Add `gradle.properties` file to build SQL with FIPS-140-3 crypto standard by default ([#5231](https://github.com/opensearch-project/sql/pull/5231))

### Maintenance

* Add ahkcs as maintainer ([#5223](https://github.com/opensearch-project/sql/pull/5223))
* Add songkant-aws as maintainer ([#5244](https://github.com/opensearch-project/sql/pull/5244))
* Revert dynamic column support for `spath` command in preparation for map-based extract-all mode ([#5139](https://github.com/opensearch-project/sql/pull/5139))
* Fix bc-fips jar hell by marking dependency as compileOnly since OpenSearch core now provides it by default ([#5158](https://github.com/opensearch-project/sql/pull/5158))
* Add CLAUDE.md with project overview, build commands, architecture, and development guidelines ([#5259](https://github.com/opensearch-project/sql/pull/5259))
