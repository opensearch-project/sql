## Version 3.6.0.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0.0

### Features
* Update mend config to allow remediation ([#5287](https://github.com/opensearch-project/sql/pull/5287))
* Add unified query parser API ([#5274](https://github.com/opensearch-project/sql/pull/5274))
* Add profiling support to unified query API ([#5268](https://github.com/opensearch-project/sql/pull/5268))
* Add Calcite native SQL planning in UnifiedQueryPlanner ([#5257](https://github.com/opensearch-project/sql/pull/5257))
* Add query cancellation support via _tasks/_cancel API for PPL queries ([#5254](https://github.com/opensearch-project/sql/pull/5254))
* Support graphLookup with literal value as its start ([#5253](https://github.com/opensearch-project/sql/pull/5253))
* PPL Highlight Support ([#5234](https://github.com/opensearch-project/sql/pull/5234))
* Support creating/updating prometheus rules ([#5228](https://github.com/opensearch-project/sql/pull/5228))
* Change the final output result of struct from list to map ([#5227](https://github.com/opensearch-project/sql/pull/5227))
* added cloudwatch style contains operator ([#5219](https://github.com/opensearch-project/sql/pull/5219))
* Update graphlookup syntax ([#5209](https://github.com/opensearch-project/sql/pull/5209))
* Onboard code diff analyzer and reviewer (sql) ([#5183](https://github.com/opensearch-project/sql/pull/5183))
* Add grammar bundle generation API for PPL language features ([#5162](https://github.com/opensearch-project/sql/pull/5162))
* Support PPL queries when having trailing pipes and/or empty pipes ([#5161](https://github.com/opensearch-project/sql/pull/5161))
* Bump ANTLR Version to 4.13.2 ([#5159](https://github.com/opensearch-project/sql/pull/5159))
* feat: Implement PPL convert command with 5 conversion functions ([#5157](https://github.com/opensearch-project/sql/pull/5157))
* Make sql plugin aware of FIPS build param (-Pcrypto.standard=FIPS-140-3) ([#5155](https://github.com/opensearch-project/sql/pull/5155))
* PPL Command: MvExpand ([#5144](https://github.com/opensearch-project/sql/pull/5144))
* Add auto-extract mode for `spath` command ([#5140](https://github.com/opensearch-project/sql/pull/5140))
* Support bi-directional graph traversal command `graphlookup` ([#5138](https://github.com/opensearch-project/sql/pull/5138))
* Add nomv command ([#5130](https://github.com/opensearch-project/sql/pull/5130))
* Improve resource monitor errors ([#5129](https://github.com/opensearch-project/sql/pull/5129))
* Support fetch_size API for PPL ([#5109](https://github.com/opensearch-project/sql/pull/5109))
* LAST/FIRST/TAKE aggregation should support TEXT type and Scripts ([#5091](https://github.com/opensearch-project/sql/pull/5091))
* fieldformat command implementation ([#5080](https://github.com/opensearch-project/sql/pull/5080))
* Implement `reverse` performance optimization ([#4775](https://github.com/opensearch-project/sql/pull/4775))

### Bug Fixes
* Fix flaky TPC-H Q1 test due to bugs in `MatcherUtils.closeTo()` ([#5283](https://github.com/opensearch-project/sql/pull/5283))
* Fix typo: rename renameClasue to renameClause ([#5252](https://github.com/opensearch-project/sql/pull/5252))
* Fix `isnotnull()` not being pushed down when combined with multiple `!=` conditions ([#5238](https://github.com/opensearch-project/sql/pull/5238))
* Fix memory leak: ExecutionEngine recreated per query appending to global function registry ([#5222](https://github.com/opensearch-project/sql/pull/5222))
* Fix PIT (Point in Time) resource leaks in v2 query engine ([#5221](https://github.com/opensearch-project/sql/pull/5221))
* Fix MAP path resolution for `top/rare`, `join`, `lookup` and `streamstats` ([#5206](https://github.com/opensearch-project/sql/pull/5206))
* Fix #5163: Return null for double overflow to Infinity in arithmetic ([#5202](https://github.com/opensearch-project/sql/pull/5202))
* Fix MAP path resolution for symbol-based PPL commands ([#5198](https://github.com/opensearch-project/sql/pull/5198))
* Fix #5176: Return actual null from JSON_EXTRACT for missing/null paths ([#5196](https://github.com/opensearch-project/sql/pull/5196))
* Fix multisearch UDT type loss through UNION (#5145, #5146, #5147) ([#5154](https://github.com/opensearch-project/sql/pull/5154))
* Fix path navigation on map columns for `spath` command ([#5149](https://github.com/opensearch-project/sql/pull/5149))
* Fix pitest dependency resolution with stable runtime version ([#5143](https://github.com/opensearch-project/sql/pull/5143))
* Fix #5114: preserve head/TopK semantics for sort-expression pushdown ([#5135](https://github.com/opensearch-project/sql/pull/5135))
* Fix fallback error handling to show original Calcite error ([#5133](https://github.com/opensearch-project/sql/pull/5133))
* Fix the bug when boolean comparison condition is simplifed to field ([#5071](https://github.com/opensearch-project/sql/pull/5071))
* Fix issue connecting with prometheus by wrapping with AccessController.doPrivilegedChecked ([#5061](https://github.com/opensearch-project/sql/pull/5061))

### Infrastructure
* Add gradle.properties file to build sql with -Pcrypto.standard=FIPS=140-3 by default ([#5231](https://github.com/opensearch-project/sql/pull/5231))
* Fix the flaky yamlRestTest caused by order of sample_logs ([#5119](https://github.com/opensearch-project/sql/pull/5119))
* Fix the filter of integTestWithSecurity ([#5098](https://github.com/opensearch-project/sql/pull/5098))

### Documentation
* Apply docs website feedback to ppl functions ([#5207](https://github.com/opensearch-project/sql/pull/5207))

### Maintenance
* Move some maintainers from active to Emeritus ([#5260](https://github.com/opensearch-project/sql/pull/5260))
* Add CLAUDE.md ([#5259](https://github.com/opensearch-project/sql/pull/5259))
* Add songkant-aws as maintainer ([#5244](https://github.com/opensearch-project/sql/pull/5244))
* Add ahkcs as maintainer ([#5223](https://github.com/opensearch-project/sql/pull/5223))
* Fix bc-fips jar hell by marking dependency as compileOnly ([#5158](https://github.com/opensearch-project/sql/pull/5158))
* Revert dynamic column support ([#5139](https://github.com/opensearch-project/sql/pull/5139))
* Increment version to 3.6.0-SNAPSHOT ([#5115](https://github.com/opensearch-project/sql/pull/5115))
* Upgrade assertj-core to 3.27.7 ([#5100](https://github.com/opensearch-project/sql/pull/5100))
