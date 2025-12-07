## Version 3.4.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.4.0

### Features
* Support `chart` command in PPL ([#4579](https://github.com/opensearch-project/sql/pull/4579))
* Support `Streamstats` command with calcite ([#4297](https://github.com/opensearch-project/sql/pull/4297))

### Enhancements
* Add `bucket_nullable` argument for `Streamstats` command ([#4831](https://github.com/opensearch-project/sql/pull/4831))
* Add `regexp_replace()` function as alias of `replace()` ([#4765](https://github.com/opensearch-project/sql/pull/4765))
* Convert `dedup` pushdown to composite + top_hits ([#4844](https://github.com/opensearch-project/sql/pull/4844))
* Merge group fields for aggregate if having dependent group fields ([#4703](https://github.com/opensearch-project/sql/pull/4703))
* Merge the implementation of `timechart` and `chart` ([#4755](https://github.com/opensearch-project/sql/pull/4755))
* PPL tostring() implementation issue #4492 ([#4497](https://github.com/opensearch-project/sql/pull/4497))
* Perform RexNode expression standardization for script push down. ([#4795](https://github.com/opensearch-project/sql/pull/4795))
* Pushdown sort by complex expressions to scan ([#4750](https://github.com/opensearch-project/sql/pull/4750))
* Pushdown the `top` `rare` commands to nested aggregation ([#4707](https://github.com/opensearch-project/sql/pull/4707))
* Refactor alias type field by adding another project with alias ([#4881](https://github.com/opensearch-project/sql/pull/4881))
* Remove count aggregation for sort on aggregate measure ([#4867](https://github.com/opensearch-project/sql/pull/4867))
* Remove redundant push-down-filters derived for bucket-non-null agg ([#4843](https://github.com/opensearch-project/sql/pull/4843))
* Remove unnecessary filter for DateHistogram aggregation ([#4877](https://github.com/opensearch-project/sql/pull/4877))
* Specify timestamp field with `timefield` in timechart command ([#4784](https://github.com/opensearch-project/sql/pull/4784))
* Support `appendpipe`command in PPL ([#4602](https://github.com/opensearch-project/sql/pull/4602))
* Support `mvdedup` eval function ([#4828](https://github.com/opensearch-project/sql/pull/4828))
* Support `mvindex` eval function ([#4794](https://github.com/opensearch-project/sql/pull/4794))
* Support push down sort on aggregation  measure for more than one agg call ([#4759](https://github.com/opensearch-project/sql/pull/4759))
* Support wildcard for replace command ([#4698](https://github.com/opensearch-project/sql/pull/4698))
* Add `bucket_nullable` argument for `Eventstats` ([#4817](https://github.com/opensearch-project/sql/pull/4817))
* Bin command error message enhancement ([#4690](https://github.com/opensearch-project/sql/pull/4690))
* Update clickbench queries with parameter bucket_nullable=false ([#4732](https://github.com/opensearch-project/sql/pull/4732))

### Bug Fixes
* Add hashCode() and equals() to the value class of ExprJavaType ([#4885](https://github.com/opensearch-project/sql/pull/4885))
* BucketAggretationParser should handle more non-composite bucket types ([#4706](https://github.com/opensearch-project/sql/pull/4706))
* Do not remove nested fields in resolving AllFieldsExcludeMeta ([#4708](https://github.com/opensearch-project/sql/pull/4708))
* Fix binning udf resolution / Add type coercion support for binning UDFs ([#4742](https://github.com/opensearch-project/sql/pull/4742))
* Fix bug that `Streamstats` command incorrectly treats null as a valid group ([#4777](https://github.com/opensearch-project/sql/pull/4777))
* Fix filter push down producing redundant filter queries ([#4744](https://github.com/opensearch-project/sql/pull/4744))
* Fix function identify problem in converting to sql dialect ([#4793](https://github.com/opensearch-project/sql/pull/4793))
* Fix search anoymizer only ([#4783](https://github.com/opensearch-project/sql/pull/4783))
* Fix sub-fields accessing of generated structs ([#4683](https://github.com/opensearch-project/sql/pull/4683))
* Fix wrong parameter and return result logic for LogPatternAggFunction ([#4868](https://github.com/opensearch-project/sql/pull/4868))
* Grouping key field type can only be overwritten when the `ExprCoreType`s are different ([#4850](https://github.com/opensearch-project/sql/pull/4850))
* Fix eval on grouped fields after timechart ([#4758](https://github.com/opensearch-project/sql/pull/4758))
* Support access to nested field of struct after fields command ([#4719](https://github.com/opensearch-project/sql/pull/4719))
* Support escaped field names in SPath parsing ([#4813](https://github.com/opensearch-project/sql/pull/4813))
* Support script pushdown in sort-on-measure pushdown rewriting ([#4749](https://github.com/opensearch-project/sql/pull/4749))
* Support serializing external OpenSearch UDFs at pushdown time ([#4618](https://github.com/opensearch-project/sql/pull/4618))
* Support using decimal as span literals ([#4717](https://github.com/opensearch-project/sql/pull/4717))
* Translate `SAFE_CAST` to `TRY_CAST` in Spark SQL ([#4788](https://github.com/opensearch-project/sql/pull/4788))
* Update syntax: like(string, PATTERN[, case_sensitive]) ([#4837](https://github.com/opensearch-project/sql/pull/4837))
* [BugFix] Fix Memory Exhaustion for Multiple Filtering Operations in PPL ([#4841](https://github.com/opensearch-project/sql/pull/4841))

### Infrastructure
* Add config for CodeRabbit review ([#4890](https://github.com/opensearch-project/sql/pull/4890))
* Fix disk no space issue in bwc-test ([#4716](https://github.com/opensearch-project/sql/pull/4716))
* Update github workflows to move from macos-13 to 14 ([#4779](https://github.com/opensearch-project/sql/pull/4779))

### Documentation
* Update PPL Command Documentation ([#4562](https://github.com/opensearch-project/sql/pull/4562))
* Doc update for `json_valid` ([#4803](https://github.com/opensearch-project/sql/pull/4803))

### Maintenance
* Bump Calcite to 1.41.0 ([#4714](https://github.com/opensearch-project/sql/pull/4714))
* Execute yamlRestTest in integration job ([#4838](https://github.com/opensearch-project/sql/pull/4838))
* Fix test failures due to version in mapping ([#4748](https://github.com/opensearch-project/sql/pull/4748))
* Support timeouts for Calcite queries ([#4857](https://github.com/opensearch-project/sql/pull/4857))
* [Maintenance] Enforce PR label of 'bugFix' instead of 'bug' ([#4773](https://github.com/opensearch-project/sql/pull/4773))
* [3.4.0] Bump Gradle to 9.2.0 and GitHub Action JDK to 25 ([#4824](https://github.com/opensearch-project/sql/pull/4824))

### Refactoring
* Implement one-batch lookahead for index enumerators ([#4345](https://github.com/opensearch-project/sql/pull/4345))