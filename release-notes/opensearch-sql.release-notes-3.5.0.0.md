## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features
* Feature tonumber : issue #4514 tonumber function as part of roadmap #4287 ([#4605](https://github.com/opensearch-project/sql/pull/4605))
* Feature addtotals and addcoltotals ([#4754](https://github.com/opensearch-project/sql/pull/4754))
* Support `mvzip` eval function ([#4805](https://github.com/opensearch-project/sql/pull/4805))
* Support `split` eval function ([#4814](https://github.com/opensearch-project/sql/pull/4814))
* Support `mvfind` eval function ([#4839](https://github.com/opensearch-project/sql/pull/4839))
* Support `mvmap` eval function ([#4856](https://github.com/opensearch-project/sql/pull/4856))
* [Feature] implement transpose command as in the roadmap #4786 ([#5011](https://github.com/opensearch-project/sql/pull/5011))
* Feature/mvcombine ([#5025](https://github.com/opensearch-project/sql/pull/5025))
* Implement spath command with field resolution ([#5028](https://github.com/opensearch-project/sql/pull/5028))

### Enhancements
* ML command supports category_field parameter ([#3909](https://github.com/opensearch-project/sql/pull/3909))
* Time Unit Unification for bin/stats ([#4450](https://github.com/opensearch-project/sql/pull/4450))
* Enhance doc and error message handling for `bins` on time-related fields ([#4713](https://github.com/opensearch-project/sql/pull/4713))
* Push down filters on nested fields as nested queries ([#4825](https://github.com/opensearch-project/sql/pull/4825))
* Support sort expression pushdown for SortMergeJoin ([#4830](https://github.com/opensearch-project/sql/pull/4830))
* Add unified query transpiler API ([#4871](https://github.com/opensearch-project/sql/pull/4871))
* Pushdown join with `max=n` option to TopHits aggregation ([#4929](https://github.com/opensearch-project/sql/pull/4929))
* Support pushdown dedup with expression ([#4957](https://github.com/opensearch-project/sql/pull/4957))
* Add scalar min/max to BuiltinFunctionName ([#4967](https://github.com/opensearch-project/sql/pull/4967))
* Add unified query compiler API ([#4974](https://github.com/opensearch-project/sql/pull/4974))
* Support nested aggregation when calcite enabled ([#4979](https://github.com/opensearch-project/sql/pull/4979))
* Support profile options for PPL - Part I Implement phases level metrics. ([#4983](https://github.com/opensearch-project/sql/pull/4983))
* Dedup pushdown (TopHits Agg) should work with Object fields ([#4991](https://github.com/opensearch-project/sql/pull/4991))
* Support enumerable TopK ([#4993](https://github.com/opensearch-project/sql/pull/4993))
* Prune old in operator push down rules ([#4992](https://github.com/opensearch-project/sql/pull/4992))
* RexCall and RelDataType standardization for script push down ([#4914](https://github.com/opensearch-project/sql/pull/4914))
* Introduce logical dedup operators for PPL ([#5014](https://github.com/opensearch-project/sql/pull/5014))
* Support read multi-values from OpenSearch if no codegen triggered ([#5015](https://github.com/opensearch-project/sql/pull/5015))
* Add unified function interface with function discovery API ([#5039](https://github.com/opensearch-project/sql/pull/5039))
* Support profile option for PPL - Part II Implement operator level metrics ([#5044](https://github.com/opensearch-project/sql/pull/5044))
* Support spath with dynamic fields ([#5058](https://github.com/opensearch-project/sql/pull/5058))
* Adopt appendcol, appendpipe, multisearch to spath ([#5075](https://github.com/opensearch-project/sql/pull/5075))
* Set `max=1` in join as default when `plugins.ppl.syntax.legacy.preferred=false` ([#5057](https://github.com/opensearch-project/sql/pull/5057))
* Add OUTPUT as an alias for REPLACE in Lookup ([#5049](https://github.com/opensearch-project/sql/pull/5049))
* Separate explain mode from format params ([#5042](https://github.com/opensearch-project/sql/pull/5042))

### Bug Fixes
* Error handling for dot-containing field names ([#4907](https://github.com/opensearch-project/sql/pull/4907))
* Replace duplicated aggregation logic with aggregateWithTrimming() ([#4926](https://github.com/opensearch-project/sql/pull/4926))
* Remove GetAlias Call ([#4981](https://github.com/opensearch-project/sql/pull/4981))
* Fix PIT context leak in Legacy SQL for non-paginated queries ([#5009](https://github.com/opensearch-project/sql/pull/5009))
* [BugFix] Not between should use range query ([#5016](https://github.com/opensearch-project/sql/pull/5016))
* Move Calcite-only tests from CrossClusterSearchIT to CalciteCrossClusterSearchIT ([#5085](https://github.com/opensearch-project/sql/pull/5085))

### Infrastructure
* Add workflow for SQL CLI integration tests ([#4770](https://github.com/opensearch-project/sql/pull/4770))
* Remove access controller step in Calcite script ([#4900](https://github.com/opensearch-project/sql/pull/4900))
* Adjust CodeRabbit review config ([#4901](https://github.com/opensearch-project/sql/pull/4901))
* Add micro benchmarks for unified query layer ([#5043](https://github.com/opensearch-project/sql/pull/5043))
* Improve coderabbit config ([#5048](https://github.com/opensearch-project/sql/pull/5048))
* Update CodeRabbit instructions ([#4962](https://github.com/opensearch-project/sql/pull/4962))
* Add feedback reminder for CodeRabbit ([#4932](https://github.com/opensearch-project/sql/pull/4932))

### Documentation
* Migrate PPL Documentation from RST to Markdown ([#4912](https://github.com/opensearch-project/sql/pull/4912))
* [DOC] Callout the aggregation result may be approximate ([#4922](https://github.com/opensearch-project/sql/pull/4922))
* Show backticks in testing-doctest.md ([#4941](https://github.com/opensearch-project/sql/pull/4941))
* Escape underscore character in documentation for LIKE ([#4958](https://github.com/opensearch-project/sql/pull/4958))
* Apply feedback from documentation-website to PPL command docs ([#4997](https://github.com/opensearch-project/sql/pull/4997))
* Add PPL docs website exporter script ([#4950](https://github.com/opensearch-project/sql/pull/4950))
* Add version numbers for all settings in the docs ([#5019](https://github.com/opensearch-project/sql/pull/5019))
* chore: add legacy ppl index.rst for backwards compatibility ([#5026](https://github.com/opensearch-project/sql/pull/5026))
* Add index.md for PPL functions documentation ([#5033](https://github.com/opensearch-project/sql/pull/5033))

### Maintenance
* Remove all AccessController refs ([#4924](https://github.com/opensearch-project/sql/pull/4924))
* Extract unified query context for shared config management ([#4933](https://github.com/opensearch-project/sql/pull/4933))
* Remove shadow jar task from build file ([#4955](https://github.com/opensearch-project/sql/pull/4955))
* Add Frequently Used Big5 PPL Queries ([#4976](https://github.com/opensearch-project/sql/pull/4976))
* Increment version to 3.5.0 ([#5040](https://github.com/opensearch-project/sql/pull/5040))
* Upgrade assertj-core to 3.27.7 ([#5100](https://github.com/opensearch-project/sql/pull/5100))
