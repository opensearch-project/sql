## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features
* Implement spath command with field resolution ([#5028](https://github.com/opensearch-project/sql/pull/5028))
* Support spath with dynamic fields ([#5058](https://github.com/opensearch-project/sql/pull/5058))
* Implement transpose command as in the roadmap #4786 ([#5011](https://github.com/opensearch-project/sql/pull/5011))
* MvCombine Command Feature ([#5025](https://github.com/opensearch-project/sql/pull/5025))

### Enhancements
* Add OUTPUT as an alias for REPLACE in Lookup ([#5049](https://github.com/opensearch-project/sql/pull/5049))
* Add scalar min/max to BuiltinFunctionName ([#4967](https://github.com/opensearch-project/sql/pull/4967))
* Add unified function interface with function discovery API ([#5039](https://github.com/opensearch-project/sql/pull/5039))
* Add unified query compiler API ([#4974](https://github.com/opensearch-project/sql/pull/4974))
* Adopt appendcol, appendpipe, multisearch to spath ([#5075](https://github.com/opensearch-project/sql/pull/5075))
* Apply feedback from documentation-website to PPL command docs ([#4997](https://github.com/opensearch-project/sql/pull/4997))
* Extract unified query context for shared config management ([#4933](https://github.com/opensearch-project/sql/pull/4933))
* Introduce logical dedup operators for PPL ([#5014](https://github.com/opensearch-project/sql/pull/5014))
* ML command supports category_field parameter ([#3909](https://github.com/opensearch-project/sql/pull/3909))
* Prune old in operator push down rules ([#4992](https://github.com/opensearch-project/sql/pull/4992))
* Push down filters on nested fields as nested queries ([#4825](https://github.com/opensearch-project/sql/pull/4825))
* Separate explain mode from format params ([#5042](https://github.com/opensearch-project/sql/pull/5042))
* Set `max=1` in join as default when `plugins.ppl.syntax.legacy.preferred=false` ([#5057](https://github.com/opensearch-project/sql/pull/5057))
* Support `mvfind` eval function ([#4839](https://github.com/opensearch-project/sql/pull/4839))
* Support `mvmap` eval function ([#4856](https://github.com/opensearch-project/sql/pull/4856))
* Support enumerable TopK ([#4993](https://github.com/opensearch-project/sql/pull/4993))
* Support nested aggregation when calcite enabled ([#4979](https://github.com/opensearch-project/sql/pull/4979))
* Support profile option for PPL - Part II Implement operator level metrics ([#5044](https://github.com/opensearch-project/sql/pull/5044))
* Support profile options for PPL - Part I Implement phases level metrics ([#4983](https://github.com/opensearch-project/sql/pull/4983))
* Support pushdown dedup with expression ([#4957](https://github.com/opensearch-project/sql/pull/4957))
* Support read multi-values from OpenSearch if no codegen triggered ([#5015](https://github.com/opensearch-project/sql/pull/5015))

### Bug Fixes
* Dedup pushdown (TopHits Agg) should work with Object fields ([#4991](https://github.com/opensearch-project/sql/pull/4991))
* Fix PIT context leak in Legacy SQL for non-paginated queries ([#5009](https://github.com/opensearch-project/sql/pull/5009))
* Fix issue connecting with prometheus by wrapping with AccessController.doPrivilegedChecked ([#5101](https://github.com/opensearch-project/sql/pull/5101))
* Move Calcite-only tests from CrossClusterSearchIT to CalciteCrossClusterSearchIT ([#5085](https://github.com/opensearch-project/sql/pull/5085))
* Not between should use range query ([#5016](https://github.com/opensearch-project/sql/pull/5016))
* Remove GetAlias Call ([#4981](https://github.com/opensearch-project/sql/pull/4981))

### Infrastructure
* Add micro benchmarks for unified query layer ([#5043](https://github.com/opensearch-project/sql/pull/5043))
* Improve coderabbit config ([#5048](https://github.com/opensearch-project/sql/pull/5048))
* Update CodeRabbit instructions ([#4962](https://github.com/opensearch-project/sql/pull/4962))

### Documentation
* Add index.md for PPL functions documentation ([#5033](https://github.com/opensearch-project/sql/pull/5033))
* Add legacy ppl/index.rst for backwards compatibility ([#5026](https://github.com/opensearch-project/sql/pull/5026))
* Add version numbers for all settings in the docs ([#5019](https://github.com/opensearch-project/sql/pull/5019))

### Maintenance
* Add 3.5 Release Notes ([#5092](https://github.com/opensearch-project/sql/pull/5092))
* Increment version to 3.5.0 ([#5040](https://github.com/opensearch-project/sql/pull/5040))
* Upgrade assertj-core to 3.27.7 ([#5102](https://github.com/opensearch-project/sql/pull/5102))