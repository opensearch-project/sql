Compatible with OpenSearch and OpenSearch Dashboards Version 3.0.0.0-beta1

### Breaking Changes
* Unified OpenSearch PPL Data Type ([#3345](https://github.com/opensearch-project/sql/pull/3345))
* Add datetime functions ([#3473](https://github.com/opensearch-project/sql/pull/3473))
* Support CAST function with Calcite ([#3439](https://github.com/opensearch-project/sql/pull/3439))

### Features
* Framework of Calcite Engine: Parser, Catalog Binding and Plan Converter ([#3249](https://github.com/opensearch-project/sql/pull/3249))
* Enable Calcite by default and refactor all related ITs ([#3468](https://github.com/opensearch-project/sql/pull/3468))
* Make PPL execute successfully on Calcite engine ([#3258](https://github.com/opensearch-project/sql/pull/3258))
* Implement ppl join command with Calcite ([#3364](https://github.com/opensearch-project/sql/pull/3364))
* Implement ppl `IN` subquery command with Calcite ([#3371](https://github.com/opensearch-project/sql/pull/3371))
* Implement ppl relation subquery command with Calcite ([#3378](https://github.com/opensearch-project/sql/pull/3378))
* Implement ppl `exists` subquery command with Calcite ([#3388](https://github.com/opensearch-project/sql/pull/3388))
* Implement ppl scalar subquery command with Calcite ([#3392](https://github.com/opensearch-project/sql/pull/3392))
* Implement lookup command ([#3419](https://github.com/opensearch-project/sql/pull/3419))
* Support In expression in Calcite Engine ([#3429](https://github.com/opensearch-project/sql/pull/3429))
* Support ppl BETWEEN operator within Calcite ([#3433](https://github.com/opensearch-project/sql/pull/3433))
* Implement ppl `dedup` command with Calcite ([#3416](https://github.com/opensearch-project/sql/pull/3416))
* Support `parse` command with Calcite ([#3474](https://github.com/opensearch-project/sql/pull/3474))
* Support `TYPEOF` function with Calcite ([#3446](https://github.com/opensearch-project/sql/pull/3446))
* New output for explain endpoint with Calcite engine ([#3521](https://github.com/opensearch-project/sql/pull/3521))
* Make basic aggregation working ([#3318](https://github.com/opensearch-project/sql/pull/3318), [#3355](https://github.com/opensearch-project/sql/pull/3355))
* Push down project and filter operator into index scan ([#3327](https://github.com/opensearch-project/sql/pull/3327))
* Enable push down optimization by default ([#3366](https://github.com/opensearch-project/sql/pull/3366))
* Calcite enable pushdown aggregation ([#3389](https://github.com/opensearch-project/sql/pull/3389))
* Support multiple table and index pattern ([#3409](https://github.com/opensearch-project/sql/pull/3409))
* Support group by span over time based column with Span UDF ([#3421](https://github.com/opensearch-project/sql/pull/3421))
* Support nested field ([#3476](https://github.com/opensearch-project/sql/pull/3476))
* Execute Calcite PPL query in thread pool ([#3508](https://github.com/opensearch-project/sql/pull/3508))
* Support UDT for date, time, timestamp ([#3483](https://github.com/opensearch-project/sql/pull/3483))
* Support UDT for IP ([#3504](https://github.com/opensearch-project/sql/pull/3504))
* Support GEO_POINT type ([#3511](https://github.com/opensearch-project/sql/pull/3511))
* Add UDF interface ([#3374](https://github.com/opensearch-project/sql/pull/3374))
* Add missing text function ([#3471](https://github.com/opensearch-project/sql/pull/3471))
* Add string builtin functions ([#3393](https://github.com/opensearch-project/sql/pull/3393))
* Add math UDF ([#3390](https://github.com/opensearch-project/sql/pull/3390))
* Add condition UDFs ([#3412](https://github.com/opensearch-project/sql/pull/3412))
* Register OpenSearchTypeSystem to OpenSearchTypeFactory ([#3349](https://github.com/opensearch-project/sql/pull/3349))
* Enable update calcite setting through _plugins/_query/settings API ([#3531](https://github.com/opensearch-project/sql/pull/3531))

### Enhancements
* Support line comment and block comment in PPL ([#2806](https://github.com/opensearch-project/sql/pull/2806))
* [Calcite Engine] Function framework refactoring ([#3522](https://github.com/opensearch-project/sql/pull/3522))

### Bug Fixes
* Fix execution errors caused by plan gap ([#3350](https://github.com/opensearch-project/sql/pull/3350))
* Support push down text field correctly ([#3376](https://github.com/opensearch-project/sql/pull/3376))
* Fix the join condition resolving bug introduced by IN subquery implementation ([#3377](https://github.com/opensearch-project/sql/pull/3377))
* Fix flaky tests ([#3456](https://github.com/opensearch-project/sql/pull/3456))
* Fix antlr4 parser issues ([#3492](https://github.com/opensearch-project/sql/pull/3492))
* Fix CSV handling of embedded crlf ([#3515](https://github.com/opensearch-project/sql/pull/3515))
* Fix return types of MOD and DIVIDE UDFs ([#3513](https://github.com/opensearch-project/sql/pull/3513))
* Fix varchar bug ([#3518](https://github.com/opensearch-project/sql/pull/3518))
* Fix text function IT for locate and strcmp ([#3482](https://github.com/opensearch-project/sql/pull/3482))
* Fix IT and CI, revert alias change ([#3423](https://github.com/opensearch-project/sql/pull/3423))
* Fix CalcitePPLJoinIT ([#3369](https://github.com/opensearch-project/sql/pull/3369))
* Keep aggregation in Calcite consistent with current PPL behavior ([#3405](https://github.com/opensearch-project/sql/pull/3405))
* Revert result ordering of `stats-by` ([#3427](https://github.com/opensearch-project/sql/pull/3427))
* Correct the precedence for logical operators ([#3435](https://github.com/opensearch-project/sql/pull/3435))
* Use correct timezone name ([#3517](https://github.com/opensearch-project/sql/pull/3517))

### Infrastructure
* Build integration test framework ([#3342](https://github.com/opensearch-project/sql/pull/3342))
* Set bouncycastle version inline ([#3469](https://github.com/opensearch-project/sql/pull/3469))
* Use entire shadow jar to fix IT ([#3447](https://github.com/opensearch-project/sql/pull/3447))
* Separate with/without pushdown ITs ([#3413](https://github.com/opensearch-project/sql/pull/3413))

### Documentation
* Documentation for PPL new engine (V3) and limitations of 3.0.0 Beta ([#3488](https://github.com/opensearch-project/sql/pull/3488))

### Maintenance
* CVE-2024-57699 High: Fix json-smart vulnerability ([#3484](https://github.com/opensearch-project/sql/pull/3484))
* Adding new maintainer @qianheng-aws ([#3509](https://github.com/opensearch-project/sql/pull/3509))
* Bump SQL main to version 3.0.0.0-beta1 ([#3489](https://github.com/opensearch-project/sql/pull/3489))
* Merge feature/calcite-engine to main ([#3448](https://github.com/opensearch-project/sql/pull/3448))
* Merge main for OpenSearch 3.0 release ([#3434](https://github.com/opensearch-project/sql/pull/3434))
