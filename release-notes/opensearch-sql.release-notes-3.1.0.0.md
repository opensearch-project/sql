## Version 3.1.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.1.0

### Breaking
* Switch percentile implementation to MergingDigest to align with OpenSearch ([#3698](https://github.com/opensearch-project/sql/pull/3698))
* Support decimal literal with Calcite ([#3673](https://github.com/opensearch-project/sql/pull/3673))

### Features
* Support ResourceMonitor with Calcite ([#3738](https://github.com/opensearch-project/sql/pull/3738))
* Support `flatten` command with Calcite ([#3747](https://github.com/opensearch-project/sql/pull/3747))
* Support `expand` command with Calcite ([#3745](https://github.com/opensearch-project/sql/pull/3745))
* Support trendline command in Calcite ([#3741](https://github.com/opensearch-project/sql/pull/3741))
* Support `appendcol` command with Calcite ([#3729](https://github.com/opensearch-project/sql/pull/3729))
* Support Grok command in Calcite engine ([#3678](https://github.com/opensearch-project/sql/pull/3678))
* Support match_only_text field type ([#3663](https://github.com/opensearch-project/sql/pull/3663))
* Add DISTINCT_COUNT_APPROX function ([#3654](https://github.com/opensearch-project/sql/pull/3654))
* Support merging object-type fields when fetching the schema from the index ([#3653](https://github.com/opensearch-project/sql/pull/3653))
* Support `top`, `rare` commands with Calcite ([#3647](https://github.com/opensearch-project/sql/pull/3647))
* Add earliest and latest in condition function ([#3640](https://github.com/opensearch-project/sql/pull/3640))
* Support `fillnull` command with Calcite ([#3634](https://github.com/opensearch-project/sql/pull/3634))
* Support function `coalesce` with Calcite ([#3628](https://github.com/opensearch-project/sql/pull/3628))
* Support functions `isempty`, `isblank` and `ispresent` with Calcite ([#3627](https://github.com/opensearch-project/sql/pull/3627))
* Support `describe` command with Calcite ([#3624](https://github.com/opensearch-project/sql/pull/3624))
* Support Limit pushdown ([#3615](https://github.com/opensearch-project/sql/pull/3615))
* Add UT for PredicateAnalyzer and AggregateAnalyzer ([#3612](https://github.com/opensearch-project/sql/pull/3612))
* Add a new row count estimation mechanism for CalciteIndexScan ([#3605](https://github.com/opensearch-project/sql/pull/3605))
* Implement `geoip` udf with Calcite ([#3604](https://github.com/opensearch-project/sql/pull/3604))
* Implement `cidrmatch` udf with Calcite ([#3603](https://github.com/opensearch-project/sql/pull/3603))
* Support `eventstats` command with Calcite ([#3585](https://github.com/opensearch-project/sql/pull/3585))
* Add lambda function and array related functions ([#3584](https://github.com/opensearch-project/sql/pull/3584))
* Implement cryptographic hash UDFs ([#3574](https://github.com/opensearch-project/sql/pull/3574))
* Calcite patterns command brain pattern method ([#3570](https://github.com/opensearch-project/sql/pull/3570))
* add json functions ([#3559](https://github.com/opensearch-project/sql/pull/3559))

### Bug Fixes
* Fix error when pushing down script filter with struct field ([#3693](https://github.com/opensearch-project/sql/pull/3693))
* Fix alias type referring to nested field ([#3674](https://github.com/opensearch-project/sql/pull/3674))
* Fix: Long IN-lists causes crash ([#3660](https://github.com/opensearch-project/sql/pull/3660))
* Add a trimmed project before aggregate to avoid NPE in Calcite ([#3621](https://github.com/opensearch-project/sql/pull/3621))
* Fix field not found issue in join output when column names are ambiguous ([#3760](https://github.com/opensearch-project/sql/pull/3760))
* Fix: correct ATAN(x, y) and CONV(x, a, b) functions bug ([#3748](https://github.com/opensearch-project/sql/pull/3748))
* Return double with correct precision for `UNIX_TIMESTAMP` ([#3679](https://github.com/opensearch-project/sql/pull/3679))
* Prevent push down limit with offset reach maxResultWindow ([#3713](https://github.com/opensearch-project/sql/pull/3713))
* Fix pushing down filter with nested filed of the text type ([#3645](https://github.com/opensearch-project/sql/pull/3645))
* Make query.size_limit only affect the final results ([#3623](https://github.com/opensearch-project/sql/pull/3623))
* Revert stream pattern method in V2 and implement SIMPLE_PATTERN in Calcite ([#3553](https://github.com/opensearch-project/sql/pull/3553))
* Remove the duplicated timestamp row in data type mapping table ([#2617](https://github.com/opensearch-project/sql/pull/2617))

### Maintenance
* Migrate existing UDFs to PPLFuncImpTable ([#3576](https://github.com/opensearch-project/sql/pull/3576))
* Modified workflow: Grammar Files & Async Query Core ([#3715](https://github.com/opensearch-project/sql/pull/3715))
* Bump setuptools to 78.1.1 ([#3671](https://github.com/opensearch-project/sql/pull/3671))
* Update PPL Limitation Docs ([#3656](https://github.com/opensearch-project/sql/pull/3656))
* create a new directory org/opensearch/direct-query/ ([#3649](https://github.com/opensearch-project/sql/pull/3649))
* Implement Parameter Validation for PPL functions on Calcite ([#3626](https://github.com/opensearch-project/sql/pull/3626))
* Add a TPC-H PPL query suite ([#3622](https://github.com/opensearch-project/sql/pull/3622))
