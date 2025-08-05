## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Enhancements
* Add compare_ip operator udfs ([#3821](https://github.com/opensearch-project/sql/pull/3821))
* Add issue template specific for PPL commands and queries ([#3962](https://github.com/opensearch-project/sql/pull/3962))
* Add missing command in index.rst ([#3943](https://github.com/opensearch-project/sql/pull/3943))
* Append limit operator for QUEERY_SIZE_LIMIT ([#3940](https://github.com/opensearch-project/sql/pull/3940))
* CVE-2025-48924: upgrade commons-lang3 to 3.18.0 ([#3895](https://github.com/opensearch-project/sql/pull/3895))
* Change compare logical when comparing date related fields with string literal ([#3798](https://github.com/opensearch-project/sql/pull/3798))
* Disable a failed PPL query fallback to v2 by default ([#3952](https://github.com/opensearch-project/sql/pull/3952))
* Filter script pushdown with RelJson serialization in Calcite ([#3859](https://github.com/opensearch-project/sql/pull/3859))
* Push down QUERY_SIZE_LIMIT ([#3880](https://github.com/opensearch-project/sql/pull/3880))
* Skipping codegen and compile for Scan only plan ([#3853](https://github.com/opensearch-project/sql/pull/3853))
* Support Sort pushdown ([#3620](https://github.com/opensearch-project/sql/pull/3620))
* Support aggregation push down with scripts ([#3916](https://github.com/opensearch-project/sql/pull/3916))
* Support casting to IP with Calcite ([#3919](https://github.com/opensearch-project/sql/pull/3919))
* Support filter push down for Sarg value ([#3840](https://github.com/opensearch-project/sql/pull/3840))
* Support function argument coercion with Calcite ([#3914](https://github.com/opensearch-project/sql/pull/3914))
* Support partial filter push down ([#3850](https://github.com/opensearch-project/sql/pull/3850))
* Support pushdown physical sort operator to speedup SortMergeJoin ([#3864](https://github.com/opensearch-project/sql/pull/3864))
* Support relevance query functions pushdown implementation in Calcite ([#3834](https://github.com/opensearch-project/sql/pull/3834))
* Support span push down ([#3823](https://github.com/opensearch-project/sql/pull/3823))

### Bug Fixes
* Byte number should treated as Long in doc values ([#3928](https://github.com/opensearch-project/sql/pull/3928))
* Convert like function call to wildcard query for Calcite filter pushdown ([#3915](https://github.com/opensearch-project/sql/pull/3915))
* Correct null order for `sort` command with Calcite ([#3835](https://github.com/opensearch-project/sql/pull/3835))
* Default to UTC for date/time functions across PPL and SQL ([#3854](https://github.com/opensearch-project/sql/pull/3854))
* Fix create PIT permissions issue ([#3921](https://github.com/opensearch-project/sql/pull/3921))
* Fix the count() only aggregation pushdown issue ([#3891](https://github.com/opensearch-project/sql/pull/3891))
* Increase the precision of sum return type ([#3974](https://github.com/opensearch-project/sql/pull/3974))
* Support casting date literal to timestamp ([#3831](https://github.com/opensearch-project/sql/pull/3831))
* Support struct field with dynamic disabled ([#3829](https://github.com/opensearch-project/sql/pull/3829))
* Support full expression in WHERE clauses ([#3849](https://github.com/opensearch-project/sql/pull/3849))
* Translate JSONException to 400 instead of 500 ([#3833](https://github.com/opensearch-project/sql/pull/3833))
* Fix incorrect push down for Sarg with nullAs is TRUE ([#3882](https://github.com/opensearch-project/sql/pull/3882))
* Fix relevance query function over optimization issue in ReduceExpressionsRule ([#3851](https://github.com/opensearch-project/sql/pull/3851))

### Infrastructure
* Add 'testing' and 'security fix' to enforce-label-action ([#3897](https://github.com/opensearch-project/sql/pull/3897))
* Add enforce-labels action ([#3816](https://github.com/opensearch-project/sql/pull/3816))
* Update the maven snapshot publish endpoint and credential ([#3806](https://github.com/opensearch-project/sql/pull/3806))
* Update the maven snapshot publish endpoint and credential ([#3886](https://github.com/opensearch-project/sql/pull/3886))

### Documentation
* Update ppl documentation index for new functions ([#3868](https://github.com/opensearch-project/sql/pull/3868))
* Update the limitation docs ([#3801](https://github.com/opensearch-project/sql/pull/3801))
* Add release notes for 2.19.3 ([#3910](https://github.com/opensearch-project/sql/pull/3910))

### Maintenance
* Bump gradle to 8.14 and java to 24 ([#3875](https://github.com/opensearch-project/sql/pull/3875))
* Update commons-lang exclude rule to exclude it everywhere ([#3932](https://github.com/opensearch-project/sql/pull/3932))