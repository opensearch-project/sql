## Version 2.19.6 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.6

### Enhancements

* Validate materialized view subqueries against SQL grammar deny list ([#5485](https://github.com/opensearch-project/sql/pull/5485))

### Bug Fixes

* Add ObjectInputFilter allowlist for deserialization in PlanSerializer, DefaultExpressionSerializer, and RelJsonSerializer ([#5469](https://github.com/opensearch-project/sql/pull/5469))

### Infrastructure

* Add CI mirror to plugin and dependency repositories to avoid Maven Central throttling ([#5591](https://github.com/opensearch-project/sql/pull/5591))
* Pin GitHub Actions to commit SHAs to prevent supply chain attacks ([#5573](https://github.com/opensearch-project/sql/pull/5573))

### Maintenance

* Bump assertj-core from 3.9.1 to 3.27.7 to address CVE-2026-24400 ([#5294](https://github.com/opensearch-project/sql/pull/5294))
