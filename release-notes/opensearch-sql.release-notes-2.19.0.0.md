Compatible with OpenSearch and OpenSearch Dashboards Version 2.19.0

### Features

* Call updateState when query is cancelled ([#3139](https://github.com/opensearch-project/sql/pull/3139))

### Bug Fixes

* Fix a regression issue of parsing datetime string with custom time format in Span ([#3079](https://github.com/opensearch-project/sql/pull/3079))
* Fix: CSV and Raw output, escape quotes ([#3063](https://github.com/opensearch-project/sql/pull/3063))
* Fix FilterOperator to cache next element and avoid repeated consumption on hasNext() calls ([#3123](https://github.com/opensearch-project/sql/pull/3123))
* Function str_to_date should work with two-digits year ([#2841](https://github.com/opensearch-project/sql/pull/2841))

### Enhancements

* Add validation method for Flint extension queries and wire it into the dispatcher ([#3096](https://github.com/opensearch-project/sql/pull/3096))
* Add grammar validation for PPL ([#3167](https://github.com/opensearch-project/sql/pull/3167))
* Add validation for unsupported type/identifier/commands ([#3195](https://github.com/opensearch-project/sql/pull/3195))
* Fix the flaky test testExtractDatePartWithTimeType() ([#3225](https://github.com/opensearch-project/sql/pull/3225))
* Allow metadata fields in PPL query ([#2789](https://github.com/opensearch-project/sql/pull/2789))

### Documentation

* Added documentation for the plugins.query.field_type_tolerance setting ([#3118](https://github.com/opensearch-project/sql/pull/3118))

### Maintenance

* Fix spotless check failure for #3148 ([#3158](https://github.com/opensearch-project/sql/pull/3158))
* Fix coverage issue for #3063 ([#3155](https://github.com/opensearch-project/sql/pull/3155))
* Call LeaseManager for BatchQuery ([#3153](https://github.com/opensearch-project/sql/pull/3153))
* Make GrammarElement public ([#3161](https://github.com/opensearch-project/sql/pull/3161))
* Update grammar validation settings ([#3165](https://github.com/opensearch-project/sql/pull/3165))
* [Backport 2.x] Add release notes for 1.3.15 ([#2537](https://github.com/opensearch-project/sql/pull/2537))
* Fix DateTimeFunctionTest.testWeekOfYearWithTimeType and YearWeekTestt.testYearWeekWithTimeType Test Failures ([#3235](https://github.com/opensearch-project/sql/pull/3235))

### Infrastructure

* [AUTO] Increment version to 2.19.0-SNAPSHOT ([#3119](https://github.com/opensearch-project/sql/pull/3119))
* Fix: CI Github Action ([#3177](https://github.com/opensearch-project/sql/pull/3177))
* Artifacts to upload should include the java version in its name to avoid conflicts ([#3239](https://github.com/opensearch-project/sql/pull/3239))
