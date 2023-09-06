Compatible with OpenSearch and OpenSearch Dashboards Version 2.10.0

### Features


### Enhancements
* [Backport 2.x] Added support of timestamp/date/time using curly brackets by @matthewryanwells in https://github.com/opensearch-project/sql/pull/1908

### Bug Fixes
* [2.x] bump okhttp to 4.10.0 (#2043) by @joshuali925 in https://github.com/opensearch-project/sql/pull/2044
* [Backport 2.x] Okio upgrade to 3.5.0 by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1963
* [Backport 2.x] Fixed response codes For Requests With security exception. by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/2029
* [Backport 2.x] Backport breaking changes by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1920
* [Manual Backport #1943] Fixing string format change #1943 by @MitchellGale in https://github.com/opensearch-project/sql/pull/1946
* [Backport 2.x] Fix CVE by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1944
* [Backport 2.x] Breaking change OpenSearch main project - Action movement (#1958) by @MitchellGale in https://github.com/opensearch-project/sql/pull/1965
* [Backport 2.x] Update backport CI, add PR merged condition by @ps48 in https://github.com/opensearch-project/sql/pull/1970
* [Backport 2.x] Fixed exception when datasource is updated with existing configuration. by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/2008

### Documentation
* [Backport 2.x] Fix doctest data by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1998

### Infrastructure
* [Backport 2.x] Add _primary preference only for segment replication enabled indices by @opensearch-trigger-bot in
  https://github.com/opensearch-project/sql/pull/2036
* [Backport 2.x] Revert "Guarantee datasource read api is strong consistent read (#1815)" by @opensearch-trigger-bot in
* [Backport 2.x] [Spotless] Adds new line at end of java files by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1925
* (#1506) Remove reservedSymbolTable and replace with HIDDEN_FIELD_NAME… by @acarbonetto in https://github.com/opensearch-project/sql/pull/1964

### Refactoring
* [Backport 2.x] Applied formatting improvements to Antlr files based on spotless changes (#2017) by @MitchellGale in
* [Backport 2.x] Statically init `typeActionMap` in `OpenSearchExprValueFactory`. by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1901
* [Backport 2.x] (#1536) Refactor OpenSearchQueryRequest and move includes to builder by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/1948
* [Backport 2.x] [Spotless] Applying Google Code Format for core/src/main files #3 (#1932) by @MitchellGale in https://github.com/opensearch-project/sql/pull/1994
* [Backport 2.x] Developer guide update with Spotless details by @opensearch-trigger-bot in https://github.com/opensearch-project/sql/pull/2004
* [Backport 2.x] [Spotless] Applying Google Code Format for core/src/main files #4 #1933 by @MitchellGale in https://github.com/opensearch-project/sql/pull/1995
* [Backport 2.x] [Spotless] Applying Google Code Format for core/src/main files #2 #1931 by @MitchellGale in https://github.com/opensearch-project/sql/pull/1993
* [Backport 2.x] [Spotless] Applying Google Code Format for core/src/main files #1 #1930 by @MitchellGale in https://github.com/opensearch-project/sql/pull/1992
* [Backport 2.x] [Spotless] Applying Google Code Format for core #5 (#1951) by @MitchellGale in https://github.com/opensearch-project/sql/pull/1996
* [Backport 2.x] [spotless] Removes Checkstyle in favor of spotless by @MitchellGale in https://github.com/opensearch-project/sql/pull/2018
* [Backport 2.x] [Spotless] Entire project running spotless by @MitchellGale in https://github.com/opensearch-project/sql/pull/2016
---
**Full Changelog**: https://github.com/opensearch-project/sql/compare/2.3.0.0...v.2.10.0.0