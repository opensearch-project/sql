### Version 2.1.0.0 Release Notes
Compatible with OpenSearch and OpenSearch Dashboards Version 2.1.0

### Features
* Support match_phrase filter function in SQL and PPL ([#604](https://github.com/opensearch-project/sql/pull/604))
* Add implementation for `simple_query_string` relevance search function in SQL and PPL ([#635](https://github.com/opensearch-project/sql/pull/635))
* Add multi_match to SQL plugin ([#649](https://github.com/opensearch-project/sql/pull/649))
* Integ match bool prefix #187 ([#634](https://github.com/opensearch-project/sql/pull/634))
* PPL describe command ([#646](https://github.com/opensearch-project/sql/pull/646))

### Maintenance
* change plugin folder name to opensearch-sql-plugin ([#670](https://github.com/opensearch-project/sql/pull/670))

### Bug Fixes
* Integ replace junit assertthat with hamcrest import ([#616](https://github.com/opensearch-project/sql/pull/616))
* Integ relevance function it fix ([#608](https://github.com/opensearch-project/sql/pull/608))
* Fix merge conflict on function name ([#664](https://github.com/opensearch-project/sql/pull/664))
* Fix `fuzziness` parsing in `multi_match` function. Update tests. ([#668](https://github.com/opensearch-project/sql/pull/668))
* ODBC SSL Compliance Fix ([#653](https://github.com/opensearch-project/sql/pull/653))


### Infrastructure
* Match Query Unit Tests ([#614](https://github.com/opensearch-project/sql/pull/614))
* Uses custom plugin to publish zips to maven  ([#638](https://github.com/opensearch-project/sql/pull/638))
* version bump to 2.1.0 and gradle version bump ([#655](https://github.com/opensearch-project/sql/pull/655))










