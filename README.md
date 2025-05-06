<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

- [OpenSearch SQL](#opensearch-sql)
  - [Code Summary](#code-summary)
    - [SQL Engine](#sql-engine)
    - [Repository Checks](#repository-checks)
    - [Issues](#issues)
  - [Highlights](#highlights)
  - [Documentation](#documentation)
  - [Forum](#forum)
  - [Contributing](#contributing)
  - [Attribution](#attribution)
  - [Code of Conduct](#code-of-conduct)
  - [Security](#security)
  - [License](#license)
  - [Copyright](#copyright)

# OpenSearch SQL

OpenSearch enables you to extract insights out of OpenSearch using the familiar SQL or Piped Processing Language (PPL) query syntax. Use aggregations, group by, and where clauses to investigate your data. Read your data as JSON documents or CSV tables so you have the flexibility to use the format that works best for you.

The following projects are related to the SQL plugin, but stored in the different repos. Please refer to links below for details. This document will focus on the SQL plugin for OpenSearch.

- [SQL CLI](https://github.com/opensearch-project/sql-cli)
- [SQL JDBC](https://github.com/opensearch-project/sql-jdbc)
- [SQL ODBC](https://github.com/opensearch-project/sql-odbc)
- [Query Workbench](https://github.com/opensearch-project/dashboards-query-workbench)

## Code Summary

### SQL Engine

|                              |                                                                                                                                              |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Test and build               | [![SQL CI][sql-ci-badge]][sql-ci-link]                                                        |
| Code coverage                | [![codecov][sql-codecov-badge]][sql-codecov-link]                                                                                         |
| Distribution build tests     | [![OpenSearch IT tests][opensearch-it-badge]][opensearch-it-link] [![OpenSearch IT code][opensearch-it-code-badge]][opensearch-it-code-link] |
| Backward compatibility tests | [![BWC tests][bwc-tests-badge]][bwc-tests-link]                                                                                              |

### Repository Checks

|              |                                                                 |
| ------------ | --------------------------------------------------------------- |
| DCO Checker  | [![Developer certificate of origin][dco-badge]][dco-badge-link] |
| Link Checker | [![Link Checker][link-check-badge]][link-check-link]            |

### Issues

|                                                                |
| -------------------------------------------------------------- |
| [![good first issues open][good-first-badge]][good-first-link] |
| [![features open][feature-badge]][feature-link]                |
| [![enhancements open][enhancement-badge]][enhancement-link]    |
| [![bugs open][bug-badge]][bug-link]                            |
| [![untriaged open][untriaged-badge]][untriaged-link]           |
| [![nolabel open][nolabel-badge]][nolabel-link]                 |

[dco-badge]: https://github.com/opensearch-project/sql/actions/workflows/dco.yml/badge.svg
[dco-badge-link]: https://github.com/opensearch-project/sql/actions/workflows/dco.yml
[link-check-badge]: https://github.com/opensearch-project/sql/actions/workflows/link-checker.yml/badge.svg
[link-check-link]: https://github.com/opensearch-project/sql/actions/workflows/link-checker.yml
[bwc-tests-badge]: https://img.shields.io/badge/BWC%20tests-in%20progress-yellow
[bwc-tests-link]: https://github.com/opensearch-project/sql/issues/193
[good-first-badge]: https://img.shields.io/github/issues/opensearch-project/sql/good%20first%20issue.svg
[good-first-link]: https://github.com/opensearch-project/sql/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22+
[feature-badge]: https://img.shields.io/github/issues/opensearch-project/sql/feature.svg
[feature-link]: https://github.com/opensearch-project/sql/issues?q=is%3Aopen+is%3Aissue+label%3Afeature
[bug-badge]: https://img.shields.io/github/issues/opensearch-project/sql/bug.svg
[bug-link]: https://github.com/opensearch-project/sql/issues?q=is%3Aopen+is%3Aissue+label%3Abug+
[enhancement-badge]: https://img.shields.io/github/issues/opensearch-project/sql/enhancement.svg
[enhancement-link]: https://github.com/opensearch-project/sql/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement+
[untriaged-badge]: https://img.shields.io/github/issues/opensearch-project/sql/untriaged.svg
[untriaged-link]: https://github.com/opensearch-project/sql/issues?q=is%3Aopen+is%3Aissue+label%3Auntriaged+
[nolabel-badge]: https://img.shields.io/github/issues-search/opensearch-project/sql?color=yellow&label=no%20label%20issues&query=is%3Aopen%20is%3Aissue%20no%3Alabel
[nolabel-link]: https://github.com/opensearch-project/sql/issues?q=is%3Aopen+is%3Aissue+no%3Alabel+
[sql-codecov-badge]: https://codecov.io/gh/opensearch-project/sql/branch/main/graphs/badge.svg?flag=sql-engine
[sql-codecov-link]: https://codecov.io/gh/opensearch-project/sql
[opensearch-it-badge]: https://img.shields.io/badge/SQL%20IT%20tests-in%20progress-yellow
[opensearch-it-link]: https://github.com/opensearch-project/opensearch-build/issues/1124
[opensearch-it-code-badge]: https://img.shields.io/badge/SQL%20IT%20code-blue
[opensearch-it-code-link]: https://github.com/opensearch-project/sql/tree/main/integ-test

## Highlights

Besides basic filtering and aggregation, OpenSearch SQL also supports complex queries, such as querying semi-structured data, JOINs, set operations, sub-queries etc. Beyond the standard functions, OpenSearch functions are provided for better analytics and visualization. Please check our [documentation](#documentation) for more details.

Recently we have been actively improving our query engine primarily for better correctness and extensibility. Behind the scene, the new enhanced engine has already supported both SQL and Piped Processing Language. Please find more details in [SQL Engine V2 - Release Notes](./docs/dev/intro-v2-engine.md).

## Documentation

Please refer to the [SQL Language Reference Manual](./docs/user/index.rst), [Piped Processing Language (PPL) Reference Manual](./docs/user/ppl/index.rst), [OpenSearch SQL/PPL Engine Development Manual](./docs/dev/index.md) and [Technical Documentation](https://opensearch.org/docs/latest/search-plugins/sql/index/) for detailed information on installing and configuring plugin.

## Forum

For additional help with the plugin, including questions about opening an issue, visit the OpenSearch [Forum](https://forum.opensearch.org/c/plugins/sql/8).

## Contributing

See [developer guide](DEVELOPER_GUIDE.rst) and [how to contribute to this project](CONTRIBUTING.md).

## Attribution

This project is based on the Apache 2.0-licensed [elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql) project. Thank you [eliranmoyal](https://github.com/eliranmoyal), [shi-yuan](https://github.com/shi-yuan), [ansjsun](https://github.com/ansjsun) and everyone else who contributed great code to that project. Read this for more details [Attributions](./docs/attributions.md).

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](./CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue.

## License

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](./NOTICE) for details.
