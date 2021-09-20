
[![Test and Build Workflow](https://github.com/opendistro-for-elasticsearch/sql/workflows/Java%20CI/badge.svg)](https://github.com/opendistro-for-elasticsearch/sql/actions)
[![codecov](https://codecov.io/gh/opendistro-for-elasticsearch/sql/branch/develop/graph/badge.svg)](https://codecov.io/gh/opendistro-for-elasticsearch/sql)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://docs-beta.opensearch.org/search-plugins/sql/endpoints/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/sql/)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

- [OpenSearch SQL](#opensearch-sql)
- [Highlights](#highlights)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Attribution](#attribution)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)


# OpenSearch SQL

OpenSearch enables you to extract insights out of OpenSearch using the familiar SQL or Piped Processing Language (PPL) query syntax. Use aggregations, group by, and where clauses to investigate your data. Read your data as JSON documents or CSV tables so you have the flexibility to use the format that works best for you.

The following projects have been merged into this repository as separate folders as of July 9, 2020. Please refer to links below for details. This document will focus on the SQL plugin for OpenSearch.

* [SQL CLI](https://github.com/opensearch-project/sql/tree/main/sql-cli)
* [SQL JDBC](https://github.com/opensearch-project/sql/tree/main/sql-jdbc)
* [SQL ODBC](https://github.com/opensearch-project/sql/tree/main/sql-odbc)
* [Query Workbench](https://github.com/opensearch-project/sql/tree/main/workbench)


## Highlights

Besides basic filtering and aggregation, OpenSearch SQL also supports complex queries, such as querying semi-structured data, JOINs, set operations, sub-queries etc. Beyond the standard functions, OpenSearch functions are provided for better analytics and visualization. Please check our [documentation](#documentation) for more details.

Recently we have been actively improving our query engine primarily for better correctness and extensibility. Behind the scene, the new enhanced engine has already supported both SQL and Piped Processing Language. Please find more details in [SQL Engine V2 - Release Notes](./docs/dev/NewSQLEngine.md).


## Documentation

Please refer to the [SQL Language Reference Manual](./docs/user/index.rst), [Piped Processing Language (PPL) Reference Manual](./docs/user/ppl/index.rst) and [Technical Documentation](https://docs-beta.opensearch.org/) for detailed information on installing and configuring plugin.


## Contributing

See [developer guide](DEVELOPER_GUIDE.rst) and [how to contribute to this project](CONTRIBUTING.md).


## Attribution

This project is based on the Apache 2.0-licensed [elasticsearch-sql](https://github.com/NLPchina/elasticsearch-sql) project. Thank you [eliranmoyal](https://github.com/eliranmoyal), [shi-yuan](https://github.com/shi-yuan), [ansjsun](https://github.com/ansjsun) and everyone else who contributed great code to that project. Read this for more details [Attributions](./docs/attributions.md).


## Code of Conduct

This project has adopted an [Open Source Code of Conduct](./CODE_OF_CONDUCT.md).


## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## License

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
