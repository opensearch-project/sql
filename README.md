<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

- [OpenSearch SQL](#opensearch-sql)
  - [Code Summary](#code-summary)
    - [SQL Engine](#sql-engine)
    - [Repository Checks](#repository-checks)
    - [Issues](#issues)
  - [Getting Started](#getting-started)
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

## Getting Started

To use the SQL plugin, send a POST request to the `_plugins/_sql` endpoint:

```bash
POST /_plugins/_sql
{
  "query": "SELECT * FROM my-index LIMIT 10"
}
```

For PPL, use the `_plugins/_ppl` endpoint:

```bash
POST /_plugins/_ppl
{
  "query": "source=my-index | fields firstName, lastName"
}
```

## Highlights

... (rest of the sections)