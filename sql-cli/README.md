[![SQL CLI Test and Build](https://github.com/opensearch-project/sql/workflows/SQL%20CLI%20Test%20and%20Build/badge.svg)](https://github.com/opensearch-project/sql/actions)
[![Latest Version](https://img.shields.io/pypi/v/opensearchsql.svg)](https://pypi.python.org/pypi/opensearchsql/)
[![Documentation](https://img.shields.io/badge/documentation-blue.svg)](https://docs-beta.opensearch.org/docs/sql/cli/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://forum.opensearch.org/c/plugins/sql)
![PyPi Downloads](https://img.shields.io/pypi/dm/opensearchsql.svg)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

# OpenSearch SQL CLI

The SQL CLI component in OpenSearch is a stand-alone Python application and can be launched by a 'wake' word `opensearchsql`. 

It only supports [OpenSearch SQL Plugin](https://docs-beta.opensearch.org/search-plugins/sql/index/)
You must have the OpenSearch SQL plugin installed to your OpenSearch instance to connect. 
Users can run this CLI from Unix like OS or Windows, and connect to any valid OpenSearch end-point such as Amazon OpenSearch Service.

![](./screenshots/usage.gif)



## Features

* Multi-line input
* Autocomplete for SQL syntax and index names
* Syntax highlighting
* Formatted output:
* Tabular format
* Field names with color
* Enabled horizontal display (by default) and vertical display when output is too wide for your terminal, for better visualization
* Pagination for large output
* Connect to OpenSearch with/without security enabled on either **OpenSearch or Amazon OpenSearch Service domains**.
* Supports loading configuration files
* Supports all SQL plugin queries

## Version
Unlike plugins which use 4-digit version number. SQl-CLI uses `x.x.x` as version number same as other python packages in OpenSearch family. As a client for OpenSearch SQL, it has independent release. 
SQL-CLI should be compatible to all OpenSearch SQL versions. However since the codebase is in a monorepo, 
so we'll cut and name sql-cli release branch and tags differently. E.g.
```
release branch: sql-cli-1.0
release tag: sql-cli-v1.0.0 
```

## Install

Launch your local OpenSearch instance and make sure you have the OpenSearch SQL plugin installed.

To install the SQL CLI:


1. We suggest you install and activate a python3 virtual environment to avoid changing your local environment:

    ```
    pip install virtualenv
    virtualenv venv
    cd venv
    source ./bin/activate
    ```


1. Install the CLI:

    ```
    pip3 install opensearchsql
    ```

    The SQL CLI only works with Python 3, since Python 2 is no longer maintained since 01/01/2020. See https://pythonclock.org/


1. To launch the CLI, run:

    ```
    opensearchsql https://localhost:9200 --username admin --password admin
    ```
    By default, the `opensearchsql` command connects to [http://localhost:9200](http://localhost:9200/).



## Configure

When you first launch the SQL CLI, a configuration file is automatically created at `~/.config/opensearchsql-cli/config` (for MacOS and Linux), the configuration is auto-loaded thereafter.

You can also configure the following connection properties:


* `endpoint`: You do not need to specify an option, anything that follows the launch command `opensearchsql` is considered as the endpoint. If you do not provide an endpoint, by default, the SQL CLI connects to [http://localhost:9200](http://localhost:9200/).
* `-u/-w`: Supports username and password for HTTP basic authentication, such as:
    * OpenSearch with [OpenSearch Security Plugin](https://docs-beta.opensearch.org/security-plugin/index/) installed
    * Amazon OpenSearch Service domain with [Fine Grained Access Control](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/fgac.html) enabled
* `--aws-auth`: Turns on AWS sigV4 authentication to connect to an Amazon Elasticsearch Service endpoint. Use with the AWS CLI (`aws configure`) to retrieve the local AWS configuration to authenticate and connect.

For a list of all available configurations, see [clirc](https://github.com/opensearch-project/sql/blob/main/sql-cli/src/opensearch_sql_cli/conf/clirc).



## Using the CLI

1. Save the sample [accounts test data](https://github.com/opensearch-project/sql/blob/main/integ-test/src/test/resources/accounts.json) file.
2. Index the sample data.

    ```
    curl -H "Content-Type: application/x-ndjson" -POST https://localhost:9200/data/_bulk -u admin:admin --insecure --data-binary "@accounts.json"
    ```


1. Run a simple SQL command in OpenSearch SQL CLI:

    ```
    SELECT * FROM accounts;
    ```

    By default, you see a maximum output of 200 rows. To show more results, add a `LIMIT` clause with the desired value.

The CLI supports all types of query that OpenSearch SQL supports. Refer to [OpenSearch SQL basic usage documentation.](https://github.com/opensearch-project/sql#basic-usage)


## Query options

Run single query from command line with options


* `--help`: help page for options
* `-q`: follow by a single query
* `-f`: support *jdbc/raw* format output
* `-v`: display data vertically
* `-e`: translate sql to DSL

## CLI Options

* `-l`: Query language option. Available options are [sql, ppl]. By default it's using sql.
* `-p`: always use pager to display output
* `--clirc`: provide path of config file to load

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](./CODE_OF_CONDUCT.md).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue for security bugs you report.

## Licensing

See the [LICENSE](./LICENSE.TXT) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](./NOTICE) for details.

