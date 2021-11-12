"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import mock
from textwrap import dedent

from click.testing import CliRunner

from .utils import estest, load_data, TEST_INDEX_NAME
from src.opensearch_sql_cli.main import cli
from src.opensearch_sql_cli.opensearchsql_cli import OpenSearchSqlCli

INVALID_ENDPOINT = "http://invalid:9200"
ENDPOINT = "http://localhost:9200"
QUERY = "select * from %s" % TEST_INDEX_NAME


class TestMain:
    @estest
    def test_explain(self, connection):
        doc = {"a": "aws"}
        load_data(connection, doc)

        err_message = "Can not connect to endpoint %s" % INVALID_ENDPOINT
        expected_output = {
            "root": {
                "name": "ProjectOperator",
                "description": {"fields": "[a]"},
                "children": [
                    {
                        "name": "OpenSearchIndexScan",
                        "description": {
                            "request": 'OpenSearchQueryRequest(indexName=opensearchsql_cli_test, sourceBuilder={"from":0,"size":200,"timeout":"1m","_source":{"includes":["a"],"excludes":[]}}, searchDone=false)'
                        },
                        "children": [],
                    }
                ],
            }
        }
        expected_tabular_output = dedent(
            """\
            fetched rows / total rows = 1/1
            +-----+
            | a   |
            |-----|
            | aws |
            +-----+"""
        )

        with mock.patch("src.opensearch_sql_cli.main.click.echo") as mock_echo, mock.patch(
            "src.opensearch_sql_cli.main.click.secho"
        ) as mock_secho:
            runner = CliRunner()

            # test -q -e
            result = runner.invoke(cli, [f"-q{QUERY}", "-e"])
            mock_echo.assert_called_with(expected_output)
            assert result.exit_code == 0

            # test -q
            result = runner.invoke(cli, [f"-q{QUERY}"])
            mock_echo.assert_called_with(expected_tabular_output)
            assert result.exit_code == 0

            # test invalid endpoint
            runner.invoke(cli, [INVALID_ENDPOINT, f"-q{QUERY}", "-e"])
            mock_secho.assert_called_with(message=err_message, fg="red")

    @estest
    def test_cli(self):
        with mock.patch.object(OpenSearchSqlCli, "connect") as mock_connect, mock.patch.object(
            OpenSearchSqlCli, "run_cli"
        ) as mock_run_cli:
            runner = CliRunner()
            result = runner.invoke(cli)

            mock_connect.assert_called_with(ENDPOINT, None)
            mock_run_cli.asset_called()
            assert result.exit_code == 0
