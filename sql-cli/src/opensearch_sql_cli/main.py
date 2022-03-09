from __future__ import unicode_literals

"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""


import click
import sys

from .config import config_location
from .opensearch_connection import OpenSearchConnection
from .utils import OutputSettings
from .opensearchsql_cli import OpenSearchSqlCli
from .formatter import Formatter

click.disable_unicode_literals_warning = True


@click.command()
@click.argument("endpoint", default="http://localhost:9200")
@click.option("-q", "--query", "query", type=click.STRING, help="Run single query in non-interactive mode")
@click.option("-e", "--explain", "explain", is_flag=True, help="Explain SQL to OpenSearch DSL")
@click.option(
    "--clirc",
    default=config_location() + "config",
    envvar="CLIRC",
    help="Location of clirc file.",
    type=click.Path(dir_okay=False),
)
@click.option(
    "-f",
    "--format",
    "result_format",
    type=click.STRING,
    default="jdbc",
    help="Specify format of output, jdbc/csv. By default, it's jdbc",
)
@click.option(
    "-v",
    "--vertical",
    "is_vertical",
    is_flag=True,
    default=False,
    help="Convert output from horizontal to vertical. Only used for non-interactive mode",
)
@click.option("-u", "--username", help="Username to connect to the OpenSearch")
@click.option("-w", "--password", help="password corresponding to username")
@click.option(
    "-p",
    "--pager",
    "always_use_pager",
    is_flag=True,
    default=False,
    help="Always use pager to display output. If not specified, smart pager mode will be used according to the \
         length/width of output",
)
@click.option(
    "--aws-auth",
    "use_aws_authentication",
    is_flag=True,
    default=False,
    help="Use AWS sigV4 to connect to AWS ELasticsearch domain",
)
@click.option(
    "-l",
    "--language",
    "query_language",
    type=click.STRING,
    default="sql",
    help="SQL OR PPL",
)
def cli(
    endpoint,
    query,
    explain,
    clirc,
    result_format,
    is_vertical,
    username,
    password,
    always_use_pager,
    use_aws_authentication,
    query_language,
):
    """
    Provide endpoint for OpenSearch client.
    By default, it uses http://localhost:9200 to connect.
    """

    if username and password:
        http_auth = (username, password)
    else:
        http_auth = None

    # TODO add validation for endpoint to avoid the cost of connecting to some obviously invalid endpoint

    # handle single query without more interaction with user
    if query:
        opensearch_executor = OpenSearchConnection(endpoint, http_auth, use_aws_authentication)
        opensearch_executor.set_connection()
        if explain:
            output = opensearch_executor.execute_query(query, explain=True, use_console=False)
        else:
            output = opensearch_executor.execute_query(query, output_format=result_format, use_console=False)
            if output and result_format == "jdbc":
                settings = OutputSettings(table_format="psql", is_vertical=is_vertical)
                formatter = Formatter(settings)
                output = formatter.format_output(output)
                output = "\n".join(output)

        click.echo(output)
        sys.exit(0)

    # use console to interact with user
    opensearchsql_cli = OpenSearchSqlCli(
        clirc_file=clirc,
        always_use_pager=always_use_pager,
        use_aws_authentication=use_aws_authentication,
        query_language=query_language,
    )
    opensearchsql_cli.connect(endpoint, http_auth)
    opensearchsql_cli.run_cli()


if __name__ == "__main__":
    cli()
