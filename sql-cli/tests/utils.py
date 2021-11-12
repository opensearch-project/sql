"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import json
import pytest
from elasticsearch import ConnectionError, helpers, ConnectionPool

from src.opensearch_sql_cli.opensearch_connection import OpenSearchConnection
from src.opensearch_sql_cli.utils import OutputSettings
from src.opensearch_sql_cli.formatter import Formatter

TEST_INDEX_NAME = "opensearchsql_cli_test"
ENDPOINT = "http://localhost:9200"


def create_index(test_executor):
    opensearch = test_executor.client
    opensearch.indices.create(index=TEST_INDEX_NAME)


def delete_index(test_executor):
    opensearch = test_executor.client
    opensearch.indices.delete(index=TEST_INDEX_NAME)


def close_connection(opensearch):
    ConnectionPool.close(opensearch)


def load_file(test_executor, filename="accounts.json"):
    opensearch = test_executor.client

    filepath = "./test_data/" + filename

    # generate iterable data
    def load_json():
        with open(filepath, "r") as f:
            for line in f:
                yield json.loads(line)

    helpers.bulk(opensearch, load_json(), index=TEST_INDEX_NAME)


def load_data(test_executor, doc):
    opensearch = test_executor.client
    opensearch.index(index=TEST_INDEX_NAME, body=doc)
    opensearch.indices.refresh(index=TEST_INDEX_NAME)


def get_connection():
    test_es_connection = OpenSearchConnection(endpoint=ENDPOINT)
    test_es_connection.set_connection(is_reconnect=True)

    return test_es_connection


def run(test_executor, query, use_console=True):
    data = test_executor.execute_query(query=query, use_console=use_console)
    settings = OutputSettings(table_format="psql")
    formatter = Formatter(settings)

    if data:
        res = formatter.format_output(data)
        res = "\n".join(res)

        return res


# build client for testing
try:
    connection = get_connection()
    CAN_CONNECT_TO_ES = True

except ConnectionError:
    CAN_CONNECT_TO_ES = False

# use @estest annotation to mark test functions
estest = pytest.mark.skipif(
    not CAN_CONNECT_TO_ES, reason="Need a OpenSearch server running at localhost:9200 accessible"
)
