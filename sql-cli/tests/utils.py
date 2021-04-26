"""
SPDX-License-Identifier: Apache-2.0

The OpenSearch Contributors require contributions made to
this file be licensed under the Apache-2.0 license or a
compatible open source license.

Modifications Copyright OpenSearch Contributors. See
GitHub history for details.
"""
"""
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

    http://www.apache.org/licenses/LICENSE-2.0

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
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