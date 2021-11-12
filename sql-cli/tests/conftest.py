"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import os
import pytest

from .utils import create_index, delete_index, get_connection


@pytest.fixture(scope="function")
def connection():
    test_connection = get_connection()
    create_index(test_connection)

    yield test_connection
    delete_index(test_connection)


@pytest.fixture(scope="function")
def default_config_location():
    from src.opensearch_sql_cli.conf import __file__ as package_root

    package_root = os.path.dirname(package_root)
    default_config = os.path.join(package_root, "clirc")

    yield default_config


@pytest.fixture(scope="session", autouse=True)
def temp_config(tmpdir_factory):
    # this function runs on start of test session.
    # use temporary directory for conf home so user conf will not be used
    os.environ["XDG_CONFIG_HOME"] = str(tmpdir_factory.mktemp("data"))
