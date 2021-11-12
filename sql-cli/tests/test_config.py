"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import os
import stat
import pytest

from src.opensearch_sql_cli.config import ensure_dir_exists


class TestConfig:
    def test_ensure_file_parent(self, tmpdir):
        subdir = tmpdir.join("subdir")
        rcfile = subdir.join("rcfile")
        ensure_dir_exists(str(rcfile))

    def test_ensure_existing_dir(self, tmpdir):
        rcfile = str(tmpdir.mkdir("subdir").join("rcfile"))

        # should just not raise
        ensure_dir_exists(rcfile)

    def test_ensure_other_create_error(self, tmpdir):
        subdir = tmpdir.join("subdir")
        rcfile = subdir.join("rcfile")

        # trigger an oserror that isn't "directory already exists"
        os.chmod(str(tmpdir), stat.S_IREAD)

        with pytest.raises(OSError):
            ensure_dir_exists(str(rcfile))
