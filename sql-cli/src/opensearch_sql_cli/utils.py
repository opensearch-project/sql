"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import sys

from collections import namedtuple

OutputSettings = namedtuple("OutputSettings", "table_format is_vertical max_width style_output missingval")

OutputSettings.__new__.__defaults__ = (None, False, sys.maxsize, None, "null")
