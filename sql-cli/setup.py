"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
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
import re
import ast

from setuptools import setup, find_packages

install_requirements = [
    "click == 7.1.1",
    "prompt_toolkit == 2.0.6",
    "Pygments >= 2.7.4",
    "cli_helpers[styles] == 1.2.1",
    "elasticsearch == 7.10.1",
    "pyfiglet == 0.8.post1",
    "boto3 == 1.16.29",
    "requests-aws4auth == 0.9",
]

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("src/opensearch_sql_cli/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

description = "OpenSearch SQL CLI with auto-completion and syntax highlighting"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="opensearch-sql-cli",
    author="OpenSearch",
    author_email="opensearch-infra@amazon.com",
    version=version,
    license="Apache 2.0",
    url="https://docs-beta.opensearch.org/search-plugins/sql/cli/",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    package_data={"opensearch_sql_cli": ["conf/clirc", "opensearch_literals/opensearch_literals.json"]},
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=install_requirements,
    entry_points={"console_scripts": ["opensearchsql=opensearch_sql_cli.main:cli"]},
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.0'
)
