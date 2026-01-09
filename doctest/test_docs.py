# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

import argparse
import doctest
import json
import os
import os.path
import random
import re
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import List

import click
import zc.customdoctests
from opensearch_sql_cli.formatter import Formatter
from opensearch_sql_cli.opensearch_connection import OpenSearchConnection
from opensearch_sql_cli.utils import OutputSettings
from opensearchpy import OpenSearch, helpers
from markdown_parser import mixed_ppl_transform


# Import Markdown parser
from markdown_parser import (
    MarkdownDocTestParser,
    ppl_markdown_transform,
    sql_markdown_transform,
)


ENDPOINT = "http://localhost:9200"

TEST_DATA = {
    'accounts': 'accounts.json',
    'people': 'people.json',
    'account2': 'account2.json',
    'nyc_taxi': 'nyc_taxi.json',
    'books': 'books.json',
    'apache': 'apache.json',
    'wildcard': 'wildcard.json',
    'nested': 'nested_objects.json',
    '.ql-datasources': 'datasources.json',
    'weblogs': 'weblogs.json',
    'json_test': 'json_test.json',
    'state_country': 'state_country.json',
    'structured': 'structured.json',
    'occupation': 'occupation.json',
    'worker': 'worker.json',
    'work_information': 'work_information.json',
    'events': 'events.json',
    'events_null': 'events_null.json',
    'events_many_hosts': 'events_many_hosts.json',
    'otellogs': 'otellogs.json',
    'time_data': 'time_test_data.json',
    'time_data2': 'time_test_data2.json',
    'time_test': 'time_test.json',
    'mvcombine_data': 'mvcombine.json',
}

DEBUG_MODE = os.environ.get('DOCTEST_DEBUG', 'false').lower() == 'true'
IGNORE_PROMETHEUS_DOCS = os.environ.get('IGNORE_PROMETHEUS_DOCS', 'false').lower() == 'true'
PROMETHEUS_DOC_FILES = {
    'user/ppl/cmd/showdatasources.rst'
}


def debug(message):
    if DEBUG_MODE:
        print(f"[DEBUG] {message}", file=sys.stderr)


def detect_doc_type_from_path(file_path):
    if '/ppl/' in file_path:
        return 'ppl_cli'
    elif '/sql/' in file_path or '/dql/' in file_path:
        return 'sql_cli'
    else:
        return 'bash'


def normalize_explain_response(data):
    if "root" in data:
        data = data["root"]

    if (request := data.get("description", {}).get("request", None)) and request.startswith("OpenSearchQueryRequest("):
        for filter_field in ["needClean", "pitId", "cursorKeepAlive", "searchAfter", "searchResponse"]:
            request = re.sub(f", {filter_field}=[A-Za-z0-9+=_-]+", "", request)
        data["description"]["request"] = request

    for child in data.get("children", []):
        normalize_explain_response(child)

    return data


def pretty_print(s):
    try:
        data = json.loads(s)
        normalize_explain_response(data)
        print(json.dumps(data, indent=2))
    except json.decoder.JSONDecodeError:
        print(s)


def requires_calcite(doc_category):
    return doc_category.endswith('_calcite')


class CategoryManager:

    def __init__(self, category_file_path='../docs/category.json'):
        self._categories = self.load_categories(category_file_path)
        self._all_docs_cache = None

    def load_categories(self, file_path):
        try:
            with open(file_path) as json_file:
                categories = json.load(json_file)
            if IGNORE_PROMETHEUS_DOCS:
                categories = {
                    category: [
                        doc for doc in docs if doc not in PROMETHEUS_DOC_FILES
                    ]
                    for category, docs in categories.items()
                }
            debug(f"Loaded {len(categories)} categories from {file_path}")

            # Validate markdown-only categories
            for category_name in categories.keys():
                self._validate_category_files(category_name, categories[category_name])

            return categories
        except Exception as e:
            raise Exception(f"Failed to load categories from {file_path}: {e}")

    def _validate_category_files(self, category_name, docs):
        """Internal method to validate category files during loading."""
        if self.is_markdown_category(category_name):
            # Markdown-only categories should not contain .rst files
            rst_files = [doc for doc in docs if doc.endswith(".rst")]
            if rst_files:
                raise Exception(
                    f"Only markdown files supported for category: {category_name}"
                )
            debug(
                f"Category {category_name} validation passed - all files are markdown"
            )
        else:
            # Non-markdown categories should only contain .rst files
            md_files = [doc for doc in docs if doc.endswith(".md")]
            if md_files:
                raise Exception(
                    f"Only .rst files supported for category: {category_name}. Markdown not yet supported."
                )
            debug(f"Category {category_name} validation passed - all files are .rst")

    def get_all_categories(self):
        return list(self._categories.keys())

    def get_category_files(self, category_name):
        return self._categories.get(category_name, [])

    def get_all_docs(self):
        if self._all_docs_cache is None:
            self._all_docs_cache = []
            for category_name, docs in self._categories.items():
                self._all_docs_cache.extend(docs)
        return self._all_docs_cache

    def find_file_category(self, file_path):
        # Convert to relative path from docs root
        if file_path.startswith('../docs/'):
            rel_path = file_path[8:]  # Remove '../docs/' prefix
        else:
            rel_path = file_path

        for category_name, docs in self._categories.items():
            if rel_path in docs:
                debug(f"Found file {rel_path} in category {category_name}")
                return category_name

        # Fallback to path-based detection
        debug(f"File {rel_path} not found in categories, using path-based detection")
        return detect_doc_type_from_path(file_path)

    def requires_calcite(self, category_name):
        return category_name.endswith("_calcite")

    def is_markdown_category(self, category_name):
        """Check if category uses Markdown files."""
        return category_name in ("ppl_cli_calcite", "bash_calcite", "bash_settings")

    def validate_category_files(self, category_name):
        """Validate that categories contain only the correct file types.

        Markdown categories should only contain .md files.
        Non-markdown categories should only contain .rst files.
        """
        docs = self.get_category_files(category_name)
        self._validate_category_files(category_name, docs)

    def get_setup_function(self, category_name):
        if self.requires_calcite(category_name):
            return set_up_test_indices_with_calcite
        else:
            return set_up_test_indices_without_calcite

    def get_parser_for_category(self, category_name):
        if category_name.startswith('bash'):
            return bash_parser
        elif category_name.startswith('ppl_cli'):
            return ppl_cli_parser
        elif category_name.startswith('sql_cli'):
            return sql_cli_parser
        else:
            # Default fallback
            return sql_cli_parser

    def find_matching_files(self, search_filename):
        # Support both .rst and .md extensions
        if not search_filename.endswith(".rst") and not search_filename.endswith(".md"):
            # Try both extensions
            all_docs = self.get_all_docs()
            matches = [
                doc
                for doc in all_docs
                if doc.endswith(search_filename + ".rst")
                or doc.endswith(search_filename + ".md")
                or doc.endswith(search_filename)
            ]
            return matches

        all_docs = self.get_all_docs()
        matches = [doc for doc in all_docs if doc.endswith(search_filename)]
        return matches


class DocTestConnection(OpenSearchConnection):

    def __init__(self, query_language="sql", endpoint=ENDPOINT):
        super(DocTestConnection, self).__init__(endpoint, query_language=query_language)
        self.set_connection()
        settings = OutputSettings(table_format="psql", is_vertical=False)
        self.formatter = Formatter(settings)

    def process(self, statement):
        debug(f"Executing {self.query_language.upper()} query: {statement}")

        try:
            data = self.execute_query(statement, use_console=False)
            debug(f"Query result: {data}")

            if data is None:
                debug(
                    "Query returned None - this may indicate an error or unsupported function"
                )
                print("Error: Query returned no data")
                return

            output = self.formatter.format_output(data)
            output = "\n".join(output)
            click.echo(output)
        except Exception as e:
            # Print detailed error information
            print(f"Error executing query: {statement}")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            if hasattr(e, "info"):
                print(f"Error info: {e.info}")
            raise


class CalciteManager:

    @staticmethod
    def set_enabled(enabled):
        import requests
        calcite_settings = {
            "transient": {
                "plugins.calcite.enabled": enabled
            }
        }
        response = requests.put(f"{ENDPOINT}/_plugins/_query/settings", 
                                json=calcite_settings, 
                                timeout=10)

        if response.status_code != 200:
            raise Exception(f"Failed to set Calcite setting: {response.status_code} {response.text}")


class DataManager:

    def __init__(self):
        self.client = OpenSearch([ENDPOINT], verify_certs=True, timeout=60)
        self.is_loaded = False

    def load_file(self, filename, index_name):
        mapping_file_path = './test_mapping/' + filename
        if os.path.isfile(mapping_file_path):
            with open(mapping_file_path, 'r') as f:
                self.client.indices.create(index=index_name, body=f.read())

        data_file_path = './test_data/' + filename
        def load_json():
            with open(data_file_path, 'r') as f:
                for line in f:
                    yield json.loads(line)

        helpers.bulk(self.client, load_json(), stats_only=True, index=index_name, refresh="wait_for")

    def load_all_test_data(self):
        if self.is_loaded:
            return

        def load_index(index_name, filename):
            if filename is not None:
                self.load_file(filename, index_name)
            else:
                debug(f"Skipping index '{index_name}' - filename is None")

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(load_index, index_name, filename): index_name
                for index_name, filename in TEST_DATA.items()
            }

            for future in as_completed(futures):
                index_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    debug(f"Error loading index '{index_name}': {str(e)}")
                    raise

        self.is_loaded = True


def sql_cli_transform(s):
    return u'sql_cmd.process({0})'.format(repr(s.strip().rstrip(';')))


def ppl_cli_transform(s):
    return u'ppl_cmd.process({0})'.format(repr(s.strip().rstrip(';')))


def bash_transform(s):
    if s.startswith("opensearchsql"):
        s = re.search(r"opensearchsql\s+-q\s+\"(.*?)\"", s).group(1)
        return u'cmd.process({0})'.format(repr(s.strip().rstrip(';')))
    return (r'pretty_print(sh("""%s""").stdout.decode("utf-8"))' % s) + '\n'


sql_cli_parser = zc.customdoctests.DocTestParser(
    ps1='os>', comment_prefix='#', transform=sql_cli_transform)

ppl_cli_parser = zc.customdoctests.DocTestParser(
    ps1='os>', comment_prefix='#', transform=ppl_cli_transform)

bash_parser = zc.customdoctests.DocTestParser(
    ps1=r'sh\$', comment_prefix='#', transform=bash_transform)


test_data_manager = None


def get_test_data_manager():
    global test_data_manager
    if test_data_manager is None:
        test_data_manager = DataManager()
    return test_data_manager


def set_up_test_indices_with_calcite(test):
    test.globs['sql_cmd'] = DocTestConnection(query_language="sql")
    test.globs['ppl_cmd'] = DocTestConnection(query_language="ppl")
    CalciteManager.set_enabled(True)
    get_test_data_manager().load_all_test_data()


def set_up_test_indices_without_calcite(test):
    test.globs['sql_cmd'] = DocTestConnection(query_language="sql")
    test.globs['ppl_cmd'] = DocTestConnection(query_language="ppl")
    CalciteManager.set_enabled(False)
    get_test_data_manager().load_all_test_data()


def tear_down(test):
    pass


docsuite = partial(doctest.DocFileSuite,
                   tearDown=tear_down,
                   optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
                   encoding='utf-8')


def get_doc_filepath(item):
    return os.path.join('../docs', item)


def get_doc_filepaths(items):
    return (get_doc_filepath(item) for item in items)


class DocTests(unittest.TestSuite):
    def run(self, result, debug=False):
        super().run(result, debug)


def create_bash_suite(filepaths, setup_func):
    return docsuite(
        *filepaths,
        parser=bash_parser,
        setUp=setup_func,
        globs={
            'sh': partial(
                subprocess.run,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=60,
                shell=True
            ),
            'pretty_print': pretty_print
        }
    )


def create_cli_suite(filepaths, parser, setup_func):
    return docsuite(
        *filepaths,
        parser=parser,
        setUp=setup_func
    )


def create_markdown_suite(filepaths, category_name, setup_func):
    """
    Create test suite for Markdown files.

    Args:
        filepaths: List of Markdown file paths
        category_name: Category name (e.g., 'ppl_cli_calcite')
        setup_func: Setup function to initialize test environment

    Returns:
        doctest.DocTestSuite
    """

    # Determine transform based on category
    if "sql" in category_name:
        transform = sql_markdown_transform
        input_langs = ["sql"]
    elif "ppl" in category_name:
        transform = mixed_ppl_transform
        input_langs = ["ppl", "bash ppl"]
    elif "bash" in category_name:
        transform = mixed_ppl_transform
        input_langs = ["bash", "bash ppl", "sh"]
    else:
        # Default to PPL
        transform = mixed_ppl_transform
        input_langs = ["ppl", "sql", "bash ppl"]

    parser = MarkdownDocTestParser(
        input_languages=input_langs,
        output_languages=["text", "console", "output", "json", "yaml"],
        transform=transform,
    )

    # Prepare globs for bash commands
    test_globs = {}
    if "bash" in category_name:
        test_globs = {
            "sh": partial(
                subprocess.run,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=60,
                shell=True,
            ),
            "pretty_print": pretty_print,
        }

    return docsuite(
        *filepaths,
        parser=parser,
        setUp=setup_func,
        globs=test_globs,
    )


# Entry point for unittest discovery
def load_tests(loader, suite, ignore):
    tests = []
    settings_tests = []
    category_manager = CategoryManager()

    for category_name in category_manager.get_all_categories():
        docs = category_manager.get_category_files(category_name)
        if not docs:
            continue

        suite = get_test_suite(category_manager, category_name, get_doc_filepaths(docs))
        if 'settings' in category_name:
            settings_tests.append(suite)
        else:
            tests.append(suite)
    random.shuffle(tests)
    if settings_tests:
        random.shuffle(settings_tests)
        tests.extend(settings_tests)
    return DocTests(tests)

def get_test_suite(category_manager: CategoryManager, category_name, filepaths):
    setup_func = category_manager.get_setup_function(category_name)

    if category_manager.is_markdown_category(category_name):
        return create_markdown_suite(list(filepaths), category_name, setup_func)
    elif category_name.startswith("bash"):
        return create_bash_suite(filepaths, setup_func)
    else:
        parser = category_manager.get_parser_for_category(category_name)
        return create_cli_suite(filepaths, parser, setup_func)

def list_available_docs(category_manager: CategoryManager):
    categories = category_manager.get_all_categories()

    print(f"Available documentation files for testing:\n")

    total = 0
    for category_name in categories:
        files = category_manager.get_category_files(category_name)
        total += len(files)
        print(f"{category_name} docs ({len(files)} files):\n")
        for doc in sorted(files):
            print(f"  ../docs/{doc}\n")

    print(f"Total: {total} documentation files available for testing\n")


def resolve_files(category_manager: CategoryManager, file_paths: List[str]):
    result = []
    for file_param in file_paths:
        resolved_files = category_manager.find_matching_files(file_param)
        # add each file if not already in the list
        for f in resolved_files:
            if f not in result:
                result.append(f)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run doctest for one or more documentation files, or all files if no arguments provided",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_docs.py                    # Run all tests (default behavior)
  python test_docs.py stats              # Run single file (extension optional)
  python test_docs.py stats.rst --verbose
  python test_docs.py stats fields basics  # Run multiple files
  python test_docs.py cmd                # Run all files matching 'cmd' (if multiple found)
  python test_docs.py ../docs/user/ppl/cmd/stats.rst --endpoint http://localhost:9201

Performance Tips:
  - Use --verbose for detailed debugging information
  - Ensure OpenSearch is running on the specified endpoint before testing
  - Extension .rst can be omitted for convenience
  - If a filename matches multiple files, all matches will be executed
        """
    )

    parser.add_argument('file_paths', nargs='*', help='Path(s) to the documentation file(s) to test')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose output with detailed diff information')
    parser.add_argument('--endpoint', '-e', default=None,
                       help='Custom OpenSearch endpoint (default: http://localhost:9200)')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List all available documentation files')

    args = parser.parse_args()
    category_manager = CategoryManager()

    if args.list:
        list_available_docs(category_manager)
        return

    if not args.file_paths:
        print("No specific files provided. Running full doctest suite...")
        unittest.main(module=None, argv=['test_docs.py'], exit=False)
        return

    if args.endpoint:
        global ENDPOINT
        ENDPOINT = args.endpoint
        print(f"Using custom endpoint: {args.endpoint}")

    all_files_to_test = resolve_files(category_manager, args.file_paths)

    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)

    all_success = True
    for file_path in all_files_to_test:
        doc_type = category_manager.find_file_category(file_path)
        suite = get_test_suite(category_manager, doc_type, [get_doc_filepath(file_path)])
        result = runner.run(suite)
        all_success = all_success and result.wasSuccessful()

    sys.exit(0 if all_success else 1)


if __name__ == '__main__':
    main()
