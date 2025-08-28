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
from functools import partial

import click
import zc.customdoctests
from opensearch_sql_cli.formatter import Formatter
from opensearch_sql_cli.opensearch_connection import OpenSearchConnection
from opensearch_sql_cli.utils import OutputSettings
from opensearchpy import OpenSearch, helpers


# =============================================================================
# CONSTANTS AND CONFIGURATION
# =============================================================================

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
    'occupation': 'occupation.json',
    'worker': 'worker.json',
    'work_information': 'work_information.json',
    'events': 'events.json'
}

DEBUG_MODE = os.environ.get('DOCTEST_DEBUG', 'false').lower() == 'true'


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def debug(message):
    if DEBUG_MODE:
        print(f"[DEBUG] {message}", file=sys.stderr)


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


# =============================================================================
# CONNECTION AND COMMAND CLASSES
# =============================================================================

class DocTestConnection(OpenSearchConnection):

    def __init__(self, query_language="sql"):
        super(DocTestConnection, self).__init__(endpoint=ENDPOINT, query_language=query_language)
        self.set_connection()
        settings = OutputSettings(table_format="psql", is_vertical=False)
        self.formatter = Formatter(settings)

    def process(self, statement):
        debug(f"Executing {self.query_language.upper()} query: {statement}")
        
        data = self.execute_query(statement, use_console=False)
        debug(f"Query result: {data}")
        
        if data is None:
            debug("Query returned None - this may indicate an error or unsupported function")
            print("Error: Query returned no data")
            return
            
        output = self.formatter.format_output(data)
        output = "\n".join(output)
        click.echo(output)


class CalciteManager:
    
    @staticmethod
    def enable():
        try:
            import requests
            calcite_settings = {
                "transient": {
                    "plugins.calcite.enabled": True
                }
            }
            response = requests.put(f"{ENDPOINT}/_plugins/_query/settings", 
                                  json=calcite_settings, 
                                  timeout=10)
            debug(f"Calcite enable response: {response.status_code} - {response.text}")
            
            if response.status_code == 200:
                debug("Calcite enabled successfully")
                return True
            else:
                debug(f"Failed to enable Calcite: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            debug(f"Failed to enable Calcite: {e}")
            return False


# =============================================================================
# TEST DATA MANAGEMENT
# =============================================================================

class TestDataManager:
    
    def __init__(self):
        self.client = OpenSearch([ENDPOINT], verify_certs=True)
    
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

        helpers.bulk(self.client, load_json(), stats_only=True, index=index_name, refresh='wait_for')

    def load_all_test_data(self):
        for index_name, filename in TEST_DATA.items():
            if filename is not None:
                self.load_file(filename, index_name)
            else:
                debug(f"Skipping index '{index_name}' - filename is None")

    def cleanup_indices(self):
        indices_to_delete = list(TEST_DATA.keys())
        self.client.indices.delete(index=indices_to_delete, ignore_unavailable=True)


# =============================================================================
# DOCTEST PARSERS AND TRANSFORMERS
# =============================================================================

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


# =============================================================================
# TEST SETUP AND TEARDOWN
# =============================================================================

test_data_manager = None


def get_test_data_manager():
    global test_data_manager
    if test_data_manager is None:
        test_data_manager = TestDataManager()
    return test_data_manager


def set_up_test_indices_with_calcite(test):
    test.globs['sql_cmd'] = DocTestConnection(query_language="sql")
    test.globs['ppl_cmd'] = DocTestConnection(query_language="ppl")
    CalciteManager.enable()
    get_test_data_manager().load_all_test_data()


def set_up_test_indices_without_calcite(test):
    test.globs['sql_cmd'] = DocTestConnection(query_language="sql")
    test.globs['ppl_cmd'] = DocTestConnection(query_language="ppl")
    get_test_data_manager().load_all_test_data()


def tear_down(test):
    get_test_data_manager().cleanup_indices()


# =============================================================================
# DOCTEST SUITE CREATION
# =============================================================================

docsuite = partial(doctest.DocFileSuite,
                   tearDown=tear_down,
                   parser=sql_cli_parser,
                   optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
                   encoding='utf-8')

doctest_file = partial(os.path.join, '../docs')


def doctest_files(items):
    return (doctest_file(item) for item in items)


class DocTests(unittest.TestSuite):
    def run(self, result, debug=False):
        super().run(result, debug)


def create_bash_suite(fn, setup_func):
    return docsuite(
        fn,
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


def create_cli_suite(fn, parser, setup_func):
    return docsuite(
        fn,
        parser=parser,
        setUp=setup_func
    )


# =============================================================================
# TEST LOADING AND EXECUTION
# =============================================================================

def load_tests(loader, suite, ignore):
    tests = []
    
    with open('../docs/category.json') as json_file:
        category = json.load(json_file)

    for category_name, docs in category.items():
        if not docs:
            continue
            
        if requires_calcite(category_name):
            setup_func = set_up_test_indices_with_calcite
            print(f"Loading {len(docs)} files from {category_name} with Calcite enabled")
        else:
            setup_func = set_up_test_indices_without_calcite
            print(f"Loading {len(docs)} files from {category_name} without Calcite")

        if category_name.startswith('bash'):
            for fn in doctest_files(docs):
                tests.append(create_bash_suite(fn, setup_func))
        elif category_name.startswith('sql_cli'):
            for fn in doctest_files(docs):
                tests.append(create_cli_suite(fn, sql_cli_parser, setup_func))
        elif category_name.startswith('ppl_cli'):
            for fn in doctest_files(docs):
                tests.append(create_cli_suite(fn, ppl_cli_parser, setup_func))

    random.shuffle(tests)
    return DocTests(tests)


# =============================================================================
# SINGLE FILE TESTING FUNCTIONALITY
# =============================================================================

class SingleFileTestRunner:
    
    def __init__(self):
        self.test_data_manager = None
    
    def get_test_data_manager(self):
        if self.test_data_manager is None:
            self.test_data_manager = TestDataManager()
        return self.test_data_manager
    
    def find_doc_file(self, filename_or_path):
        if os.path.exists(filename_or_path):
            return filename_or_path
        
        if not os.path.sep in filename_or_path:
            try:
                with open('../docs/category.json') as json_file:
                    category = json.load(json_file)
                
                all_docs = []
                for cat_name, docs in category.items():
                    all_docs.extend(docs)
                
                search_filename = filename_or_path
                if not search_filename.endswith('.rst'):
                    search_filename += '.rst'
                
                matches = [doc for doc in all_docs if doc.endswith(search_filename)]
                
                if len(matches) == 1:
                    found_path = f"../docs/{matches[0]}"
                    print(f"Found: {found_path}")
                    return found_path
                elif len(matches) > 1:
                    print(f"Multiple files found matching '{search_filename}':")
                    for match in matches:
                        print(f"  ../docs/{match}")
                    print("Please specify the full path or a more specific filename.")
                    return None
                else:
                    print(f"No documentation file found matching '{search_filename}'")
                    print("Use --list to see all available files")
                    return None
                    
            except Exception as e:
                print(f"Error searching for file: {e}")
                return None
        
        if not filename_or_path.startswith('../docs/'):
            potential_path = f"../docs/{filename_or_path}"
            if os.path.exists(potential_path):
                return potential_path
        
        return filename_or_path

    def determine_doc_type(self, file_path):
        try:
            with open('../docs/category.json') as json_file:
                category = json.load(json_file)
            
            rel_path = os.path.relpath(file_path, '../docs')
            
            for category_name, docs in category.items():
                if rel_path in docs:
                    return category_name
            
            if '/ppl/' in file_path:
                return 'ppl_cli'
            elif '/sql/' in file_path or '/dql/' in file_path:
                return 'sql_cli'
            else:
                return 'bash'
                
        except Exception as e:
            print(f"Warning: Could not determine doc type from category.json: {e}")
            if '/ppl/' in file_path:
                return 'ppl_cli'
            elif '/sql/' in file_path or '/dql/' in file_path:
                return 'sql_cli'
            else:
                return 'bash'

    def run_single_doctest(self, file_path, verbose=False, endpoint=None):
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist")
            return False
        
        if endpoint:
            global ENDPOINT
            ENDPOINT = endpoint
            print(f"Using custom endpoint: {endpoint}")
        
        doc_type = self.determine_doc_type(file_path)
        print(f"Detected doc type: {doc_type}")
        print(f"Running doctest for: {file_path}")
        
        optionflags = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
        if verbose:
            optionflags |= doctest.REPORT_NDIFF
        
        if requires_calcite(doc_type):
            setup_func = set_up_test_indices_with_calcite
            print(f"Using Calcite-enabled setup for category: {doc_type}")
        else:
            setup_func = set_up_test_indices_without_calcite
            print(f"Using standard setup (no Calcite) for category: {doc_type}")

        if doc_type.startswith('bash'):
            parser = bash_parser
            globs = {
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
        elif doc_type.startswith('ppl_cli'):
            parser = ppl_cli_parser
            globs = {}
        else:
            parser = sql_cli_parser
            globs = {}
        
        try:
            print("Setting up test environment...")
            
            suite = doctest.DocFileSuite(
                file_path,
                parser=parser,
                setUp=setup_func,
                tearDown=tear_down,
                optionflags=optionflags,
                encoding='utf-8',
                globs=globs
            )
            
            runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
            result = runner.run(suite)
            
            if result.wasSuccessful():
                print(f"\nSUCCESS: All tests in {os.path.basename(file_path)} passed!")
                print(f"Tests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
                return True
            else:
                print(f"\nFAILED: {len(result.failures + result.errors)} test(s) failed in {os.path.basename(file_path)}")
                print(f"Tests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
                
                if verbose:
                    print("\nDetailed failure information:")
                    for failure in result.failures:
                        print(f"\n--- FAILURE in {failure[0]} ---")
                        print(failure[1])
                    for error in result.errors:
                        print(f"\n--- ERROR in {error[0]} ---")
                        print(error[1])
                else:
                    print("Use --verbose for detailed failure information")
                
                return False
                
        except Exception as e:
            print(f"Error running doctest: {e}")
            if verbose:
                import traceback
                traceback.print_exc()
            return False

    def list_available_docs(self):
        try:
            with open('../docs/category.json') as json_file:
                category = json.load(json_file)
            
            print("Available documentation files for testing:")
            
            total_docs = 0
            for category_name, docs in category.items():
                if not docs:
                    continue
                    
                calcite_status = " (Calcite enabled)" if requires_calcite(category_name) else " (no Calcite)"
                print(f"\n{category_name.upper()} docs ({len(docs)} files){calcite_status}:")
                for doc in sorted(docs):
                    print(f"  ../docs/{doc}")
                total_docs += len(docs)
            
            print(f"\nTotal: {total_docs} documentation files available for testing")
                
        except Exception as e:
            print(f"Error reading category.json: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run doctest for one or more documentation files, or all files if no arguments provided",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_docs.py                    # Run all tests (default behavior)
  python test_docs.py stats              # Run single file (extension optional)
  python test_docs.py stats.rst --verbose
  python test_docs.py stats fields basics
  python test_docs.py ../docs/user/ppl/cmd/stats.rst --endpoint http://localhost:9201

Performance Tips:
  - Use --verbose for detailed debugging information
  - Ensure OpenSearch is running on the specified endpoint before testing
  - Extension .rst can be omitted for convenience
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
    
    runner = SingleFileTestRunner()
    
    if args.list:
        runner.list_available_docs()
        return
    
    if not args.file_paths:
        print("No specific files provided. Running full doctest suite...")
        unittest.main(module=None, argv=['test_docs.py'], exit=False)
        return
    
    all_success = True
    total_files = len(args.file_paths)
    
    for i, file_path in enumerate(args.file_paths, 1):
        if total_files > 1:
            print(f"\n{'='*60}")
            print(f"Testing file {i}/{total_files}: {file_path}")
            print('='*60)
        
        actual_file_path = runner.find_doc_file(file_path)
        if not actual_file_path:
            print(f"Skipping {file_path} - file not found")
            all_success = False
            continue
        
        success = runner.run_single_doctest(
            actual_file_path, 
            verbose=args.verbose,
            endpoint=args.endpoint
        )
        
        if not success:
            all_success = False
    
    if total_files > 1:
        print(f"\n{'='*60}")
        print(f"SUMMARY: Tested {total_files} files")
        if all_success:
            print("All tests passed!")
        else:
            print("Some tests failed!")
        print('='*60)
    
    sys.exit(0 if all_success else 1)


if __name__ == '__main__':
    main()
