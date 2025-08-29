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

ENDPOINT = "http://localhost:9200"
ACCOUNTS = "accounts"
EMPLOYEES = "employees"
PEOPLE = "people"
ACCOUNT2 = "account2"
NYC_TAXI = "nyc_taxi"
BOOKS = "books"
APACHE = "apache"
WILDCARD = "wildcard"
NESTED = "nested"
DATASOURCES = ".ql-datasources"
WEBLOGS = "weblogs"
JSON_TEST = "json_test"
STATE_COUNTRY = "state_country"
OCCUPATION = "occupation"
WORKER = "worker"
WORK_INFORMATION = "work_information"

class DocTestConnection(OpenSearchConnection):

    def __init__(self, query_language="sql"):
        super(DocTestConnection, self).__init__(endpoint=ENDPOINT, query_language=query_language)
        self.set_connection()

        settings = OutputSettings(table_format="psql", is_vertical=False)
        self.formatter = Formatter(settings)

    def process(self, statement):
        data = self.execute_query(statement, use_console=False)
        output = self.formatter.format_output(data)
        output = "\n".join(output)

        click.echo(output)


"""
For _explain requests, there are several additional request fields that will inconsistently
appear/change depending on underlying cluster state. This method normalizes these responses in-place
to make _explain doctests more consistent.

If the passed response is not an _explain response, the input is left unmodified.
"""
def normalize_explain_response(data):
    if "root" in data:
        data = data["root"]

    if (request := data.get("description", {}).get("request", None)) and request.startswith("OpenSearchQueryRequest("):
        for filter_field in ["needClean", "pitId", "cursorKeepAlive", "searchAfter", "searchResponse"]:
            # The value of PIT may contain `+=_-`.
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


sql_cmd = DocTestConnection(query_language="sql")
ppl_cmd = DocTestConnection(query_language="ppl")
test_data_client = OpenSearch([ENDPOINT], verify_certs=True)


def sql_cli_transform(s):
    return u'sql_cmd.process({0})'.format(repr(s.strip().rstrip(';')))


def ppl_cli_transform(s):
    return u'ppl_cmd.process({0})'.format(repr(s.strip().rstrip(';')))


def bash_transform(s):
    # TODO: add ppl support, be default cli uses sql
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


def set_up_test_indices(test):
    set_up(test)
    load_file("accounts.json", index_name=ACCOUNTS)
    load_file("people.json", index_name=PEOPLE)
    load_file("account2.json", index_name=ACCOUNT2)
    load_file("nyc_taxi.json", index_name=NYC_TAXI)
    load_file("books.json", index_name=BOOKS)
    load_file("apache.json", index_name=APACHE)
    load_file("wildcard.json", index_name=WILDCARD)
    load_file("nested_objects.json", index_name=NESTED)
    load_file("datasources.json", index_name=DATASOURCES)
    load_file("weblogs.json", index_name=WEBLOGS)
    load_file("json_test.json", index_name=JSON_TEST)
    load_file("state_country.json", index_name=STATE_COUNTRY)
    load_file("occupation.json", index_name=OCCUPATION)
    load_file("worker.json", index_name=WORKER)
    load_file("work_information.json", index_name=WORK_INFORMATION)


def load_file(filename, index_name):
    # Create index with the mapping if mapping file exists
    mapping_file_path = './test_mapping/' + filename
    if os.path.isfile(mapping_file_path):
        with open(mapping_file_path, 'r') as f:
            test_data_client.indices.create(index=index_name, body=f.read())

    # generate iterable data
    data_file_path = './test_data/' + filename
    def load_json():
        with open(data_file_path, 'r') as f:
            for line in f:
                yield json.loads(line)

    # Need to enable refresh, because the load won't be visible to search immediately
    # https://stackoverflow.com/questions/57840161/elasticsearch-python-bulk-helper-api-with-refresh
    helpers.bulk(test_data_client, load_json(), stats_only=True, index=index_name, refresh='wait_for')


def set_up(test):
    test.globs['sql_cmd'] = sql_cmd
    test.globs['ppl_cmd'] = ppl_cmd


def tear_down(test):
    # drop leftover tables after each test
    test_data_client.indices.delete(index=[ACCOUNTS, EMPLOYEES, PEOPLE, ACCOUNT2, NYC_TAXI, BOOKS, APACHE, WILDCARD, NESTED, WEBLOGS, JSON_TEST, STATE_COUNTRY, OCCUPATION, WORKER, WORK_INFORMATION], ignore_unavailable=True)


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


def doc_suite(fn):
    return docsuite(
        fn,
        parser=bash_parser,
        setUp=set_up_test_indices,
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


def load_tests(loader, suite, ignore):
    tests = []
    # Load doctest docs by category
    with open('../docs/category.json') as json_file:
        category = json.load(json_file)

    bash_docs = category['bash']
    ppl_cli_docs = category['ppl_cli']
    sql_cli_docs = category['sql_cli']

    # docs with bash-based examples
    for fn in doctest_files(bash_docs):
        tests.append(doc_suite(fn))

    # docs with sql-cli based examples
    # TODO: add until the migration to new architecture is done, then we have an artifact including ppl and sql both
    # for fn in doctest_files('sql/basics.rst'):
    #     tests.append(docsuite(fn, setUp=set_up_accounts))
    for fn in doctest_files(sql_cli_docs):
        tests.append(
            docsuite(
                fn,
                parser=sql_cli_parser,
                setUp=set_up_test_indices
            )
        )

    # docs with ppl-cli based examples
    for fn in doctest_files(ppl_cli_docs):
        tests.append(
            docsuite(
                fn,
                parser=ppl_cli_parser,
                setUp=set_up_test_indices
            )
        )

    # randomize order of tests to make sure they don't depend on each other
    random.shuffle(tests)

    return DocTests(tests)


# Single file doctest functionality
def find_doc_file(filename_or_path):
    """Find documentation file by name or return the path if it's already a full path"""
    # If it's already a full path that exists, return it
    if os.path.exists(filename_or_path):
        return filename_or_path
    
    # If it's just a filename, search for it in the docs directory
    if not os.path.sep in filename_or_path:
        try:
            with open('../docs/category.json') as json_file:
                category = json.load(json_file)
            
            # Search in all categories
            all_docs = category['bash'] + category['ppl_cli'] + category['sql_cli']
            
            # Add .rst extension if not present
            search_filename = filename_or_path
            if not search_filename.endswith('.rst'):
                search_filename += '.rst'
            
            # Find files that end with the given filename
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
    
    # If it's a relative path, try to find it
    if not filename_or_path.startswith('../docs/'):
        potential_path = f"../docs/{filename_or_path}"
        if os.path.exists(potential_path):
            return potential_path
    
    return filename_or_path


def determine_doc_type(file_path):
    """Determine the type of documentation file based on category.json"""
    try:
        with open('../docs/category.json') as json_file:
            category = json.load(json_file)
        
        # Convert absolute path to relative path from docs directory
        rel_path = os.path.relpath(file_path, '../docs')
        
        if rel_path in category['bash']:
            return 'bash'
        elif rel_path in category['ppl_cli']:
            return 'ppl_cli'
        elif rel_path in category['sql_cli']:
            return 'sql_cli'
        else:
            # Try to guess based on file path
            if '/ppl/' in file_path:
                return 'ppl_cli'
            elif '/sql/' in file_path or '/dql/' in file_path:
                return 'sql_cli'
            else:
                return 'bash'  # default fallback
    except Exception as e:
        print(f"Warning: Could not determine doc type from category.json: {e}")
        # Fallback to path-based detection
        if '/ppl/' in file_path:
            return 'ppl_cli'
        elif '/sql/' in file_path or '/dql/' in file_path:
            return 'sql_cli'
        else:
            return 'bash'


def run_single_doctest(file_path, verbose=False, endpoint=None):
    """Run doctest for a single documentation file"""
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        return False
    
    # Update endpoint if provided
    if endpoint:
        global ENDPOINT
        ENDPOINT = endpoint
        print(f"Using custom endpoint: {endpoint}")
    
    doc_type = determine_doc_type(file_path)
    print(f"Detected doc type: {doc_type}")
    print(f"Running doctest for: {file_path}")
    
    # Configure doctest options
    optionflags = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
    if verbose:
        optionflags |= doctest.REPORT_NDIFF
    
    # Choose appropriate parser and setup based on doc type
    if doc_type == 'bash':
        parser = bash_parser
        setup_func = set_up_test_indices
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
    elif doc_type == 'ppl_cli':
        parser = ppl_cli_parser
        setup_func = set_up_test_indices
        globs = {}
    else:  # sql_cli
        parser = sql_cli_parser
        setup_func = set_up_test_indices
        globs = {}
    
    try:
        print("Setting up test environment...")
        
        # Create and run the doctest suite
        suite = doctest.DocFileSuite(
            file_path,
            parser=parser,
            setUp=setup_func,
            tearDown=tear_down,
            optionflags=optionflags,
            encoding='utf-8',
            globs=globs
        )
        
        # Run the test
        runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
        result = runner.run(suite)
        
        # Print summary
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


def list_available_docs():
    """List all available documentation files that can be tested"""
    try:
        with open('../docs/category.json') as json_file:
            category = json.load(json_file)
        
        print("Available documentation files for testing:")
        print(f"\nBash-based docs ({len(category['bash'])} files):")
        for doc in sorted(category['bash']):
            print(f"  ../docs/{doc}")
        
        print(f"\nPPL CLI docs ({len(category['ppl_cli'])} files):")
        for doc in sorted(category['ppl_cli']):
            print(f"  ../docs/{doc}")
        
        print(f"\nSQL CLI docs ({len(category['sql_cli'])} files):")
        for doc in sorted(category['sql_cli']):
            print(f"  ../docs/{doc}")
        
        total_docs = len(category['bash']) + len(category['ppl_cli']) + len(category['sql_cli'])
        print(f"\nTotal: {total_docs} documentation files available for testing")
            
    except Exception as e:
        print(f"Error reading category.json: {e}")


def main():
    """Main entry point for single file testing"""
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
    
    if args.list:
        list_available_docs()
        return
    
    # If no file paths provided, run the default unittest behavior
    if not args.file_paths:
        print("No specific files provided. Running full doctest suite...")
        # Run the standard unittest discovery
        unittest.main(module=None, argv=['test_docs.py'], exit=False)
        return
    
    # Single file testing mode
    all_success = True
    total_files = len(args.file_paths)
    
    for i, file_path in enumerate(args.file_paths, 1):
        if total_files > 1:
            print(f"\n{'='*60}")
            print(f"Testing file {i}/{total_files}: {file_path}")
            print('='*60)
        
        # Find the actual file path (handles both full paths and just filenames)
        actual_file_path = find_doc_file(file_path)
        if not actual_file_path:
            print(f"Skipping {file_path} - file not found")
            all_success = False
            continue
        
        success = run_single_doctest(
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
