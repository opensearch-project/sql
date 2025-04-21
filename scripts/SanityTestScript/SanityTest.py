#  Copyright OpenSearch Contributors
#  SPDX-License-Identifier: Apache-2.0

import signal
import sys
import requests
import json
import csv
import logging
from datetime import datetime
import pandas as pd
import argparse
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

"""
Environment: python3

Example to use this script:

python SanityTest.py --base-url ${URL_ADDRESS} --username *** --password *** --input-csv test_queries.csv --output-file test_report --max-workers 2

The input file test_queries.csv should contain column: `query`

For more details, please use command:

python SanityTest.py --help

"""

class PPLTester:
  def __init__(self, base_url, username, password, max_workers, timeout, output_file, start_row, end_row, log_level):
    self.base_url = base_url
    self.auth = HTTPBasicAuth(username, password)
    self.headers = { 'Content-Type': 'application/json' }
    self.max_workers = max_workers
    self.timeout = timeout
    self.output_file = output_file
    self.start = start_row - 1 if start_row else None
    self.end = end_row - 1 if end_row else None
    self.log_level = log_level
    self.logger = self._setup_logger()
    self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
    self.thread_local = threading.local()
    self.test_results = []

  def _setup_logger(self):
    logger = logging.getLogger('PPLTester')
    logger.setLevel(self.log_level)

    fh = logging.FileHandler('flint_test.log')
    fh.setLevel(self.log_level)

    ch = logging.StreamHandler()
    ch.setLevel(self.log_level)

    formatter = logging.Formatter(
      '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


  # Call submit API to submit the query
  def submit_query(self, query):
    url = f"{self.base_url}/_plugins/_ppl"
    response_json = None
    payload = {
      "query": query,
    }
    try:
      response = requests.post(url, auth=self.auth, headers=self.headers, json=payload, timeout=self.timeout)
      response_json = response.json()
      response.raise_for_status()
      return response_json
    except Exception as e:
      return {"error": str(e), "response": response_json}

  # Run the test and return the result
  def run_test(self, query, seq_id, expected_status):
    self.logger.info(f"Starting test: {seq_id}, {query}")
    start_time = datetime.now()
    submit_result = self.submit_query(query)
    if "error" in submit_result:
      self.logger.warning(f"Submit error: {submit_result}")
      return {
        "query_name": seq_id,
        "query": query,
        "expected_status": expected_status,
        "status": "FAILED",
        "check_status": "FAILED" == expected_status if expected_status else None,
        "error": {
          "error_message": submit_result.get("error"),
          "response_error": submit_result.get("response", {}).get("error")
        },
        # "error": submit_result["error"] if "error" in submit_result else submit_result["response"]["error"],
        "duration": 0,
        "start_time": start_time,
        "end_time": datetime.now()
      }

    self.logger.debug(f"Submit return: {submit_result}")

    # TODO: need to check results
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
      "query_name": seq_id,
      "query": query,
      "expected_status": expected_status,
      "status": "SUCCESS",
      "check_status": expected_status == "SUCCESS",
      "error": None,
      "result": submit_result,
      "duration": duration,
      "start_time": start_time,
      "end_time": end_time
    }


  def run_tests_from_csv(self, csv_file):
    with open(csv_file, 'r') as f:
      reader = csv.DictReader(f)
      queries = [(row['query'], i, row.get('expected_status', None)) for i, row in enumerate(reader, start=1) if row['query'].strip()]

    # Filtering queries based on start and end
    queries = queries[self.start:self.end]

    # Parallel execution
    futures = [self.executor.submit(self.run_test, query, seq_id, expected_status) for query, seq_id, expected_status in queries]
    for future in as_completed(futures):
      result = future.result()
      self.test_results.append(result)

  def generate_report(self):
    self.logger.info("Generating report...")
    total_queries = len(self.test_results)
    successful_queries = sum(1 for r in self.test_results if r['status'] == 'SUCCESS')
    failed_queries = sum(1 for r in self.test_results if r['status'] == 'FAILED')
    submit_failed_queries = sum(1 for r in self.test_results if r['status'] == 'SUBMIT_FAILED')
    check_failed_queries = sum(1 for r in self.test_results if r['check_status'] == False)
    timeout_queries = sum(1 for r in self.test_results if r['status'] == 'TIMEOUT')

    # Create report
    report = {
      "summary": {
        "total_queries": total_queries,
        "check_failed": check_failed_queries,
        "successful_queries": successful_queries,
        "failed_queries": failed_queries,
        "submit_failed_queries": submit_failed_queries,
        "timeout_queries": timeout_queries,
        "execution_time": sum(r['duration'] for r in self.test_results)
      },
      "detailed_results": self.test_results
    }

    # Save report to JSON file
    with open(f"{self.output_file}.json", 'w') as f:
      json.dump(report, f, indent=2, default=str)

    # Save reults to Excel file
    df = pd.DataFrame(self.test_results)
    df.to_excel(f"{self.output_file}.xlsx", index=False)

    self.logger.info(f"Generated report in {self.output_file}.xlsx and {self.output_file}.json")

def signal_handler(sig, frame, tester):
  print(f"Signal {sig} received, generating report...")
  try:
    tester.executor.shutdown(wait=False, cancel_futures=True)
    tester.generate_report()
  finally:
    sys.exit(0)

def main():
  # Parse command line arguments
  parser = argparse.ArgumentParser(description="Run tests from a CSV file and generate a report.")
  parser.add_argument("--base-url", required=True, help="Base URL of the service")
  parser.add_argument("--username", required=True, help="Username for authentication")
  parser.add_argument("--password", required=True, help="Password for authentication")
  parser.add_argument("--input-csv", required=True, help="Path to the CSV file containing test queries")
  parser.add_argument("--output-file", required=True, help="Path to the output report file")
  parser.add_argument("--max-workers", type=int, default=2, help="optional, Maximum number of worker threads (default: 2)")
  parser.add_argument("--timeout", type=int, default=600, help="optional, Timeout in seconds (default: 600)")
  parser.add_argument("--start-row", type=int, default=None, help="optional, The start row of the query to run, start from 1")
  parser.add_argument("--end-row", type=int, default=None, help="optional, The end row of the query to run, not included")
  parser.add_argument("--log-level", default="INFO", help="optional, Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL, default: INFO)")

  args = parser.parse_args()

  tester = PPLTester(
    base_url=args.base_url,
    username=args.username,
    password=args.password,
    max_workers=args.max_workers,
    timeout=args.timeout,
    output_file=args.output_file,
    start_row=args.start_row,
    end_row=args.end_row,
    log_level=args.log_level,
  )

  # Register signal handlers to generate report on interrupt
  signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, tester))
  signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, tester))

  # Running tests
  tester.run_tests_from_csv(args.input_csv)

  # Gnerate report
  tester.generate_report()

if __name__ == "__main__":
  main()
