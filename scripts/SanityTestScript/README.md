# Sanity Test Script

### Description
This Python script executes test queries from a CSV file using an asynchronous query API and generates comprehensive test reports.

The script produces two report types:
1. An Excel report with detailed test information for each query
2. A JSON report containing both test result overview and query-specific details

Apart from the basic feature, it also has some advanced functionality includes:
1. Concurrent query execution (note: the async query service has session limits, so use thread workers moderately despite it already supports session ID reuse)
2. Configurable query timeout for the requests
3. Flexible row selection from the input CSV file, by specifying start row and end row of the input CSV file.
4. Expected status validation when expected_status is present in the CSV
5. Ability to generate partial reports if testing is interrupted

### Usage
To use this script, you need to have Python **3.6** or higher installed. It also requires the following Python libraries:
```shell
pip install requests pandas openpyxl pyspark setuptools pyarrow grpcio grpcio-status protobuf
```

After getting the requisite libraries, you can run the script with the following command line parameters in your shell:
```shell
python SanityTest.py --base-url ${BASE_URL} --username *** --password *** --input-csv test_queries.csv --output-file test_report
```
You need to replace the placeholders with your actual values of BASE_URL, and USERNAME, PASSWORD for authentication to your endpoint.

Running against the localhost cluster, `BASE_URL` should be set to `http://localhost:9200`. You can launch an OpenSearch cluster with SQL plugin locally in opensearch-sql repo directory with command:
```shell
./gradlew run
```

For more details of the command line parameters, you can see the help manual via command:
```shell
python SanityTest.py --help   

usage: SanityTest.py [-h] --spark-url BASE_URL --username USERNAME --password PASSWORD --input-csv INPUT_CSV --output-file OUTPUT_FILE
                                      [--max-workers MAX_WORKERS] [--timeout TIMEOUT]
                                      [--start-row START_ROW] [--end-row END_ROW]
                                      [--log-level LOG_LEVEL]

Run tests from a CSV file and generate a report.

options:
  -h, --help            show this help message and exit
  --base-url BASE_URL  OpenSearch Cluster Connect URL of the service
  --username USERNAME   Username for authentication
  --password PASSWORD   Password for authentication
  --input-csv INPUT_CSV
                        Path to the CSV file containing test queries
  --output-file OUTPUT_FILE
                        Path to the output report file
  --max-workers MAX_WORKERS
                        optional, Maximum number of worker threads (default: 2)
  --timeout TIMEOUT     optional, Timeout in seconds (default: 600)
  --start-row START_ROW
                        optional, The start row of the query to run, start from 1
  --end-row END_ROW     optional, The end row of the query to run, not included
  --log-level LOG_LEVEL
                        optional, Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL, default: INFO)
```

### Input CSV File
As claimed in the description, the input CSV file should at least have the column of `query` to run the tests. It also supports an optional column of `expected_status` whose value could be `SUCCESS`, `FAILED` or `TIMEOUT`. The script will check the actual status against the expected status and generate a new column of `check_status` for the check result -- TRUE means the status check passed; FALSE means the status check failed.

#### JSON Report
The JSON report provides the same information as the Excel report. But in JSON format, additionally, it includes a statistical summary of the test results at the beginning of the report.

An example of JSON report:
```json
{
  "summary": {
    "total_queries": 2,
    "check_failed": 0,
    "successful_queries": 1,
    "failed_queries": 1,
    "timeout_queries": 0,
    "execution_time": 1.245655
  },
  "detailed_results": [
    {
      "seq_id": 1,
      "query_name": "Successful Demo",
      "query": "source=opensearch_dashboards_sample_data_flights | patterns FlightDelayType | stats count() by patterns_field",
      "expected_status": "SUCCESS",
      "status": "SUCCESS",
      "duration": 0.843509,
      "start_time": "2025-04-22 16:30:22.461069",
      "end_time": "2025-04-22 16:30:23.304578",
      "result": {
        "schema": [
          {
            "name": "count()",
            "type": "int"
          },
          {
            "name": "patterns_field",
            "type": "string"
          }
        ],
        "datarows": [
          [
            9311,
            " "
          ],
          [
            689,
            "  "
          ]
        ],
        "total": 2,
        "size": 2
      },
      "check_status": true
    },
    {
      "seq_id": 2,
      "query_name": "Failed Demo",
      "query": "source=opensearch_dashboards_sample_data_flights_2 | patterns FlightDelayType | stats count() by patterns_field",
      "expected_status": "FAILED",
      "status": "FAILED",
      "duration": 0.402146,
      "start_time": "2025-04-22 16:30:22.461505",
      "end_time": "2025-04-22 16:30:22.863651",
      "error": {
        "error": "404 Client Error: Not Found for url: http://k8s-calcitep-opensear-8312a971dd-1309739395.us-west-1.elb.amazonaws.com/_plugins/_ppl",
        "response": {
          "error": {
            "reason": "Error occurred in OpenSearch engine: no such index [opensearch_dashboards_sample_data_flights_2]",
            "details": "[opensearch_dashboards_sample_data_flights_2] IndexNotFoundException[no such index [opensearch_dashboards_sample_data_flights_2]]\nFor more details, please send request for Json format to see the raw response from OpenSearch engine.",
            "type": "IndexNotFoundException"
          },
          "status": 404
        }
      },
      "check_status": true
    }
  ]
}
```
