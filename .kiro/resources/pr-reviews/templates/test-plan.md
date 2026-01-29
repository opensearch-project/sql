# PR <num> Test Plan
- Commit: <sha>
- Date: <YYYY-MM-DD>
- Environment: <cluster url/port, index prefix, data source>
- Assumptions: <settings, licenses, plugins>

## Scenarios
1) <name>
   - Data/setup: <ingest path or generator>
   - Query: ```
     <ppl query>
     ```
   - Expected: <result/shape/latency>
2) ...

## Commands to run
- `./gradlew opensearch-sql:run`
- `<python script>`

## Metrics to capture
- Latency: <thresholds if stated>
- Logs/errors to collect

## Exit criteria
- All scenarios pass OR failures recorded in report.
