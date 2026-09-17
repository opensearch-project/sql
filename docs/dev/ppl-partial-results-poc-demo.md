# PPL Progressive Query Execution: Option C PoC Demo

## Scope

This report validates the listener-based partial-result implementation on the Calcite PPL path
using OTel-shaped log data. It covers:

- asynchronous submit, poll, and delete;
- monotonic `fraction_done` progress that remains below `1.0` while running;
- `APPEND` partial results for stable-prefix plans;
- `REPLACE` partial results for fully pushed aggregation and composite aggregation;
- a time-span aggregation with multiple composite pages;
- progress-only execution for blocking plans without a valid partial producer;
- equality between every retained final result and the existing synchronous result.

This PoC does not use whole-plan replay or incremental Calcite operators. Blocking plans such as
`eventstats` and ordered `sort` therefore publish progress but no rows while running.

## Environment and data

The demo ran on September 17, 2026 with:

- 3 OpenSearch nodes;
- 16 GiB minimum and maximum heap on every node;
- a 16 GiB integration-test JVM;
- 4,000,000 OTel log documents in `logs-00001`;
- 12 primary shards and no replicas;
- 10,000 OpenSearch rows per scan page;
- 10,000 product IDs and 1,000 cluster names;
- timestamps starting at `2025-09-04T16:00:00Z` and increasing by 100 ms per document.

The test installs an index template for `logs-*` based on the OTel sample mapping. Each generated
document preserves the sample's structure, including `resource.attributes`,
`attributes.cluster.name`, `attributes.obs_body_length`, `@timestamp`, `severityText`, and the
JSON-formatted `body`. The 100 ms timestamp interval creates 13,334 distinct 30-second buckets, so
the span query exercises composite pagination rather than completing in one page.

Run the demo with:

```bash
./gradlew :integ-test:pplAsyncLargeDataDemo \
  -DignorePrometheus \
  -Dppl.demo.rows=4000000
```

The test clears and then writes complete REST responses and a machine-readable summary to:

```text
integ-test/build/reports/ppl-async-large-data-demo/
```

## Result summary

All six final asynchronous results matched their synchronous result, including schema, row count,
row values, and ordering where the query defines an order.

| Query pattern | Running contract | First progress | First non-empty data | Final | Final rows | Final equals sync |
|---|---:|---:|---:|---:|---:|---:|
| REX stable prefix | `APPEND` | 1,894 ms | 2,095 ms, 1,600 rows | 4,206 ms | 250,000 | Yes |
| Fully pushed aggregation | `REPLACE` | 1,872 ms | 1,872 ms, 1 row | 1,976 ms | 1 | Yes |
| Composite aggregation plus post-processing | `REPLACE` | 138 ms | 138 ms, 800 rows | 393 ms | 10,000 | Yes |
| `eventstats` blocking plan | Progress only | 64 ms | Not available while running | 39,258 ms | 4,000 | Yes |
| Ordered `sort` blocking plan | Progress only | 126 ms | Not available while running | 389 ms | 4,000 | Yes |
| 30-second span aggregation | `REPLACE` | 74 ms | 74 ms, 1,600 rows | 326 ms | 13,334 | Yes |

REX returned usable rows 2,111 ms before completion. The composite queries returned valid
replacement snapshots before completion. `eventstats` demonstrated a long-running blocking plan:
progress arrived 39,194 ms before its final rows. The sort query validates the progress-only
contract but does not show a material latency benefit on this dataset.

## REST workflow

Submit:

```http
POST /_plugins/_ppl
Content-Type: application/json

{
  "query": "source=logs-00001 | ...",
  "wait_for_completion_timeout": "0ms",
  "keep_alive": "10m"
}
```

Poll the current result:

```http
GET /_plugins/_ppl/jobs/{id}?offset=0&count=10000
```

Cancel and release retained state:

```http
DELETE /_plugins/_ppl/jobs/{id}
```

A running response has `status: RUNNING`, a finite `progress.fraction_done` in `[0, 0.8]`, and an
`update_mode` after the physical plan is classified. Successful completion reports exactly `1.0`.

## Query results

### 1. REX stable-prefix query

```text
source=logs-00001
| rex field=body "level[^a-z]+(?<loglevel>error|warn|info)"
| fields `@timestamp`, severityText, body, loglevel
| head 250000
```

The root Calcite result stream publishes immutable prefixes. The client appends rows it has not
already consumed.

First non-empty running result:

```json
{
  "status": "RUNNING",
  "update_mode": "APPEND",
  "progress": {"fraction_done": 0.006},
  "total": 1600,
  "size": 1600,
  "window": {"offset": 0, "count": 10000},
  "datarows": [
    [
      "2025-09-04 16:00:00.1",
      "INFO",
      "{\"msg\":\"Error finding unassigned IPs for ENI eni-1\",...}",
      "info"
    ]
  ]
}
```

Final result metadata:

```json
{
  "status": "SUCCEEDED",
  "update_mode": "APPEND",
  "progress": {"fraction_done": 1},
  "total": 250000,
  "size": 10000,
  "window": {"offset": 0, "count": 10000}
}
```

The test fetched all 25 final pages and verified that the first 1,600 rows were an exact prefix of
the final result.

### 2. Fully pushed aggregation

```text
source=logs-00001
| stats sum(`attributes.obs_body_length`) as total_body_bytes,
        avg(severityNumber) as avg_severity,
        max(flags) as max_flags,
        min(severityNumber) as min_severity
```

OpenSearch partial-reduce callbacks are parsed into a complete provisional row. Every newer
snapshot replaces the previous one.

First running result:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.8},
  "total": 1,
  "size": 1,
  "datarows": [[295833667, 11.491361934667411, 3, 0]]
}
```

Final result:

```json
{
  "status": "SUCCEEDED",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 1},
  "total": 1,
  "size": 1,
  "datarows": [[710000000, 11.499984, 3, 0]]
}
```

The changed aggregate values demonstrate why the running contract is `REPLACE`.

### 3. Composite aggregation with coordinator post-processing

```text
source=logs-00001
| stats count() as total by `resource.attributes.productid`
| eval doubled = total * 2
| fields `resource.attributes.productid`, total, doubled
```

Completed composite pages pass through the row-local Calcite operators. The listener publishes all
completed root rows as a replacement snapshot.

First non-empty result:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.12},
  "total": 800,
  "size": 800,
  "datarows": [
    ["pr123456", 400, 800],
    ["pr123457", 400, 800]
  ]
}
```

The final response contained 10,000 rows and matched the synchronous result.

### 4. Blocking `eventstats`

```text
source=logs-00001
| rex field=body "caller[^a-z]+(?<caller>[a-z]+/[a-z]+[.]go)"
| eventstats count() as product_log_count by `resource.attributes.productid`
| where severityText = 'ERROR'
| fields `@timestamp`, severityText, `resource.attributes.productid`,
         `attributes.cluster.name`, caller, product_log_count
```

`eventstats` cannot finalize any output row until its aggregate state is complete. Option C has no
valid row producer for this plan, so running responses contain progress only.

First running result:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.002},
  "total": 0,
  "size": 0,
  "datarows": []
}
```

Final result metadata and sample:

```json
{
  "status": "SUCCEEDED",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 1},
  "total": 4000,
  "size": 4000,
  "datarows": [
    [
      "2025-09-04 16:08:20",
      "ERROR",
      "pr128456",
      "xyz-cluster0-ci-prod-us-east-1",
      "network/eni.go",
      400
    ]
  ]
}
```

### 5. Blocking ordered sort

```text
source=logs-00001
| where `attributes.cluster.name` = 'xyz-cluster0-ci-prod-us-east-1'
| sort - `@timestamp`
| fields `@timestamp`, severityText, body, `attributes.cluster.name`
```

An ordered sort can revise every output position until it has consumed its input. Running responses
therefore contain progress only.

First running result:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.002},
  "total": 0,
  "size": 0,
  "datarows": []
}
```

Final result metadata and sample:

```json
{
  "status": "SUCCEEDED",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 1},
  "total": 4000,
  "size": 4000,
  "datarows": [
    [
      "2025-09-09 07:05:00",
      "ERROR",
      "{\"msg\":\"Error finding unassigned IPs for ENI eni-3999000\",...}",
      "xyz-cluster0-ci-prod-us-east-1"
    ]
  ]
}
```

### 6. 30-second span aggregation

```text
source=logs-00001
| stats count() by span(@timestamp, 30s)
```

The 13,334 time buckets require multiple composite pages. Completed buckets are exposed as
replacement snapshots while the query is running.

First non-empty result:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.21000000000000002},
  "total": 1600,
  "size": 1600,
  "datarows": [
    [300, "2025-09-04 16:00:00"],
    [300, "2025-09-04 16:00:30"]
  ]
}
```

Final result metadata:

```json
{
  "status": "SUCCEEDED",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 1},
  "total": 13334,
  "size": 10000,
  "window": {"offset": 0, "count": 10000}
}
```

The test fetched both final pages and reconstructed all 13,334 buckets before comparing them with
the synchronous result.

## Correctness checks

The REST test enforces:

- every running progress value is finite, monotonic, and less than `1.0`;
- only successful completion reports `1.0`;
- running `APPEND` rows are an exact prefix of the final result;
- running `REPLACE` responses are complete snapshots, not deltas;
- unsupported blocking plans return no running rows;
- all final pages reconstruct the same result as synchronous PPL;
- the API can delete retained state and a subsequent GET returns `404`;
- a query that completes within `wait_for_completion_timeout` returns the normal result without a
  retained job ID.

The exact generated summary and full REST responses from this run are available in
`integ-test/build/reports/ppl-async-large-data-demo/`.
