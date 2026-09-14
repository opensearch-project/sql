# PPL Progressive Query Execution: Option C PoC Demo

## Scope

This report validates the listener-based partial-result implementation on the Calcite PPL path.
It covers:

- asynchronous submit, poll, and delete;
- monotonic `fraction_done` progress that remains below `1.0` while running;
- `APPEND` partial results for stable-prefix plans;
- `REPLACE` partial results for fully pushed aggregation and composite aggregation;
- progress-only execution for blocking plans that do not have a valid partial producer;
- equality between the retained final result and the existing synchronous result.

This PoC does not use whole-plan replay or incremental Calcite operators. Blocking plans such as
`eventstats` and ordered `sort` therefore publish progress but no rows while running.

## Environment

The demo ran on September 16, 2026 with:

- 3 OpenSearch nodes;
- 16 GiB minimum and maximum heap on every node;
- a 16 GiB integration-test JVM;
- 4,000,000 documents;
- 12 primary shards;
- 10,000 OpenSearch rows per scan page.

Run the same demo with:

```bash
./gradlew :integ-test:pplAsyncLargeDataDemo \
  -DignorePrometheus \
  -Dppl.demo.rows=4000000
```

The test writes complete REST responses and a machine-readable summary to:

```text
integ-test/build/reports/ppl-async-large-data-demo/
```

## Result summary

All five final asynchronous results matched their synchronous result, including schema, row count,
row values, and ordering where the query defines an order.

| Query pattern | Running contract | First progress | First non-empty data | Final | Final rows | Final equals sync |
|---|---:|---:|---:|---:|---:|---:|
| REX stable prefix | `APPEND` | 270 ms | 270 ms, 3,200 rows | 3,454 ms | 250,000 | Yes |
| Fully pushed aggregation | `REPLACE` | 335 ms | 390 ms, 1 row | 494 ms | 1 | Yes |
| Composite aggregation plus post-processing | `REPLACE` | 130 ms | 130 ms, 800 rows | 371 ms | 10,000 | Yes |
| `eventstats` blocking plan | Progress only | 62 ms | Not available while running | 12,705 ms | 4,000 | Yes |
| Ordered `sort` blocking plan | Progress only | 444 ms | Not available while running | 601 ms | 4,000 | Yes |

The meaningful early-response result is strongest for REX, composite aggregation, and
`eventstats`. The fully pushed aggregation produced a valid provisional row 104 ms before
completion. The sort query is included to validate correctness and the progress-only contract, not
to claim a material latency benefit on this dataset.

## REST workflow

Submit:

```http
POST /_plugins/_ppl
Content-Type: application/json

{
  "query": "source=ppl_async_large_data_demo | ...",
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
source=ppl_async_large_data_demo
| where event_id % 16 = 0
| rex field=email "(?<user>[^@]+)@(?<domain>.+)"
| fields event_id, email, user, domain
| head 250000
```

The root Calcite result stream publishes immutable prefixes. The client appends rows it has not
already consumed.

First running result:

```json
{
  "status": "RUNNING",
  "update_mode": "APPEND",
  "progress": {"fraction_done": 0.004},
  "total": 3200,
  "size": 3200,
  "window": {"offset": 0, "count": 10000},
  "datarows": [
    [0, "user0000000@example000.com", "user0000000", "example000.com"],
    [16, "user0000016@example016.com", "user0000016", "example016.com"]
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

The test fetched all 25 final pages and verified that the first 3,200 rows were an exact prefix of
the final result.

### 2. Fully pushed aggregation

```text
source=ppl_async_large_data_demo
| stats sum(event_id % 1000) as sum_mod_1000,
        avg(event_id % 997) as avg_mod_997,
        max(event_id % 991) as max_mod_991,
        min(event_id % 983) as min_mod_983
```

OpenSearch partial-reduce callbacks are parsed into a complete provisional row. Every newer
snapshot replaces the previous one.

First progress response:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.13333333333333333},
  "total": 0,
  "size": 0,
  "datarows": []
}
```

First running result:

```json
{
  "status": "RUNNING",
  "update_mode": "REPLACE",
  "progress": {"fraction_done": 0.6000000000000001},
  "total": 1,
  "size": 1,
  "datarows": [[832568917, 497.8944495137378, 990, 0]]
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
  "datarows": [[1998000000, 497.9956755, 990, 0]]
}
```

The changed aggregate values demonstrate why the running contract is `REPLACE`.

### 3. Composite aggregation with coordinator post-processing

```text
source=ppl_async_large_data_demo
| stats count() as total by group_id
| eval doubled = total * 2
| fields group_id, total, doubled
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
  "datarows": [[0, 400, 800], [1, 400, 800]]
}
```

The final response contained 10,000 rows and matched the synchronous result.

### 4. Blocking `eventstats`

```text
source=ppl_async_large_data_demo
| rex field=email "(?<domain>[^@]+)@(?<maildomain>.+)"
| eventstats count() as group_count by group_id
| where event_id % 1000 = 0
| fields event_id, group_id, maildomain, group_count
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
    [0, 0, "example000.com", 400],
    [10000, 0, "example000.com", 400]
  ]
}
```

Progress arrived 12,643 ms before the final response.

### 5. Blocking ordered sort

```text
source=ppl_async_large_data_demo
| rex field=email "(?<domain>example[0-9]+[.]com)"
| where domain = 'example000.com'
| sort - event_id
| fields event_id, email, domain
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
    [3999000, "user3999000@example000.com", "example000.com"],
    [3998000, "user3998000@example000.com", "example000.com"]
  ]
}
```

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

The exact generated summary for this run is available in
`integ-test/build/reports/ppl-async-large-data-demo/summary.json`.
