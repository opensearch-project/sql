# Running PPL Locally with OTel Tracing → observability-stack

Launch the SQL/PPL plugin against a local OpenSearch node with distributed tracing enabled, and export spans to a running [observability-stack](https://github.com/opensearch-project/observability-stack) docker-compose deployment.

## Prerequisites

**observability-stack docker-compose running.** From the repo:
```bash
cd <observability-stack>/docker-compose
docker compose up -d
```
The OTel Collector must be listening on OTLP gRPC `0.0.0.0:4317` (default).

**`telemetry-otel` plugin zip built from the OpenSearch source tree.** This module is not published as a standalone plugin — it must be built locally:
```bash
cd <OpenSearch>/plugins/telemetry-otel
../../gradlew bundlePlugin
# produces build/distributions/telemetry-otel-<version>.zip
```
Pass the path with `-DtelemetryOtelSrc=<OpenSearch>/plugins/telemetry-otel` when running the plugin.

## One-time: fix port conflict

Both the local `:run` task and the docker `opensearch` service want host port `9200`. Free `9200` for the local run by remapping only the docker host port (intra-network traffic between the stack's containers stays on `9200`).

**`observability-stack/.env`** — keep `OPENSEARCH_PORT=9200`. It's used as the intra-network target port by data-prepper, dashboards, and the exporter.

**`observability-stack/docker-compose.local-opensearch.yml`** — change the host mapping only:
```yaml
ports:
  - "9210:9200"     # host 9210 → container 9200 (default was "${OPENSEARCH_PORT}:9200")
  - "9600:9600"     # unchanged
```

Recreate the affected containers:
```bash
cd <observability-stack>/docker-compose
docker compose down opensearch data-prepper opensearch-dashboards
docker compose up -d opensearch data-prepper opensearch-dashboards
```

After this: docker OpenSearch is reachable at `https://localhost:9210` (admin/`My_password_123!@#`), the local plugin `:run` gets `http://localhost:9200`.

## Launch

```bash
cd <os-sql>
./gradlew :opensearch-sql-plugin:run -DenableTelemetry \
  -DtelemetryOtelSrc=<OpenSearch>/plugins/telemetry-otel
```

The `-DenableTelemetry` flag (wired in `plugin/build.gradle`) does the following against `testClusters.integTest`:

- installs the `telemetry-otel` zip from `<telemetryOtelSrc>/build/distributions/telemetry-otel-<version>.zip`
- sets `opensearch.experimental.feature.telemetry.enabled=true` (system property)
- enables the tracer: `telemetry.feature.tracer.enabled=true`, `telemetry.tracer.enabled=true`
- sets sampling to 100%: `telemetry.tracer.sampler.probability=1.0`
- selects the OTLP gRPC exporter: `telemetry.otel.tracer.span.exporter.class=io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter`
- points the exporter at the collector: `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`

Wait until the node reports:
```
[integTest-0] Successfully instantiated the SpanExporter class ... OtlpGrpcSpanExporter
[integTest-0] publish_address {127.0.0.1:9200}
[integTest-0] started
```

## Smoke test

Index a small dataset and run a PPL query:
```bash
curl -s -X POST 'http://localhost:9200/test-logs/_bulk' -H 'Content-Type: application/x-ndjson' --data-binary '{"index":{}}
{"host":"h1","status":200,"latency_ms":15}
{"index":{}}
{"host":"h1","status":500,"latency_ms":40}
{"index":{}}
{"host":"h2","status":404,"latency_ms":60}
'
curl -s -X POST 'http://localhost:9200/test-logs/_refresh'

curl -s -X POST 'http://localhost:9200/_plugins/_ppl' \
  -H 'Content-Type: application/json' \
  -d '{"query":"source=test-logs | where status > 300 | stats count() by host"}'
```

## Verify traces landed

**Inspect spans directly in the observability-stack OpenSearch:**
```bash
curl -k -s \
  'https://localhost:9210/otel-v1-apm-span-*/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{"size":10,"query":{"prefix":{"name":"opensearch.query"}},
       "sort":[{"startTime":{"order":"desc"}}],
       "_source":["traceId","spanId","parentSpanId","name","kind",
                  "durationInNanos","attributes"]}'
```

Expected per query: one CLIENT root span `opensearch.query` plus four INTERNAL children — `opensearch.query.prepare`, `.analyze`, `.optimize`, `.execute`. The `.prepare` span is trace-only and covers transport-side parse + AST build + anonymize (dominant on cold start due to ANTLR grammar init); the others match profile phase keys 1-to-1 (both derived from the same `MetricName`), so trace and profile report the same phases with the same durations. Phases (per the profile's original definition — do not be misled by internal function names): `.analyze` = semantic analyze + Calcite plan conversion + rule-based optimize (`CalciteToolsHelper.optimize`); `.optimize` = physical shuttle + `RelRunner.prepareStatement` (inside `OpenSearchRelRunners.run`); `.execute` = `executeQuery` + `buildResultSet`. Root attributes:

| Attribute | Example |
|-----------|---------|
| `db.query.type` | `ppl` |
| `db.operation.name` | `EXECUTE` (or `EXPLAIN`) |
| `db.query.text` | `source=test-logs \| where status > 300 \| stats count() by host` |

**Or view in Dashboards:** http://localhost:5601 → Observability → Trace Analytics.
