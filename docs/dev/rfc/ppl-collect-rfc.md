# RFC: PPL `collect` command — append pipeline results to a destination index

- **Status:** Draft
- **Target engine:** Apache Calcite PPL path (OpenSearch SQL plugin)
- **Related:** `LogicalTableSpool` (Calcite Spool family), OpenSearch bulk / reindex (`ReindexAction`), Point-in-Time (PIT) search

## Problem Statement

PPL has no way to write the rows flowing through a pipeline back into an index.
Every command reads from an index and returns rows to the client, so a common
analytics pattern is unsupported: computing a derived or summarized result set
with PPL (filter, aggregate, transform) and persisting it into a separate index
for later querying, dashboards, or scheduled rollups. Users who want to
materialize a PPL result today must export the rows and re-ingest them through a
separate write path outside the query language.

## Current State

- PPL is read-only end to end. The Calcite PPL path lowers every query to a read
  plan (index scan, pushed-down filters and aggregations, enumerable post-
  processing) and streams rows out to the caller. There is no write node in the
  command surface.
- OpenSearch already has mature write machinery the language does not reach:
  the bulk API, the server-side reindex (bulk-by-scroll) action, Point-in-Time
  (PIT) search for cursor pagination, indexing-pressure backpressure, and
  per-principal authorization. None of it is exposed through a PPL command.
- Apache Calcite already models writes with a canonical `LogicalTableModify`
  node and an `EnumerableTableModify` physical node, but the PPL path does not
  use them because no command needs to write yet.

The shortcoming: the only way to persist a PPL-computed result set is to leave
the query language, which breaks the "compute and store in one pipeline" pattern
and duplicates ingestion logic.

## Long-Term Goals

- **Ideal outcome.** PPL pipelines can persist their results into an index as a
  first-class, composable step, so analysts build "filter / aggregate /
  transform / store" in a single query without leaving the language.
- **Primary objectives.** Append pipeline rows to a caller-specified, pre-
  existing destination index; pass those rows through unchanged so the step
  composes inside a pipeline; stream arbitrarily large inputs without buffering
  the full result set in the coordinator; decouple the write from search-time
  visibility.
- **Sustainability and scalability.** Reuse the existing OpenSearch bulk write
  path (with the server-side reindex action available as an optional future
  optimization) and existing Calcite write modeling (`LogicalTableSpool`)
  rather than introducing new plan or AST node types. Riding proven engine
  surfaces means batching, retry, and backpressure come from the platform
  instead of bespoke command code, keeping the maintenance and operational
  surface small as data volumes grow.
- **Root-problem alignment.** The proposal targets the actual gap (no write
  command), not a symptom. A pipeline write step is the minimal, direct closure
  of the read-only limitation.
- **Confidence.** High. The read side (PIT streaming) and the write side
  (batched bulk with retry/backpressure) already exist and are independently
  proven in the plugin and the distribution; the design composes them through
  Calcite's standard write node rather than inventing new mechanisms.

## Proposal

Introduce a `collect` command that appends the rows flowing through a pipeline
into a named, pre-existing destination index, while passing those same rows
through to the result set so the pipeline can continue or return them.

```
... | collect index=<destination-index>
```

`index=<destination-index>` is required and names a pre-existing destination.
Rows reaching `collect` are appended as documents whose `_source` is built from
the pipeline row's fields, and the same rows pass through to the result set.
`collect` is a write step normally placed at the end of a pipeline, after any
filtering, projection, or aggregation.

Core semantics:

1. **Pass-through output.** `collect` emits its input rows downstream unchanged;
   its output row type equals its input row type. There is no synthetic write-
   summary row.
2. **Append-only.** Each input row becomes one new document with an
   auto-generated `_id`. There is no ordering guarantee across documents, and
   re-running a query writes new documents.
3. **Pre-existing destination.** The destination must exist before the query
   runs. A missing index is an error.
4. **Decoupled visibility.** The write does not force a refresh; documents become
   searchable on the destination's own refresh cycle, so the query does not
   block on visibility.
5. **No index creation, no update/upsert, no cross-cluster writes, and no
   user-facing async task API** in the first version.

## Approach

`collect` reuses Calcite's canonical write node and the existing PPL visitor.
No new AST node and no new logical plan node are introduced.

### AST and logical plan

- The existing `Collect` AST node carries the destination index name.
- `CalciteRelNodeVisitor.visitCollect` resolves the destination through the same
  catalog path that `scan()` uses (`RelOptSchema.getTableForMember`) and builds a
  standard `LogicalTableSpool` (LAZY/LAZY) over the pipeline subtree. `Spool` is
  Calcite's existing "iterate the input and, in addition to returning its
  results, forward them into a target table" node, which is exactly the
  pass-through write contract — and because `Spool extends SingleRel`, its row
  type equals the input row type, so pass-through holds with no rowcount
  mismatch:

```
LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[OpenSearch, dest_index]])
  <pipeline subtree>
    CalciteLogicalIndexScan(table=[[OpenSearch, src_index]])
```

`LogicalTableModify` (Calcite's other write node) is **not** used: its row type
is a fixed single BIGINT rowcount (`createDmlRowType(INSERT)`), which conflicts
with pass-through and with Volcano's same-row-type-per-RelSet invariant.
`LogicalTableSpool` avoids that entirely.

Because the destination is resolved as a first-class `RelOptTable` at planning
time, a destination index that does not exist fails to resolve during planning.
Pre-existence is enforced as a natural consequence of name resolution rather than
a separate runtime check. Dot-prefixed (system/hidden) destinations are refused
at plan time.

### Write-path strategy by output size

The **write volume reaching `collect`** — not the source index size — is the
decisive variable for choosing a write mechanism. Two independent axes govern
the choice:

- **Output size.** How many rows actually reach `collect`. A post-aggregation
  result (`... | stats ... | collect`) is usually a handful of rows; a filtered
  pass-through (`source=big | where ... | collect`) can be millions.
- **Pushability.** Whether the input subtree collapses to a single all-pushdown
  `OpenSearchIndex` scan (so the write source could in principle be expressed as
  a server-side query), versus carrying an aggregation or row-wise transform
  above the scan.

|                  | Small output (≤ one batch)   | Large output (many batches)                                                          |
|------------------|------------------------------|--------------------------------------------------------------------------------------|
| **Pushable**     | single `bulk()` call         | **full table scope** → server-side `ReindexAction`; partial / row-consumed → PIT-streamed scan + batched `bulk()` |
| **Non-pushable** | single `bulk()` call         | engine-produced rows + batched `bulk()`                                              |

These are not mutually exclusive. There is **one default engine plus one
plan-shape-selected optimization**:

1. **Default engine — batched bulk.** Stream the input, accumulate rows into
   bounded batches, write each batch with `bulk()` under retry/backpressure, and
   pass the rows through. It is correct for every input shape and every size,
   and it is the only path that satisfies pass-through.
2. **Optimization — server-side reindex,** selected when the **query plan shows
   a full table scope** (the `LogicalTableSpool` input is a single
   `OpenSearchIndex` scan, optionally with a pushed-down filter, and nothing
   above it changes the rows) **and** the pass-through rows are not consumed
   downstream. In that case the entire read-write loop is offloaded to
   `ReindexAction` server-side, avoiding the coordinator read-write double hop.

Output size changes only the mechanics of the default engine, never its
identity:

- **Small output is the degenerate case and the common one.** When the row count
  fits a single batch (default 1000, the existing `BATCH_SIZE`), the engine
  issues exactly one `bulk()` call: no Point-in-Time (PIT), no scroll, no
  background task. This is the lowest-latency path and is naturally
  pass-through-correct, because the operator emits the same rows it wrote.
- **Large output is the same engine iterated.** The read streams via PIT when
  the input is a document scan exceeding the result window; the write chunks
  into fixed-size batches, each wrapped with retry and backoff. Peak memory is
  one batch on the write side and one page on the read side, never the full
  result set.

This deliberately mirrors OpenSearch's own streaming-ingestion design. The
experimental Streaming Bulk API (`POST _bulk/stream`, introduced 2.17.0)
accumulates operations into a batch (`batch_size`, default 1) and lets
backpressure flow between producer and cluster over a chunked HTTP/2 channel.
That API is a **client-facing REST handler** that requires the non-default
`transport-reactor-netty4` HTTP transport and is flagged experimental and
not-for-production, so `collect` cannot use it as an in-cluster seam. `collect`
adopts the same accumulate-and-backpressure shape over the in-process
`OpenSearchClient.bulk()` instead, which is the production-supported write path
and needs no transport plugin.

### Physical lowering

A converter rule matches a `LogicalTableSpool` whose target table unwraps to an
`OpenSearchIndex` and lowers it to a physical `EnumerableOpenSearchTableSpool`
(an `EnumerableRel` that also extends `TableSpool`). Both nodes share the input
row type, so the conversion is row-type preserving (no Volcano set conflict).
The physical operator drains its input, batches index requests, writes each
batch via the native bulk path under retry/backoff, and emits every input row
downstream unchanged (pass-through). The destination index and cluster client
come from `getTable().unwrap(OpenSearchIndex.class)`.

The full-table-scope reindex fast lane is a future optimization layered on the
same rule via the `CollectWriteStrategy.isFullTableScope` detector:

- **Full table scope, rows unconsumed → reindex.** When the input is a single
  `OpenSearchIndex` scan (optionally carrying a pushed-down filter) with no
  row-changing operator above it, and the `collect` output is terminal (the
  pass-through rows are not consumed downstream), lower instead to a server-side
  `ReindexAction` whose `source = src_index + pushed-down query` and
  `dest = dest_index`. The rows never transit the coordinator.
- **Everything else → the bulk spool** above, which is correct for every input
  shape and output size; its only adaptive behavior is internal (how many
  batches it emits, and whether a PIT-backed read is created).

The full-scope detection rides the pushdown analysis the scan layer already
performs; the row-consumed test is a plan-walk checking whether `collect` is the
output node. When either condition fails, the converter rule falls back to the
batched-bulk operator, so correctness never depends on the optimization firing.

### Streaming read (PIT)

When the input is a document-returning scan, the read streams the full source
through Point-in-Time pagination rather than a single bounded window. PIT engages
under the conditions enforced by the existing request builder:

- the request is a document request (`size != 0`; aggregation requests with
  `size == 0` return a bounded result in one shot and bypass PIT);
- the source index mapping is non-empty;
- the requested total exceeds `index.max_result_window`.

A PIT id is created once with a keep-alive, each page advances via `search_after`
with a `_doc` plus `_shard_doc` tiebreaker, the page size is
`index.max_result_window`, and the PIT is cleaned on the final page. The PIT
keep-alive must outlast the full read-and-write duration; `collect` imposes no
fixed top-side size and lets the scan stream the entire source.

### Streaming write (batched bulk — the committed core)

The committed write engine drains the operator's input, accumulates rows into a
bounded batch (default 1000, the existing `BATCH_SIZE`), and issues a `bulk()`
per batch through the in-process `OpenSearchClient`. Each `bulk()` is wrapped
with `Retry` + `BackoffPolicy` so write-thread-pool rejections (HTTP 429),
indexing pressure, segment-replication pressure, and remote-store lag are
absorbed as retries rather than counted as per-document failures. Bulk reports
per-item status, so genuine document errors stay distinguishable from transient
pressure. No refresh is forced, so visibility stays on the destination's own
`index.refresh_interval` cycle — the read does not block on the written
documents becoming searchable.

- **Small output** collapses to one `bulk()`: the batch never fills, so the
  operator flushes a single partial batch at end-of-input. No PIT or scroll is
  created, and latency is one round trip plus the destination's write time.
- **Large output** iterates: batches flush as they fill, backpressure naturally
  paces the producer (a slow `bulk()` stalls the drain, which stalls the PIT
  read), and peak heap stays at one batch on the write side and one page on the
  read side. There is no coordinator-side buffering of the full result set.

This is the OSS bulk path's documented contract: individual document failures in
a bulk request do not fail the whole request (each item carries its own status),
and shard indexing backpressure surfaces as 429 rejections that the retry layer
is responsible for absorbing.

### Server-side reindex (the full-table-scope optimization)

`ReindexAction` (server-side bulk-by-scroll) runs the entire read-write loop
server-side on the `reindex` thread pool with built-in throttling, slicing, and
cancellation, avoiding the coordinator read-write double hop. It is the right
engine for one specific, common shape: **copying a whole index (or a filtered
slice of it) into another** — `source=src [| where ...] | collect index=dest`,
where the plan is a full table scope and nothing above the scan changes the
rows. For that workload it is dramatically cheaper than draining every document
through the coordinator, so `collect` selects it rather than the default bulk
path.

It coexists with, rather than replaces, the batched-bulk engine, because two
constraints bound where it applies:

- **It does not surface rows.** `ReindexAction` returns a `BulkByScrollResponse`
  (counts only). It can therefore only be used when the pass-through rows are
  **not consumed downstream** — i.e. `collect` is the terminal command. In that
  position there is no consumer for the rows, so emitting the reindex summary is
  the correct output and pass-through is moot. The moment a downstream command
  consumes the rows, the converter rule falls back to batched bulk (whose single
  PIT read serves both the write and the pass-through output), so pass-through
  is never violated.
- **Its setup cost is disproportionate for small input** (task spawn + scroll
  context + thread-pool scheduling). The selector only chooses reindex above a
  size threshold; below it, a single `bulk()` over the streamed scan is cheaper
  and the optimization is skipped.

So the decision is a plan-shape gate, evaluated by the converter rule:
**full table scope + terminal `collect` + large input → reindex; anything else →
batched bulk.** Both are real, supported paths; reindex is the fast lane for the
bulk-copy case, batched bulk is the correct general engine for everything else.

The `reindex-client` artifact provides the engine and request types at compile
time (the reindex transport action itself ships in the OpenSearch distribution).

### Safety and security

- **Pre-existence** is enforced at plan time via destination resolution;
  auto-creation of the destination index is refused.
- **System and hidden indices** (dot-prefixed) are refused as destinations.
- **Authorization.** The write is authorized as the calling principal; the
  caller's thread context is threaded through the write so index-level
  permissions (and field/document-level controls where configured) apply.
- **Backpressure.** The write path uses retry plus backoff for 429 and pressure
  signals; persistent failures are surfaced rather than silently dropped.
- **Append-only with auto-id** means re-running a query duplicates rows; there is
  no destructive operation on existing documents.

### Rollout

1. Wire `visitCollect` to `LogicalTableSpool`; remove the bespoke sink nodes.
2. Implement the single physical modify operator and converter rule: batched
   `bulk()` over the (PIT-streamed when applicable) input, pass-through output.
   Small output collapses to one `bulk()`; large output streams.
3. Add the retry/backoff backpressure layer and confirm 429 / pressure signals
   are absorbed while per-document errors surface.
4. Add safety controls: pre-existence (plan-time), dot/hidden-index refusal,
   authorization threading.
5. Land `CalcitePPLCollectIT` and plan-shape unit tests, covering small-output
   (single-batch, no PIT), large-output (multi-batch, PIT-streamed) round
   trips, the absent-index error, and dot-index refusal.
6. Add the full-table-scope reindex path: when the converter rule sees a
   terminal `collect` over a single (optionally filtered) `OpenSearchIndex` scan
   above the size threshold, lower to `ReindexAction` instead of batched bulk.
   Add IT coverage for the index-copy round trip and the fallback to batched
   bulk when the rows are consumed downstream.

## Alternative

- **Server-side reindex (`ReindexAction`) as the *universal* write engine.** An
  earlier framing made reindex the write engine for all pushable input. That is
  rejected because reindex returns counts, not rows, so it cannot serve a
  `collect` whose rows are consumed downstream, and its task / scroll setup is
  wasteful for small input. Reindex is instead a co-equal, plan-shape-selected
  path for the full-table-scope copy case only (see "Server-side reindex"
  above); the two engines coexist rather than one replacing the other.
- **Bounded async-bulk window.** A refinement of the committed batched-bulk
  engine that keeps a capped number of `bulk()` requests in flight rather than
  one at a time, to raise large-output throughput. It is a compatible future
  tuning of the same operator, not a separate design, and is out of scope for
  the first version.
- **Calcite built-in `EnumerableTableModify` only.** Reusing the stock physical
  modify node would avoid a custom operator, but it returns a write-count and
  drains its input into an in-memory collection, which matches neither pass-
  through output nor the server-side reindex strategy. It could serve as a
  partial, non-pass-through workaround on the non-pushable path only.
- **Field annotation options.** A future extension may let `collect` stamp
  additional constant or computed fields onto every written document (for
  example a constant source tag or a write-time timestamp). The option grammar
  and the set of computed fields are open and intentionally out of scope for the
  core command here.
- **User-facing async/cancel surface.** Exposing a task id, cancel, or
  rethrottle API is deferred; the streaming write already runs off the request
  thread.

## Implementation Discussion

- **Pass-through via an existing Calcite node.** Calcite's `LogicalTableModify`
  returns a single BIGINT rowcount (`createDmlRowType(INSERT)`) and its physical
  form `EnumerableTableModify` drains into an in-memory collection, so neither
  expresses pass-through. `LogicalTableSpool` (Spool family) does: it forwards
  its input rows AND writes them to a target table, and being a `SingleRel` its
  row type equals the input. So pass-through holds with an existing node, the
  converter rule is row-type preserving, and there is no Volcano set conflict.
- **One plan-time branch: the reindex selector.** The converter rule makes a
  single structural decision — full table scope + terminal `collect` + above
  the size threshold → lower to `ReindexAction`; otherwise → the batched-bulk
  operator. The detection reuses the scan layer's pushdown analysis (is the
  input a single scan with only a pushed-down filter?) plus a plan-walk (is
  `collect` the output node?). When in doubt it must fall back to batched bulk,
  since that path is always correct.
- **Within the batched-bulk operator, small/large is a runtime continuum, not a
  plan-time branch.** The operator does not predict output size at planning
  time. It always batches; a small result produces one partial batch and never
  opens a PIT, while a large result fills and flushes batches and streams the
  read via PIT. The only plan-time signal it needs is whether the input is a
  document scan (PIT-eligible) versus an aggregation result (`size == 0`,
  bounded, no PIT), which the scan layer already determines.
- **PIT keep-alive sizing.** When a PIT-backed read is created (large
  document-scan input), the keep-alive must outlast the entire read-and-write
  duration; it needs to account for write backpressure stretching wall-clock
  time, not just read time. Small-output writes create no PIT and avoid this
  entirely.
- **Backpressure semantics.** Retry-with-backoff must distinguish transient
  pressure (429, indexing pressure, segrep / remote-store lag) from genuine
  per-document failures, so the former are absorbed as retries and the latter
  surfaced via the per-item bulk status.
- **No new node types.** Both the AST (`Collect`) and the logical plan
  (`LogicalTableSpool`) reuse existing Calcite nodes; the only new class is the
  physical `EnumerableOpenSearchTableSpool` plus its converter rule. The prior
  bespoke sink nodes are removed.
