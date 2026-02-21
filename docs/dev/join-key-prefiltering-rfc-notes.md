# Join Key Pre-filtering: Incremental RFC Notes (Issue #5093)

This document turns the RFC discussion into an incremental, reviewable implementation plan.

## Problem Recap

For sparse star-join workloads, the current join path can still scan most/all of the fact table even when the dimension-side filters are very selective.

Example shape:

```ppl
source=request_logs
| lookup dim_lookup host_key append service_name, environment, region
| where _lookup = "host"
| where service_name = "payment-service"
| head 10
| fields request_id, region;
```

Current behavior often results in:
1. filtered scan of `dim_lookup`
2. broad scan of `request_logs`
3. hash join + late `head`

## Incremental Plan

### Phase 1 (safe, narrow)

Apply key pre-filtering only when all conditions below are met:

- join type is effectively `inner` after predicates
- join condition is a single equality key (`fact.key = dim.key`)
- dimension side has pushdown-safe filters
- dimension key cardinality is below threshold (configurable)

Execution sketch:

1. evaluate/pushdown dimension-side predicates first
2. materialize matching join keys (bounded set)
3. inject fact-side `terms` filter on join key
4. continue existing join projection path

### Phase 2 (broader coverage)

- add support for selected left joins where null-preserving semantics are unchanged
- add adaptive thresholding based on planner stats / max terms size
- early-limit strategies for top-k style pipelines where semantically safe

## Correctness Guardrails

- Never apply optimization if any semantic ambiguity exists.
- Preserve existing behavior for unsupported query shapes.
- Keep optimization behind a feature flag in initial rollout.

## Suggested Config Knobs

- `plugins.sql.optimization.joinKeyPrefilter.enabled` (default: false)
- `plugins.sql.optimization.joinKeyPrefilter.maxKeys` (default: 10_000)
- `plugins.sql.optimization.joinKeyPrefilter.maxBytes` (default: bounded)

## Validation Matrix

1. **Correctness tests**
   - result equivalence against baseline for supported shapes
   - no change for unsupported shapes
2. **Performance tests**
   - sparse star-join fixture (expect substantial speedup)
   - dense/non-selective joins (expect neutral/slight overhead)
3. **Safety tests**
   - large keyset guard (optimizer must bail out)

## Benchmark Reproduction Aid

Use the helper script:

```bash
scripts/benchmark_join_prefilter.sh
```

By default this calls `POST /_plugins/_ppl` repeatedly and reports timing. See script usage for custom endpoint/query and hyperfine integration.
