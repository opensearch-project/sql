
# collect

The `collect` command appends the rows flowing through the pipeline into a pre-existing destination index, and passes those same rows through as the query result (pass-through semantics). It is a terminal write: the events are both persisted to the destination and returned to the client unchanged.

> **Note**: The destination index must already exist; `collect` never creates it. Writing to a system or hidden (dot-prefixed) index is refused at plan time.

## Syntax

The `collect` command has the following syntax:

```syntax
collect index=<destination> [source=<string>] [host=<string>] [sourcetype=<string>] [marker=<string>] [testmode=<boolean>]
```

## Parameters

The `collect` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<destination>` | Required | The pre-existing index to append rows to. Must not be a system or hidden (dot-prefixed) index. |
| `source` | Optional | Stamps a literal `source` field with the given value onto every written (and passed-through) row. |
| `host` | Optional | Stamps a literal `host` field onto every row. |
| `sourcetype` | Optional | Stamps a literal `sourcetype` field onto every row. |
| `marker` | Optional | Stamps a literal `marker` field onto every row. |
| `testmode` | Optional | When `true`, performs a dry run: the rows pass through (with any stamps applied) but nothing is written to the destination. Default is `false`. |

## Example 1: Appending pipeline results to a destination index

The following query filters error events and appends them to the pre-existing `errors_summary` index, while returning the same rows as the result:

```ppl
source=otellogs
| where severityText = 'ERROR'
| fields severityText, body
| collect index=errors_summary
```

## Example 2: Stamping provenance fields on write

The following query stamps a `source` and `marker` value onto every written row, so the destination documents can be attributed later:

```ppl
source=otellogs
| where severityText = 'ERROR'
| fields severityText, body
| collect index=errors_summary source='otel-pipeline' marker='run-42'
```

## Example 3: Dry run with testmode

The following query previews what would be written (including the stamped `source` field) without actually writing to the destination:

```ppl
source=otellogs
| where severityText = 'ERROR'
| fields severityText, body
| collect index=errors_summary source='otel-pipeline' testmode=true
```

## Limitations

- The destination index must pre-exist; `collect` does not create it.
- A source larger than `index.max_result_window` is streamed in full to the destination via point-in-time (PIT) paging; the pass-through result returned to the client is still bounded by `plugins.query.size_limit`.

> **Doctest note**: `collect` is intentionally omitted from `docs/category.json` (doctest). Its examples require a pre-existing destination index, which the doctest harness cannot provision from a `.md` example block, so the output cannot be projected to deterministic values there. The command is verified end-to-end by `CalcitePPLCollectIT` instead.
