
# outputlookup

The `outputlookup` command is a terminal write sink: it materializes the current pipeline result into a lookup index and returns a single `rows_written` count. Use it to build or refresh a lookup (dimension) dataset from a search, which can then be read back with `source=<name>` or enriched into other searches with the `lookup` command.

A lookup name refers to an alias over a private backing index. Overwrite writes a fresh backing index and atomically repoints the alias, so readers of the lookup always see a complete, consistent dataset — never a half-written state.

## Syntax

The `outputlookup` command has the following syntax:

```syntax
outputlookup [append=<bool>] [override_if_empty=<bool>] [key_field=<field>(, <field>)*] [max=<int>] <name>
```

The following are examples of the `outputlookup` command syntax:

```syntax
source = table | outputlookup my_lookup
source = table | where status = 'active' | outputlookup active_hosts
source = table | stats count by region | outputlookup region_counts
source = table | fields id, name | outputlookup append=true my_lookup
source = table | outputlookup override_if_empty=false my_lookup
source = table | fields id, name | outputlookup key_field=id my_lookup
source = table | fields region, host, val | outputlookup key_field=region, host my_lookup
source = table | outputlookup max=1000 sample_lookup
```

## Parameters

The `outputlookup` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<name>` | Required | The lookup name to write to. Resolves to an alias over a backing index. It must be a lookup alias or not yet exist; an existing concrete index (including the source index itself) is rejected. |
| `append` | Optional | When `false` (default), overwrites the lookup with the result. When `true`, appends the result to the existing lookup. Because OpenSearch is schemaless, appended rows may introduce new fields (unlike Splunk, which freezes the schema). Default is `false`. |
| `override_if_empty` | Optional | When `true` (default), an empty result clears the existing lookup. When `false`, an empty result leaves the existing lookup intact. Default is `true`. |
| `key_field` | Optional | One or more fields (comma-separated) used as the upsert key. Rows are written by a deterministic `_id` derived from the key values, so re-running the same command updates matching rows in place instead of creating duplicates. Setting `key_field` implies `append=true`. Every field listed must be a field of the result; a multivalue key value is rejected. |
| `max` | Optional | Caps the number of rows written. |

## Example 1: Building a lookup from an aggregation

The following query writes per-region counts into a lookup and returns the number of rows written:

```ppl ignore
source = events
  | stats count as cnt by region
  | outputlookup region_counts
```

The query returns the following result:

```text
+--------------+
| rows_written |
|--------------|
| 3            |
+--------------+
```

The lookup can then be read back:

```ppl ignore
source = region_counts
```

## Example 2: Idempotent upsert with `key_field`

The following query upserts by `id`, so re-running it updates existing rows instead of duplicating them:

```ppl ignore
source = users
  | fields id, name, department
  | outputlookup key_field=id users_lookup
```

Running the command a second time with overlapping `id` values leaves the row count unchanged for the overlapping keys and only inserts genuinely new keys.

## Example 3: Preserving a lookup on an empty result

The following query refreshes `error_hosts` only when the search returns rows; an empty result keeps the previous lookup intact:

```ppl ignore
source = logs
  | where level = 'ERROR'
  | fields host
  | outputlookup override_if_empty=false error_hosts
```

## Limitations

- `outputlookup` is a terminal command: it returns a `rows_written` count rather than forwarding the input rows (a deliberate divergence from Splunk, which forwards the events).
- Splunk file-location and CSV-encoding options (`createinapp`, `.csv.gz`, `output_format`) are not supported; the destination is always a lookup index alias.
- The write executes under the caller's security context. The caller needs write, create-index, alias, and delete privileges on the destination.
