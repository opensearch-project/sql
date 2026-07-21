
# outputlookup

The `outputlookup` command is a terminal write sink: it materializes the current pipeline result into a lookup and returns a single `rows_written` count. Use it to build or refresh a lookup (dimension) dataset from a search, which can then be read back with `source=<name>` or enriched into other searches with the `lookup` command.

A lookup `<name>` is a filtered alias over a dedicated per-lookup, plain, non-hidden backing index (`<name>__lookup`, the same artifact the OpenSearch Dashboards data importer produces), so lookups are isolated from one another. Overwrite writes a fresh slice into the backing index and atomically repoints the alias, so a reader always sees the whole old lookup or the whole new one, never a partial state; append adds the result to the current slice.

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
makeresults format=json data='[{"host":"h1","role":"web"},{"host":"h2","role":"db"}]' | outputlookup host_roles
makeresults format=csv data='code,label\n200,OK\n404,Not Found' | outputlookup key_field=code http_codes
```

## Parameters

The `outputlookup` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<name>` | Required | The lookup name. It is a filtered alias published over the backing index; read it back with `source=<name>` or `lookup <name>`. If the name is an existing concrete index, or a non-filtered alias, the command refuses rather than overwriting it. |
| `append` | Optional | When `false` (default), overwrites the lookup with the result. When `true`, appends the result to the existing lookup. Because OpenSearch is schemaless, appended rows may introduce new fields. Default is `false`. |
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

## Example 4: Seeding a lookup from inline data with `makeresults`

`outputlookup` does not need a source index. Combined with `makeresults`, it seeds a small lookup from inline JSON or CSV. This is also how the OpenSearch Dashboards CSV import builds a lookup: it generates a `makeresults ... | outputlookup <name>` query.

```ppl ignore
makeresults format=json data='[{"host":"h1","role":"web"},{"host":"h2","role":"db"}]'
  | outputlookup host_roles
```

The lookup can then be read back or used to enrich another search:

```ppl ignore
source = access_logs | lookup host_roles host
```

Use `key_field` for an idempotent, re-runnable seed (matching rows update in place instead of duplicating):

```ppl ignore
makeresults format=csv data='code,label\n200,OK\n404,Not Found'
  | outputlookup key_field=code http_codes
```

## Limitations

- `outputlookup` is a terminal command: it returns a `rows_written` count rather than forwarding the input rows.
- The destination is always a lookup; there is no file output target.
- Overwrite is weak/eventually consistent but gap-free: it writes a fresh slice and atomically repoints the alias, so a concurrent read always sees the whole old slice or the whole new one, never a partial state. The previous slice is orphaned until reclaimed.
- Each lookup has its own backing index, so its field types are independent of other lookups. Types are fixed by the first write (first-writer-wins) and dynamic-mapped for new fields thereafter; a later value whose type is incompatible with an existing field fails that document rather than silently retyping the field.
- `outputlookup` is for bounded lookup (dimension) tables, not a bulk data sink. The number of rows a single write may produce is capped by `plugins.ppl.outputlookup.max_rows` (NodeScope, Dynamic, default `1000000`, minimum `1`). Exceeding it fails the query (no slice is written) rather than silently truncating. The optional `max=<int>` command argument truncates to at most N rows and must not exceed the setting. To write a larger lookup you can raise `plugins.ppl.outputlookup.max_rows` (it is dynamic; a higher ceiling costs more coordinator heap per write), or ingest large data through a bulk indexing path instead.
- The write executes under the caller's security context. The caller needs write and get privileges on the backing index and alias privileges to publish or repoint the filtered alias, plus create-index the first time the backing index is created; no cluster-level privilege is required. Because the backing index is an ordinary non-hidden index, write authorization scopes to it like any bulk grant.
