# rest

The `rest` command is a leading command that reads an allow-listed, read-only in-cluster management endpoint (cluster/cat/nodes) and emits the response as PPL rows. Its rows come from the endpoint dispatch, not from an index, so `rest` appears at the start of a query.

> **Note**: The `rest` command is supported only on the Calcite query engine (`plugins.calcite.enabled=true`). Each endpoint has a fixed output schema, and the dispatch runs under the caller's security context, so a user who cannot call an endpoint directly cannot call it through `rest`. The command is read-only; mutating and non-allow-listed endpoints are rejected.

## Syntax

The `rest` command has the following syntax:

```syntax
rest <endpoint-path> [count=<int>] [timeout=<duration>] [<get-arg>=<value> ...]
```

## Parameters

The `rest` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<endpoint-path>` | Required | An allow-listed, read-only endpoint path (see the allow-list below), for example `/_cluster/health`. |
| `count=<int>` | Optional | Caps the number of emitted rows. |
| `timeout=<duration>` | Optional | Request timeout passed to the transport action, for example `30s`. |
| `<get-arg>=<value>` | Optional | Endpoint query arguments, validated per endpoint (for example `level=indices` for `/_cluster/health`). |

## Allow-list

`rest` resolves only an explicit, curated set of read-only endpoints. Anything outside the list, including any mutating endpoint, is rejected with a clear error.

| Endpoint | Output columns |
| --- | --- |
| `/_cluster/health` | `cluster_name` (string), `status` (string), `number_of_nodes` (integer), `number_of_data_nodes` (integer), `active_primary_shards` (integer), `active_shards` (integer), `relocating_shards` (integer), `initializing_shards` (integer), `unassigned_shards` (integer), `timed_out` (boolean) |
| `/_cat/indices` | `index` (string), `health` (string), `pri` (integer), `rep` (integer), `active_shards` (integer) |

## Example 1: Reading cluster health

The following query reads cluster health and projects two columns:

```ppl
| rest /_cluster/health | fields status, number_of_nodes
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+-----------------+
| status | number_of_nodes |
|--------+-----------------|
| green  | 1               |
+--------+-----------------+
```

## Example 2: Listing indexes from cat indices

The following query lists indexes and filters and sorts them on the Calcite plan:

```ppl
| rest /_cat/indices | where health = "green" | sort index | fields index, health, pri
```

The downstream `where`, `sort`, and `fields` compose over the `rest` row source exactly like any index scan.
