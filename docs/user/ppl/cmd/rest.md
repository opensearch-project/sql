# rest

The `rest` command is a leading command that reads an allow-listed, read-only in-cluster management endpoint (cluster/cat/nodes) and emits the response as PPL rows. Its rows come from the endpoint dispatch, not from an index, so `rest` appears at the start of a query.

> **Note**: The `rest` command is supported only on the Calcite query engine (`plugins.calcite.enabled=true`). Each endpoint has a fixed output schema, and the dispatch runs under the caller's security context, so a user who cannot call an endpoint directly cannot call it through `rest`. The command is read-only; mutating and non-allow-listed endpoints are rejected. Each endpoint requires the same cluster-monitor privilege as calling it natively, so `rest` grants no extra access. Some allow-listed endpoints surface operational metadata (for example `/_cat/nodes` exposes node addresses and resource utilization, `/_cat/plugins` the installed plugin inventory, and `/_cluster/state` cluster-state identifiers); this is a deliberate, read-only, monitor-privileged trade-off. `/_cluster/settings` is redacted with the node's setting filter so `Property.Filtered` keys are not surfaced.

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
| `timeout=<duration>` | Optional | Reserved for forward compatibility. It is currently rejected with a clear error, because a single uniform timeout does not map cleanly across the different endpoints. |
| `<get-arg>=<value>` | Optional | Endpoint query arguments, validated per endpoint by both key and value (for example `local=true` for `/_cluster/health`, `health=green` for `/_cat/indices`, `expand_wildcards=open` for `/_resolve/index`). |

## Allow-list

`rest` resolves only an explicit, curated set of read-only endpoints. Anything outside the list, including any mutating endpoint, is rejected with a clear error.

| Endpoint | Output columns | Accepted args |
| --- | --- | --- |
| `/_cluster/health` | `cluster_name` (string), `status` (string), `number_of_nodes` (integer), `number_of_data_nodes` (integer), `active_primary_shards` (integer), `active_shards` (integer), `relocating_shards` (integer), `initializing_shards` (integer), `unassigned_shards` (integer), `timed_out` (boolean) | `local` |
| `/_cluster/state` | `cluster_name` (string), `state_uuid` (string), `version` (long), `cluster_manager_node` (string) | (none) |
| `/_cluster/settings` | `setting` (string), `value` (string), `tier` (string) | (none) |
| `/_cat/indices` | `index` (string), `health` (string), `pri` (integer), `rep` (integer), `active_shards` (integer) | `health` |
| `/_cat/nodes` | `name` (string), `ip` (string), `node_role` (string), `heap_percent` (integer), `ram_percent` (integer), `cpu` (integer) | (none) |
| `/_cat/cluster_manager` | `id` (string), `host` (string), `ip` (string), `node` (string) | (none) |
| `/_cat/plugins` | `name` (string), `component` (string), `version` (string) | (none) |
| `/_cat/shards` | `index` (string), `shard` (integer), `prirep` (string), `state` (string), `node` (string) | (none) |
| `/_resolve/index` | `name` (string), `type` (string) | `expand_wildcards` |

## Example 1: Counting the nodes in the cluster

The following query reads cluster health and projects a column that is deterministic on a single-node cluster:

```ppl
| rest '/_cluster/health' | fields number_of_nodes
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------------+
| number_of_nodes |
|-----------------|
| 1               |
+-----------------+
```

`/_cluster/health` also exposes `status`, `active_shards`, and the other columns listed in the allow-list, which you can project and filter the same way.

## Example 2: Composing downstream commands over a cat endpoint

The `rest` row source composes with downstream `where`, `sort`, `stats`, and `fields` exactly like an index scan. The following query reads `/_cat/cluster_manager` and counts the rows:

```ppl
| rest '/_cat/cluster_manager' | stats count() as managers
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+
| managers |
|----------|
| 1        |
+----------+
```

For example, `| rest '/_cat/indices' | where health = 'green' | sort index | fields index, health, pri` lists green indexes; the projected columns come from the endpoint's fixed schema.
