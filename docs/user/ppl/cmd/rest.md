# rest

The `rest` command is a leading command that reads an allow-listed, read-only in-cluster management endpoint and emits the response as PPL rows. Its rows come from the endpoint dispatch, not from an index, so `rest` appears at the start of a query.

> **Note**: The `rest` command is supported only on the Calcite query engine (`plugins.calcite.enabled=true`). Each endpoint has a fixed output schema, and the dispatch runs under the caller's security context, so a user who cannot call an endpoint directly cannot call it through `rest`. The command is read-only; mutating and non-allow-listed endpoints are rejected. Each endpoint requires the same cluster-monitor privilege as calling it natively, so `rest` grants no extra access.

The `rest` command is a generic, extensible framework: a plugin contributes additional read-only endpoints through the `RestEndpointProvider` extension point without changing the grammar. This first version ships a single built-in endpoint, `/_cluster/health`. Additional endpoints (for example `/_cat/nodes`, `/_cat/shards`, `/_cluster/state`, `/_cluster/settings`) can be added in follow-ups, together with optional response redaction applied centrally at the endpoint choke point.

## Enabling the command

`/_cluster/health` is **enabled by default**: `plugins.ppl.rest.allowed_endpoints` defaults to `["/_cluster/health"]`. Any other endpoint is rejected until a deployment adds it to the allow-list (a node-level setting, applied at node startup and not changeable at runtime):

```yaml
plugins.ppl.rest.allowed_endpoints: ["/_cluster/health"]
```

Every endpoint must be listed explicitly by name; there is no wildcard, so a newly installed or upgraded provider is never enabled without an explicit allow-list change. Set an empty list to disable the command entirely.

## Syntax

```syntax
rest <endpoint-path> [count=<int>] [<get-arg>=<value> ...]
```

## Parameters

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<endpoint-path>` | Required | An allow-listed, read-only endpoint path (see the allow-list below), for example `/_cluster/health`. |
| `count=<int>` | Optional | Caps the number of emitted rows. |
| `<get-arg>=<value>` | Optional | Endpoint query arguments, validated per endpoint by both key and value (for example `local=true` for `/_cluster/health`). |

## Allow-list

`rest` resolves only an explicit, curated set of read-only endpoints. Anything outside the list, including any mutating endpoint, is rejected with a clear error.

| Endpoint | Output columns | Accepted args |
| --- | --- | --- |
| `/_cluster/health` | `cluster_name` (string), `status` (string), `number_of_nodes` (integer), `number_of_data_nodes` (integer), `active_primary_shards` (integer), `active_shards` (integer), `relocating_shards` (integer), `initializing_shards` (integer), `unassigned_shards` (integer), `timed_out` (boolean) | `local` |

## Example: Counting the nodes in the cluster

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

`/_cluster/health` also exposes `status`, `active_shards`, and the other columns listed in the allow-list. The `rest` row source composes with downstream `where`, `sort`, `stats`, and `fields` exactly like an index scan, for example `| rest '/_cluster/health' | where status = 'green' | fields status, active_shards`.
