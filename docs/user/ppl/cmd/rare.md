
# rare

The `rare` command identifies the least common combination of values across all fields specified in the field list.

> **Note**: The command returns up to 10 results for each distinct combination of values in the group-by fields.

> **Note**: The `rare` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `rare` command has the following syntax:

```syntax
rare [rare-options] <field-list> [by-clause]
```

## Parameters

The `rare` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Required | A comma-delimited list of field names. |
| `<by-clause>` | Optional | One or more fields to group the results by. |
| `rare-options` | Optional | Additional options for controlling output: <br> - `showcount`: Whether to create a field in the output containing the frequency count for each combination of values. Default is `true`. <br> - `countfield`: The name of the field that contains the count. Default is `count`. <br> - `usenull`: Whether to output null values. Default is the value of `plugins.ppl.syntax.legacy.preferred`. |

## Example 1: Find the least common values without showing counts

The following query uses `showcount=false` to find the least common severity levels without displaying frequency counts:

```ppl
source=otellogs
| rare showcount=false severityText
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+
| severityText |
|--------------|
| DEBUG        |
| WARN         |
| INFO         |
| ERROR        |
+--------------+
```

## Example 2: Find the least common values grouped by field

The following query finds the least common severity levels grouped by service:

```ppl
source=otellogs
| rare showcount=false severityText by `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 12/12
+----------------------------------+--------------+
| resource.attributes.service.name | severityText |
|----------------------------------+--------------|
| product-catalog                  | DEBUG        |
| product-catalog                  | ERROR        |
| product-catalog                  | WARN         |
| frontend-proxy                   | ERROR        |
| frontend-proxy                   | WARN         |
| recommendation                   | ERROR        |
| payment                          | ERROR        |
| checkout                         | INFO         |
| checkout                         | ERROR        |
| cart                             | INFO         |
| cart                             | DEBUG        |
| frontend                         | INFO         |
+----------------------------------+--------------+
```

## Example 3: Find the least common values with frequency counts

The following query finds the least common severity levels with their frequency counts:

```ppl
source=otellogs
| rare severityText
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+-------+
| severityText | count |
|--------------+-------|
| DEBUG        | 3     |
| WARN         | 4     |
| INFO         | 6     |
| ERROR        | 7     |
+--------------+-------+
```

## Example 4: Customize the count field name

The following query uses `countfield` to specify a custom name for the frequency count field:

```ppl
source=otellogs
| rare countfield='cnt' severityText
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+-----+
| severityText | cnt |
|--------------+-----|
| DEBUG        | 3   |
| WARN         | 4   |
| INFO         | 6   |
| ERROR        | 7   |
+--------------+-----+
```

## Example 5: Specify null value handling

The following query uses `usenull=false` to exclude null values:

```ppl
source=otellogs
| rare usenull=false instrumentationScope.name
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-----------------------------------------------------------------------------+-------+
| instrumentationScope.name                                                   | count |
|-----------------------------------------------------------------------------+-------|
| Microsoft.Extensions.Hosting                                                | 1     |
| go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc | 1     |
| @opentelemetry/instrumentation-http                                         | 2     |
+-----------------------------------------------------------------------------+-------+
```

The following query uses `usenull=true` to include null values in the results:

```ppl
source=otellogs
| rare usenull=true instrumentationScope.name
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------------------------------------------------------------+-------+
| instrumentationScope.name                                                   | count |
|-----------------------------------------------------------------------------+-------|
| Microsoft.Extensions.Hosting                                                | 1     |
| go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc | 1     |
| @opentelemetry/instrumentation-http                                         | 2     |
| null                                                                        | 16    |
+-----------------------------------------------------------------------------+-------+
```