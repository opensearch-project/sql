
# rare

The `rare` command returns the least common values for a field.

## Syntax

The `rare` command has the following syntax:

```syntax
rare [N] <field-list> [by <group-list>]
```

## Parameters

The `rare` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `N` | Optional | The number of results to return. Default is `10`. |
| `<field-list>` | Required | A comma-delimited list of field names. |
| `<group-list>` | Optional | A comma-delimited list of field names to group by. |

## Example 1: Find the least common values

The following query finds the rarest severity levels, helping you identify unusual log patterns that may warrant investigation:

```ppl
source=otellogs
| rare severityText
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+-------+
| severityText | count |
|--------------+-------|
| FATAL        | 2     |
| DEBUG        | 3     |
| WARN         | 4     |
| ERROR        | 5     |
| INFO         | 6     |
+--------------+-------+
```

## Example 2: Find the rarest services

The following query identifies services with the fewest log entries, which may indicate services that are underinstrumented or rarely active:

```ppl
source=otellogs
| rare 3 `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------------------------+-------+
| resource.attributes.service.name | count |
|----------------------------------+-------|
| recommendation                   | 1     |
| payment                          | 2     |
| cart                             | 3     |
+----------------------------------+-------+
```

