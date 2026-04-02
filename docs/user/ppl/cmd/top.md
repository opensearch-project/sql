
# top

The `top` command returns the most common values for a field.

## Syntax

The `top` command has the following syntax:

```syntax
top [N] <field-list> [by <group-list>]
```

## Parameters

The `top` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `N` | Optional | The number of results to return. Default is `10`. |
| `<field-list>` | Required | A comma-delimited list of field names. |
| `<group-list>` | Optional | A comma-delimited list of field names to group by. |

## Example 1: Find the most common values

The following query finds the most common severity levels across all logs, helping you understand the overall health of your system:

```ppl
source=otellogs
| top 3 severityText
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+-------+
| severityText | count |
|--------------+-------|
| INFO         | 6     |
| ERROR        | 5     |
| WARN         | 4     |
+--------------+-------+
```

## Example 2: Find the most common values grouped by field

The following query finds the most frequently logging service for each severity level, useful for identifying which services generate the most noise:

```ppl
source=otellogs
| top 1 `resource.attributes.service.name` by severityText
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------------------------+-------+
| severityText | resource.attributes.service.name | count |
|--------------+----------------------------------+-------|
| ERROR        | checkout                         | 2     |
| INFO         | frontend                         | 4     |
| DEBUG        | cart                             | 2     |
| FATAL        | payment                          | 1     |
| WARN         | frontend-proxy                   | 2     |
+--------------+----------------------------------+-------+
```

## Example 3: Find the most common services

The following query identifies the top 3 services generating the most log entries:

```ppl
source=otellogs
| top 3 `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------------------------+-------+
| resource.attributes.service.name | count |
|----------------------------------+-------|
| frontend                         | 4     |
| product-catalog                  | 4     |
| cart                             | 3     |
+----------------------------------+-------+
```

