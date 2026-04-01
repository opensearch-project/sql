
# rename

The `rename` command renames one or more fields in the search results.

## Syntax

The `rename` command has the following syntax:

```syntax
rename <source-field> AS <target-field> ["," <source-field> AS <target-field>]...
```

## Parameters

The `rename` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<source-field>` | Required | The name of the field to rename. |
| `<target-field>` | Required | The new name for the field. |

## Example 1: Rename a single field

The following query renames `severityText` to `level` for cleaner output when sharing results with a team:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| rename severityText as level
| sort severityNumber, `resource.attributes.service.name`
| fields level, `resource.attributes.service.name`, body
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-------+----------------------------------+----------------------------------------------------------------------------------------------+
| level | resource.attributes.service.name | body                                                                                         |
|-------+----------------------------------+----------------------------------------------------------------------------------------------|
| ERROR | api-gateway                      | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR | auth-service                     | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      |
| ERROR | cart-service                     | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
+-------+----------------------------------+----------------------------------------------------------------------------------------------+
```

## Example 2: Rename multiple fields

The following query renames multiple fields to shorter, dashboard-friendly names:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| rename severityText as level, severityNumber as code
| sort code, `resource.attributes.service.name`
| fields level, code, `resource.attributes.service.name`
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-------+------+----------------------------------+
| level | code | resource.attributes.service.name |
|-------+------+----------------------------------|
| ERROR | 17   | api-gateway                      |
| ERROR | 17   | auth-service                     |
| ERROR | 17   | cart-service                     |
+-------+------+----------------------------------+
```

## Example 3: Rename and use in subsequent commands

The following query renames a field and then uses the new name in a `where` filter to find critical issues:

```ppl
source=otellogs
| rename severityNumber as level_num
| where level_num >= 17
| sort level_num, `resource.attributes.service.name`
| fields severityText, level_num, `resource.attributes.service.name`
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+-----------+----------------------------------+
| severityText | level_num | resource.attributes.service.name |
|--------------+-----------+----------------------------------|
| ERROR        | 17        | api-gateway                      |
| ERROR        | 17        | auth-service                     |
| ERROR        | 17        | cart-service                     |
+--------------+-----------+----------------------------------+
```

