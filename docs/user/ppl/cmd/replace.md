
# replace

The `replace` command replaces field values with specified values. It searches for exact matches and replaces them.

## Syntax

The `replace` command has the following syntax:

```syntax
replace <value> WITH <new-value> IN <field> [, <value> WITH <new-value> IN <field>]...
```

## Parameters

The `replace` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<value>` | Required | The value to search for. |
| `<new-value>` | Required | The replacement value. |
| `<field>` | Required | The field in which to perform the replacement. |

## Example 1: Normalize service names for a compact dashboard

The following query shortens long service names for a compact dashboard view:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| replace 'payment-service' with 'payment' in `resource.attributes.service.name`
| replace 'inventory-service' with 'inventory' in `resource.attributes.service.name`
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| WARN         | frontend-proxy                   |
| WARN         | frontend-proxy                   |
| WARN         | product-catalog                  |
| WARN         | product-catalog                  |
+--------------+----------------------------------+
```

## Example 2: Replace service names for display

The following query shortens service names for a compact dashboard view:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| replace 'payment-service' with 'payment' in `resource.attributes.service.name`
| replace 'inventory-service' with 'inventory' in `resource.attributes.service.name`
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| WARN         | frontend-proxy                   |
| WARN         | frontend-proxy                   |
| WARN         | product-catalog                  |
| WARN         | product-catalog                  |
+--------------+----------------------------------+
```
