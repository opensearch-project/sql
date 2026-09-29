
# table

The `table` command is an alias for the [`fields`](fields.md) command and provides the same field selection capabilities. It allows you to keep or remove fields from the search results using enhanced syntax options.

## Syntax

The `table` command has the following syntax:

```syntax
table [+|-] <field-list>
```

## Parameters

The `table` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Required | A comma-delimited or space-delimited list of fields to keep or remove. Supports wildcard patterns. |
| `[+|-]` | Optional | Specifies the fields to keep or remove. If the plus sign (`+`) is used, only the fields specified in the field list are kept. If the minus sign (`-`) is used, all the fields specified in the field list are removed. Default is `+`. |

## Example: Basic table command usage  

The following query builds a quick incident summary table showing severity, service, and the log message for recent errors:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| sort - severityNumber, `resource.attributes.service.name`
| table severityText `resource.attributes.service.name` body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                         |
|--------------+----------------------------------+----------------------------------------------------------------------------------------------|
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142                               |
| ERROR        | checkout                         | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | frontend-proxy                   | [2024-02-01T09:20:00.456Z] "POST /api/checkout HTTP/1.1" 503 - 0 30000 checkout-8d4f7b-mk2p9 |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Related documentation 

- [`fields`](fields.md) -- An alias command with identical functionality  
