
# fieldformat

The `fieldformat` command sets a field to the result of a specified expression and appends the evaluated field to the search results. This command is an alias of [`eval`](./eval.md/).

It also supports string concatenation using the dot (`.`) operator, allowing you to append strings to expressions.


## Syntax

The `fieldformat` command has the following syntax:

```syntax
 fieldformat <field>=[(prefix).]<expression>[.(suffix)] ["," <field>=[(prefix).]<expression>[.(suffix)] ]...

```

## Parameters

The `fieldformat` command supports the following parameters.

| Parameter| Required/Optional | Description                                                                                                                                   |
|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `<field>`      | Required | The name of the field to create or update. If the field does not exist, it is added. If it already exists, its value is overwritten. |
| `<expression>` | Required | The expression to evaluate. It may include optional prefix and/or suffix strings that are concatenated using the dot (`.`) operator. |
| `prefix`       | Optional | A string placed before the expression. When combined using the dot (`.`) operator, it is concatenated as a prefix to the evaluated result. |
| `suffix`       | Optional | A string placed after the expression. When combined using the dot (`.`) operator, it is concatenated as a suffix to the evaluated result. | 

## Example 1: Creating a computed field for incident classification  

The following query creates an `is_critical` field that indicates whether a log entry represents a critical issue, useful for filtering in dashboards:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| fieldformat is_critical = IF(severityNumber >= 21, 'CRITICAL', 'ERROR')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, is_critical
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+-------------+
| severityText | resource.attributes.service.name | is_critical |
|--------------+----------------------------------+-------------|
| WARN         | frontend-proxy                   | ERROR       |
| WARN         | frontend-proxy                   | ERROR       |
| WARN         | product-catalog                  | ERROR       |
| WARN         | product-catalog                  | ERROR       |
+--------------+----------------------------------+-------------+
```
  

## Example 2: Overriding a field with a formatted value  

The following query overrides the `severityNumber` field with a human-readable severity tier:
  
```ppl
source=otellogs
| dedup severityText
| sort severityNumber
| fieldformat severityNumber = CASE(severityNumber < 9, 'low', severityNumber < 17, 'medium', severityNumber >= 17, 'high')
| fields severityText, severityNumber
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| DEBUG        | low            |
| INFO         | medium         |
| WARN         | medium         |
| ERROR        | high           |
+--------------+----------------+
```
  
## Related commands

- [`eval`](./eval.md)