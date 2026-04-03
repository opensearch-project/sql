
# fieldformat

The `fieldformat` command sets the value to a field with the specified expression and appends the field with evaluated result to the search results. The command is an alias of eval command.
Additionally, it also provides string concatenation dot operator followed by and/or follows a string that will be concatenated to the expression.


## Syntax

The `fieldformat` command has the following syntax:

```syntax
 fieldformat <field>=[(prefix).]<expression>[.(suffix)] ["," <field>=[(prefix).]<expression>[.(suffix)] ]...

```

## Parameters

The `fieldformat` command supports the following parameters.

| Parameter| Required/Optional | Description                                                                                                                                   |
|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `<field>`      | Required          | The name of the field to create or update. If the field does not exist, a new field is added. If it already exists, its value is overwritten. |
| `<expression>` | Required          | The expression to evaluate.  The expression can have a prefix and/or suffix string part that will be concatenated to the expression.          |
| `prefix`       | Optional          | A string before the expression followed by dot operator which will be concatenated as prefix to the evaluated expression value.               |
| `suffix`       | Optional          | A string that follows  the expression and dot operator which will be concatenated as suffix to the evaluated expression value.                |
  

## Example 1: Create a computed field for incident classification  

The following query creates an `is_critical` field that flags whether a log entry represents a critical issue, useful for filtering in dashboards:
  
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
  

## Example 2: Override a field with a formatted value  

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
  
