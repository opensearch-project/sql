# show datasources


The `show datasources` command queries datasources configured in the PPL engine. The `show datasources` command can only be used as the first command in the PPL query.

## Syntax

Use the following syntax:

`show datasources`  

## Example 1: Fetch all PROMETHEUS datasources  

The following example PPL query shows how to use `showdatasources` to fetch all the datasources of type prometheus.
PPL query for all PROMETHEUS DATASOURCES
  
```ppl
show datasources
| where CONNECTOR_TYPE='PROMETHEUS'
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------+----------------+
| DATASOURCE_NAME | CONNECTOR_TYPE |
|-----------------+----------------|
| my_prometheus   | PROMETHEUS     |
+-----------------+----------------+
```
  

## Limitations  

The `show datasources` command can only work with `plugins.calcite.enabled=false`.