
# show datasources

The `show datasources` command queries data sources configured in the PPL engine. The `show datasources` command can only be used as the first command in a PPL query.

To use the `show datasources` command, `plugins.calcite.enabled` must be set to `false`.
{: .note}

## Syntax

The `show datasources` command has the following syntax:

```syntax
show datasources
```

The `show datasources` command takes no parameters.  

## Example 1: Fetch all Prometheus data sources

The following query fetches all Prometheus data sources:
  
```ppl
show datasources
| where CONNECTOR_TYPE='PROMETHEUS'
```
  
The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------------+----------------+
| DATASOURCE_NAME | CONNECTOR_TYPE |
|-----------------+----------------|
| my_prometheus   | PROMETHEUS     |
+-----------------+----------------+
```

