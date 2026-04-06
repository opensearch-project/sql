
# fillnull

The `fillnull` command replaces `null` values in one or more fields of the search results with a specified value.

> **Note**: The `fillnull` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `fillnull` command has the following syntax:

```syntax
fillnull with <replacement> [in <field-list>]
fillnull using <field> = <replacement> [, <field> = <replacement>]
fillnull value=<replacement> [<field-list>]
```

The following syntax variations are available:

* `with <replacement> in <field-list>` -- Apply the same value to specified fields.
* `using <field>=<replacement>, ...` -- Apply different values to different fields.
* `value=<replacement> [<field-list>]` -- Alternative syntax with an optional space-delimited field list.

## Parameters

The `fillnull` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<replacement>` | Required | The value that replaces null values. |
| `<field>` | Required (with `using` syntax) | The name of the field to which a specific replacement value is applied. |
| `<field-list>` | Optional | A list of fields in which null values are replaced. You can specify the list as comma-delimited (using `with` or `using` syntax) or space-delimited (using `value=` syntax). By default, all fields are processed. |

## Example 1: Replace null values with different values per field

The following query fills in missing instrumentation scope names with a default value:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| fields severityText, `resource.attributes.service.name`, instrumentationScope.name
| fillnull using instrumentationScope.name = 'unknown'
| sort `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 11/11
+--------------+----------------------------------+-----------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | instrumentationScope.name                                                   |
|--------------+----------------------------------+-----------------------------------------------------------------------------|
| ERROR        | checkout                         | unknown                                                            |
| ERROR        | checkout                         | unknown                                                            |
| ERROR        | frontend-proxy                   | unknown                                                            |
| WARN         | frontend-proxy                   | unknown                                                            |
| WARN         | frontend-proxy                   | unknown                                                            |
| ERROR        | payment                          | @opentelemetry/instrumentation-http                                         |
| ERROR        | payment                          | unknown                                                            |
| WARN         | product-catalog                  | go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc |
| WARN         | product-catalog                  | unknown                                                            |
| ERROR        | product-catalog                  | unknown                                                            |
| ERROR        | recommendation                   | unknown                                                            |
+--------------+----------------------------------+-----------------------------------------------------------------------------+
```
  

## Example 2: Replace null values using value= syntax

The following query uses the `value=` syntax to fill null instrumentation scope names, helping identify uninstrumented services:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| fields severityText, `resource.attributes.service.name`, instrumentationScope.name
| fillnull value='unknown' instrumentationScope.name
| sort `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+--------------+----------------------------------+-------------------------------------+
| severityText | resource.attributes.service.name | instrumentationScope.name           |
|--------------+----------------------------------+-------------------------------------|
| ERROR        | checkout                         | unknown                    |
| ERROR        | checkout                         | unknown                    |
| ERROR        | frontend-proxy                   | unknown                    |
| ERROR        | payment                          | @opentelemetry/instrumentation-http |
| ERROR        | payment                          | unknown                    |
| ERROR        | product-catalog                  | unknown                    |
| ERROR        | recommendation                   | unknown                    |
+--------------+----------------------------------+-------------------------------------+
```
  
