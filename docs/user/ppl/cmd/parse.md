
# parse

The `parse` command extracts information from a text field using a regular expression and adds the extracted information to the search results. It uses Java regex patterns. For more information, see the [Java regular expression documentation](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

## The rex and parse commands compared

The `rex` and `parse` commands both extract information from text fields using Java regular expressions with named capture groups. To compare the capabilities of the `rex` and `parse` commands, see the [`rex` command documentation](rex.md).

## Syntax

The `parse` command has the following syntax:

```syntax
parse <field> <pattern>
```

## Parameters

The `parse` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The text field to parse. |
| `<pattern>` | Required | The regular expression pattern used to extract new fields from the specified text field. If a field with the same name already exists, its values are replaced. |

## Regular expression

The regular expression pattern is used to match the whole text field of each document based on the [Java regular expression syntax](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html). Each named capture group in the expression becomes a new `STRING` field.  

## Example 1: Extract error details from log messages  

The following query extracts the error summary and detail from error log messages. This is useful for categorizing errors during incident triage:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| parse body '(?<errmsg>[^:]+): (?<detail>.+)'
| fields errmsg, detail
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------------+----------------------------------------------------------+
| errmsg         | detail                                                   |
|----------------+----------------------------------------------------------|
| Payment failed | connection timeout to payment gateway after 30000ms      |
|                |                                                          |
| Out of memory  | Java heap space - shutting down pod payment-6f8d4b-ht7q3 |
+----------------+----------------------------------------------------------+
```
  

## Example 2: Extract IP addresses from log messages  

The following query extracts IP addresses from log messages for a specific service:
  
```ppl
source=otellogs
| where `resource.attributes.service.name` = 'frontend'
| parse body '.+from (?<sourceip>[0-9.]+)'
| fields body, sourceip
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------------------------------------------------------------+----------+
| body                                                               | sourceip |
|--------------------------------------------------------------------+----------|
| HTTP GET /api/products 200 45ms                                    |          |
| User U300 authenticated via OAuth2 from 10.0.0.5                   | 10.0.0.5 |
| Deployment frontend-v2.2.0 rolled out successfully to 3/3 replicas |          |
+--------------------------------------------------------------------+----------+
```
  

## Limitations

The `parse` command has the following limitations:

- Fields created by the `parse` command cannot be parsed again.
- Fields created by the `parse` command cannot be overridden by other commands.
- The source text field used by the `parse` command cannot be overridden.
- Fields created by the `parse` command cannot be filtered or sorted after they are used in the `stats` command.
- Fields created by the `parse` command do not appear in the final results unless the original source field is included in the `fields` command.
