
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
| fields body, errmsg, detail
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------------------------------------------------------------------+----------------+-----------------------------------------------------+
| body                                                                     | errmsg         | detail                                              |
|--------------------------------------------------------------------------+----------------+-----------------------------------------------------|
| Payment failed: connection timeout to payment gateway after 30000ms      | Payment failed | connection timeout to payment gateway after 30000ms |
| NullPointerException in UserService.getProfile at line 142               |                |                                                     |
| HTTP POST /api/checkout 503 Service Unavailable - upstream connect error |                |                                                     |
+--------------------------------------------------------------------------+----------------+-----------------------------------------------------+
```
  

## Example 2: Extract IP addresses from authentication logs  

The following query extracts IP addresses from authentication-related log messages, useful for identifying suspicious login sources:
  
```ppl
source=otellogs
| where LIKE(body, '%from%')
| parse body '.+from (?<sourceip>[0-9.]+)'
| fields body, sourceip
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------------------------------------------------------------+--------------+
| body                                                                      | sourceip     |
|---------------------------------------------------------------------------+--------------|
| Slow query detected: SELECT * FROM inventory WHERE stock < 10 took 3200ms |              |
| User U300 authenticated via OAuth2 from 10.0.0.5                          | 10.0.0.5     |
| Failed to authenticate user U400: invalid credentials from 203.0.113.50   | 203.0.113.50 |
+---------------------------------------------------------------------------+--------------+
```
  

## Limitations

The `parse` command has the following limitations:

- Fields created by the `parse` command cannot be parsed again.
- Fields created by the `parse` command cannot be overridden by other commands.
- The source text field used by the `parse` command cannot be overridden.
- Fields created by the `parse` command cannot be filtered or sorted after they are used in the `stats` command.
- Fields created by the `parse` command do not appear in the final results unless the original source field is included in the `fields` command.

