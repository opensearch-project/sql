
# PPL syntax

Every PPL query starts with the `search` command. It specifies the index to search and retrieve documents from.

`PPL` supports exactly one `search` command per PPL query, and it is always the first command. The word `search` can be omitted.

Subsequent commands can follow in any order.


## Syntax

```sql
search source=<index> [boolean-expression]
source=<index> [boolean-expression]
```

## Parameters

The `search` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<index>` | Optional | Specifies the index to query. |
| `<boolean-expression>` | Optional | Specifies an expression that evaluates to a Boolean value. |


## Syntax notation conventions

PPL command syntax uses the following notation conventions.

### Placeholders

Placeholders are shown in angle brackets (`< >`). These must be replaced with actual values.

**Example**: `<field>` means you must specify an actual field name like `age` or `firstname`.

### Optional elements

Optional elements are enclosed in square brackets (`[ ]`). These can be omitted from the command.

**Examples**:
- `[+|-]` means the plus or minus signs are optional.
- `[<alias>]` means the alias placeholder is optional.

### Required choices

Required choices between alternatives are shown in parentheses and are delimited with pipe separators (`(option1 | option2)`). You must choose exactly one of the specified options.

**Example**: `(on | where)` means you must use either `on` or `where`, but not both.

## Example 1: Fetch data from an index with a filter

The following query retrieves all error logs from the `otellogs` index. The filter is specified inline with the `source` command:

```ppl
source=otellogs severityText = 'ERROR'
| fields severityText, `resource.attributes.service.name`
| sort `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | api-gateway                      |
| ERROR        | auth-service                     |
| ERROR        | cart-service                     |
| ERROR        | payment-service                  |
| ERROR        | user-service                     |
+--------------+----------------------------------+
```

## Example 2: Fetch data with the search keyword

The following query uses the explicit `search` keyword, which is equivalent to omitting it:

```ppl
search source=otellogs
| where severityText = 'FATAL'
| fields severityText, body
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------+---------------------------------------------------------------------------------+
| severityText | body                                                                            |
|--------------+---------------------------------------------------------------------------------|
| FATAL        | Out of memory: Java heap space - shutting down pod payment-service-7d4b8c-xk2q9 |
| FATAL        | Database primary node unreachable: connection refused to db-primary-01:5432     |
+--------------+---------------------------------------------------------------------------------+
```

## Example 3: Pipe multiple commands

The following query demonstrates chaining multiple commands to filter, aggregate, and sort results:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| stats count() as error_count by `resource.attributes.service.name`
| sort - error_count
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-------------+----------------------------------+
| error_count | resource.attributes.service.name |
|-------------+----------------------------------|
| 2           | payment-service                  |
| 1           | api-gateway                      |
| 1           | auth-service                     |
+-------------+----------------------------------+
```
