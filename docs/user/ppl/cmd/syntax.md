
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

### Optional choices

Optional choices between alternatives are shown in square brackets with pipe separators (`[option1 | option2]`). You can choose one of the options or omit them entirely.

**Example**: `[asc | desc]` means you can specify `asc`, `desc`, or neither.

### Repetition

An ellipsis (`...`) indicates that the preceding element can be repeated multiple times.

**Examples**:
- `<field>...` means one or more fields without commas: `field1 field2 field3`
- `<field>, ...` means comma-separated repetition: `field1, field2, field3`
  

## Examples

**Example 1: Search through an index**

In the following query, the `search` command refers to the `otellogs` index as the source and uses the `fields` and `where` commands for the conditions:

```ppl
search source=otellogs
| where severityText = 'ERROR'
| fields severityText, `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | payment                          |
| ERROR        | checkout                         |
| ERROR        | payment                          |
| ERROR        | frontend-proxy                   |
| ERROR        | recommendation                   |
| ERROR        | product-catalog                  |
| ERROR        | checkout                         |
+--------------+----------------------------------+
```

**Example 2: Get all documents**

To get all documents from the `otellogs` index, specify it as the `source`. The following example limits the output to 5 rows using `head`:

```ppl
source=otellogs
| head 5
| fields severityText, `resource.attributes.service.name`, body
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------------------------+-----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                          |
|--------------+----------------------------------+-----------------------------------------------------------------------------------------------|
| INFO         | frontend                         | [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 |
| INFO         | cart                             | Order #1234 placed successfully by user U100                                                  |
| WARN         | product-catalog                  | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        |
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms                           |
| DEBUG        | cart                             | Cache miss for key user:session:U200 in Valkey cluster                                        |
+--------------+----------------------------------+-----------------------------------------------------------------------------------------------+
```

**Example 3: Get documents that match a condition**

To get all documents from the `otellogs` index that have `severityText` equal to `ERROR` and `resource.attributes.service.name` equal to `payment`, use the following query:

```ppl
source=otellogs severityText = 'ERROR' AND `resource.attributes.service.name` = 'payment'
| fields severityText, `resource.attributes.service.name`, body
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------+----------------------------------+-------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                    |
|--------------+----------------------------------+-------------------------------------------------------------------------|
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms     |
| ERROR        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3 |
+--------------+----------------------------------+-------------------------------------------------------------------------+
```

