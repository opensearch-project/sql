
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

The following query shows basic field selection using the `table` command:
  
```ppl
source=accounts
| table firstname lastname age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+-----+
| firstname | lastname | age |
|-----------+----------+-----|
| Amber     | Duke     | 32  |
| Hattie    | Bond     | 36  |
| Nanette   | Bates    | 28  |
| Dale      | Adams    | 33  |
+-----------+----------+-----+
```
  

## Related documentation 

- [`fields`](fields.md) -- An alias command with identical functionality  