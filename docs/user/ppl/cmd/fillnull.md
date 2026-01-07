
# fillnull

The `fillnull` command replaces `null` values in one or more fields of the search results with a specified value.

The `fillnull` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.
{: .note}

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

## Example 1: Replace null values in a single field with a specified value

The following query replaces null values in the `email` field with `\<not found\>`:
  
```ppl
source=accounts
| fields email, employer
| fillnull with '<not found>' in email
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+----------+
| email                 | employer |
|-----------------------+----------|
| amberduke@pyrami.com  | Pyrami   |
| hattiebond@netagy.com | Netagy   |
| <not found>           | Quility  |
| daleadams@boink.com   | null     |
+-----------------------+----------+
```
  

## Example 2: Replace null values in multiple fields with a specified value  

The following query replaces null values in both the `email` and `employer` fields with `\<not found\>`:
  
```ppl
source=accounts
| fields email, employer
| fillnull with '<not found>' in email, employer
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+-------------+
| email                 | employer    |
|-----------------------+-------------|
| amberduke@pyrami.com  | Pyrami      |
| hattiebond@netagy.com | Netagy      |
| <not found>           | Quility     |
| daleadams@boink.com   | <not found> |
+-----------------------+-------------+
```
  

## Example 3: Replace null values in all fields with a specified value  

The following query replaces null values in all fields when no `field-list` is specified:
  
```ppl
source=accounts
| fields email, employer
| fillnull with '<not found>'
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+-------------+
| email                 | employer    |
|-----------------------+-------------|
| amberduke@pyrami.com  | Pyrami      |
| hattiebond@netagy.com | Netagy      |
| <not found>           | Quility     |
| daleadams@boink.com   | <not found> |
+-----------------------+-------------+
```
  

## Example 4: Replace null values in multiple fields with different specified values  

The following query shows how to use the `fillnull` command with different replacement values for multiple fields using the `using` syntax:
  
```ppl
source=accounts
| fields email, employer
| fillnull using email = '<not found>', employer = '<no employer>'
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+---------------+
| email                 | employer      |
|-----------------------+---------------|
| amberduke@pyrami.com  | Pyrami        |
| hattiebond@netagy.com | Netagy        |
| <not found>           | Quility       |
| daleadams@boink.com   | <no employer> |
+-----------------------+---------------+
```
  

## Example 5: Replace null values in specific fields using the value= syntax

The following query shows how to use the `fillnull` command with the `value=` syntax to replace null values in specific fields:
  
```ppl
source=accounts
| fields email, employer
| fillnull value="<not found>" email employer
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+-------------+
| email                 | employer    |
|-----------------------+-------------|
| amberduke@pyrami.com  | Pyrami      |
| hattiebond@netagy.com | Netagy      |
| <not found>           | Quility     |
| daleadams@boink.com   | <not found> |
+-----------------------+-------------+
```
  

## Example 6: Replace null values in all fields using the value= syntax

When no `field-list` is specified, the replacement applies to all fields in the result:
  
```ppl
source=accounts
| fields email, employer
| fillnull value='<not found>'
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+-------------+
| email                 | employer    |
|-----------------------+-------------|
| amberduke@pyrami.com  | Pyrami      |
| hattiebond@netagy.com | Netagy      |
| <not found>           | Quility     |
| daleadams@boink.com   | <not found> |
+-----------------------+-------------+
```
  

## Limitations

The `fillnull` command has the following limitations:

* When applying the same value to all fields without specifying field names, all fields must be of the same type. For mixed types, use separate `fillnull` commands or explicitly specify fields.
* The replacement value type must match all field types in the field list. When applying the same value to multiple fields, all fields must be of the same type (all strings or all numeric). The following query shows the error that occurs when this rule is violated:

    ```sql
      # This FAILS - same value for mixed-type fields
      source=accounts | fillnull value=0 firstname, age
      # ERROR: fillnull failed: replacement value type INTEGER is not compatible with field 'firstname' (type: VARCHAR). The replacement value type must match the field type.
    ```
  