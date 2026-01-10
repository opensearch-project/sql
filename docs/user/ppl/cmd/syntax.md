
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

**Example 1: Search through accounts index**

In the following query, the `search` command refers to an `accounts` index as the source and uses the `fields` and `where` commands for the conditions:

```ppl ignore
search source=accounts
| where age > 18
| fields firstname, lastname
```

**Example 2: Get all documents**

To get all documents from the `accounts` index, specify it as the `source`:

```ppl ignore
search source=accounts;
```


| account_number | firstname | address | balance | gender | city | employer | state | age | email | lastname |
:--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :---
| 1  | Amber  | 880 Holmes Lane | 39225 | M | Brogan | Pyrami | IL | 32 | amberduke@pyrami.com  | Duke
| 6  | Hattie | 671 Bristol Street | 5686 | M | Dante | Netagy | TN | 36  | hattiebond@netagy.com | Bond
| 13 | Nanette | 789 Madison Street | 32838 | F | Nogal | Quility | VA | 28 | null | Bates
| 18 | Dale  | 467 Hutchinson Court | 4180 | M | Orick | null | MD | 33 | daleadams@boink.com | Adams

**Example 3: Get documents that match a condition**

To get all documents from the `accounts` index that either have `account_number` equal to 1 or have `gender` as `F`, use the following query:

```ppl ignore
search source=accounts account_number=1 or gender=\"F\";
```

| account_number | firstname | address | balance | gender | city | employer | state | age | email | lastname |
:--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :---
| 1 | Amber | 880 Holmes Lane | 39225 | M | Brogan | Pyrami | IL | 32 | amberduke@pyrami.com | Duke |
| 13 | Nanette | 789 Madison Street | 32838 | F | Nogal | Quility | VA | 28 | null | Bates |
