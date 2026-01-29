
# eval

The `eval` command evaluates the specified expression and appends the result of the evaluation to the search results.

> **Note**: The `eval` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `eval` command has the following syntax:

```syntax
eval <field>=<expression> ["," <field>=<expression> ]...
```

## Parameters

The `eval` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The name of the field to create or update. If the field does not exist, a new field is added. If it already exists, its value is overwritten. |
| `<expression>` | Required | The expression to evaluate. |  
  

## Example 1: Create a new field  

The following query creates a new `doubleAge` field for each document:
  
```ppl
source=accounts
| eval doubleAge = age * 2
| fields age, doubleAge
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----+-----------+
| age | doubleAge |
|-----+-----------|
| 32  | 64        |
| 36  | 72        |
| 28  | 56        |
| 33  | 66        |
+-----+-----------+
```
  

## Example 2: Override an existing field  

The following query overrides the `age` field by adding `1` to its value:
  
```ppl
source=accounts
| eval age = age + 1
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----+
| age |
|-----|
| 33  |
| 37  |
| 29  |
| 34  |
+-----+
```
  

## Example 3: Create a new field using a field defined in eval

The following query creates a new field based on another field defined in the same `eval` expression. In this example, the new `ddAge` field is calculated by multiplying the `doubleAge` field by `2`. The `doubleAge` field itself is defined earlier in the `eval` command:
  
```ppl
source=accounts
| eval doubleAge = age * 2, ddAge = doubleAge * 2
| fields age, doubleAge, ddAge
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----+-----------+-------+
| age | doubleAge | ddAge |
|-----+-----------+-------|
| 32  | 64        | 128   |
| 36  | 72        | 144   |
| 28  | 56        | 112   |
| 33  | 66        | 132   |
+-----+-----------+-------+
```
  

## Example 4: String concatenation  

The following query uses the `+` operator for string concatenation. You can concatenate string literals and field values as follows:
  
```ppl
source=accounts 
| eval greeting = 'Hello ' + firstname 
| fields firstname, greeting
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+---------------+
| firstname | greeting      |
|-----------+---------------|
| Amber     | Hello Amber   |
| Hattie    | Hello Hattie  |
| Nanette   | Hello Nanette |
| Dale      | Hello Dale    |
+-----------+---------------+
```
  

## Example 5: Multiple string concatenation with type casting  

The following query performs multiple concatenation operations, including type casting from numeric values to strings:
  
```ppl
source=accounts | eval full_info = 'Name: ' + firstname + ', Age: ' + CAST(age AS STRING) | fields firstname, age, full_info
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+-----+------------------------+
| firstname | age | full_info              |
|-----------+-----+------------------------|
| Amber     | 32  | Name: Amber, Age: 32   |
| Hattie    | 36  | Name: Hattie, Age: 36  |
| Nanette   | 28  | Name: Nanette, Age: 28 |
| Dale      | 33  | Name: Dale, Age: 33    |
+-----------+-----+------------------------+
```
  

