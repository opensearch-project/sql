# eval


The `eval` command evaluates the expression and appends the result to the search result.

## Syntax

Use the following syntax:

`eval <field>=<expression> ["," <field>=<expression> ]...`
* `field`: mandatory. If the field name does not exist, a new field is added. If the field name already exists, it will be overridden.  
* expression: mandatory. Any expression supported by the system.  
  

## Example 1: Create a new field  

The following example PPL query shows how to use `eval` to create a new field for each document. In this example, the new field is `doubleAge`.
  
```ppl
source=accounts
| eval doubleAge = age * 2
| fields age, doubleAge
```
  
Expected output:
  
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

The following example PPL query shows how to use `eval` to override an existing field. In this example, the existing field `age` is overridden by the `age` field plus 1.
  
```ppl
source=accounts
| eval age = age + 1
| fields age
```
  
Expected output:
  
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
  

## Example 3: Create a new field with field defined in eval  

The following example PPL query shows how to use `eval` to create a new field based on the fields defined in the `eval` expression. In this example, the new field `ddAge` is the evaluation result of the `doubleAge` field multiplied by 2. `doubleAge` is defined in the `eval` command.
  
```ppl
source=accounts
| eval doubleAge = age * 2, ddAge = doubleAge * 2
| fields age, doubleAge, ddAge
```
  
Expected output:
  
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

The following example PPL query shows using the `+` operator for string concatenation. You can concatenate string literals and field values.
  
```ppl
source=accounts 
| eval greeting = 'Hello ' + firstname 
| fields firstname, greeting
```
  
Expected output:
  
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

The following example PPL query shows multiple concatenations with type casting from numeric to string.
  
```ppl
source=accounts | eval full_info = 'Name: ' + firstname + ', Age: ' + CAST(age AS STRING) | fields firstname, age, full_info
```
  
Expected output:
  
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
  

## Limitations  

The `eval` command is not rewritten to [query domain-specific language (DSL)](https://opensearch.org/docs/latest/query-dsl/index/). It is only run on the coordinating node.