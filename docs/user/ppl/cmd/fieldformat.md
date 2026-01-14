
# fieldformat

The `fieldformat` command fieldformatuates the specified expression and appends the result of the fieldformatuation to the search results.

> **Note**: The `fieldformat` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `fieldformat` command has the following syntax:

```syntax
fieldformat <field>=<expression> ["," <field>=<expression> ]...
```

## Parameters

The `fieldformat` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The name of the field to create or update. If the field does not exist, a new field is added. If it already exists, its value is overwritten. |
| `<expression>` | Required | The expression to fieldformatuate. |  
  

## Example 1: Create a new field  

The following query creates a new `doubleAge` field for each document:
  
```ppl
source=accounts
| fieldformat doubleAge = age * 2
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
| fieldformat age = age + 1
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
  

## Example 3: Create a new field using a field defined in fieldformat

The following query creates a new field based on another field defined in the same `fieldformat` expression. In this example, the new `ddAge` field is calculated by multiplying the `doubleAge` field by `2`. The `doubleAge` field itself is defined earlier in the `fieldformat` command:
  
```ppl
source=accounts
| fieldformat doubleAge = age * 2, ddAge = doubleAge * 2
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
| fieldformat greeting = 'Hello ' + firstname 
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
source=accounts | fieldformat full_info = 'Name: ' + firstname + ', Age: ' + CAST(age AS STRING) | fields firstname, age, full_info
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
  

