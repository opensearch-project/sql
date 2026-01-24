
# fieldformat

The `fieldformat` command sets the value to a field with the specified expression and appends the field with evaluated result to the search results. The command is a alias of eval command.
Additionally, it also provides string concatenation dot operator followed by and/or follows a string that will be concatenated to the expression.


## Syntax

The `fieldformat` command has the following syntax:

```syntax
Following are possible syntaxes:
 fieldformat <field>=[(prefix).]<expression>[.(suffix)] ["," <field>=[(prefix).]<expression>[.(suffix)] ]...

```

## Parameters

The `fieldformat` command supports the following parameters.

| Parameter      | Required/Optional | Description                                                                                                                                   |
|----------------|  |-----------------------------------------------------------------------------------------------------------------------------------------------|
| `<field>`      | Required | The name of the field to create or update. If the field does not exist, a new field is added. If it already exists, its value is overwritten. |
| `<expression>` | Required | The expression to evaluate.  The expression can have a prefix and/or suffix string part that will be concatenated to the expression. |
| `prefix`       | Optional | A string before the expression followed by dot operator which will be concatenated as prefix to the evaluated expression value. |
| `suffix`       | Optional | A string that follows  the expression and dot operator which will be concatenated as suffix to the evaluated expression value. |
  

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
  

  

## Example 3: String concatenation with prefix 

The following query uses the `.` (dot) operator for string concatenation. You can concatenate string literals and field values as follows:
  
```ppl
source=accounts 
| fieldformat greeting = 'Hello '.tostring( firstname) 
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
  

## Example 5: string concatenation with dot operator , prefix and suffix

The following query performs prefix and suffix string concatenation operations using dot operator:

```ppl
source=accounts | fieldformat age_info =  'Age: '.CAST(age AS STRING).' years.' | fields firstname, age, age_info
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------+-----+----------------+
| firstname | age | age_info       |
|-----------+-----+----------------|
| Amber     | 32  | Age: 32 years. |
| Hattie    | 36  | Age: 36 years. |
| Nanette   | 28  | Age: 28 years. |
| Dale      | 33  | Age: 33 years. |
+-----------+-----+----------------+
  

