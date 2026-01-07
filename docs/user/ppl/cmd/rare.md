
# rare

The `rare` command identifies the least common combination of values across all fields specified in the field list.

> **Note**: The command returns up to 10 results for each distinct combination of values in the group-by fields.

> **Note**: The `rare` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `rare` command has the following syntax:

```syntax
rare [rare-options] <field-list> [by-clause]
```

## Parameters

The `rare` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Required | A comma-delimited list of field names. |
| `<by-clause>` | Optional | One or more fields to group the results by. |
| `rare-options` | Optional | Additional options for controlling output: <br> - `showcount`: Whether to create a field in the output containing the frequency count for each combination of values. Default is `true`. <br> - `countfield`: The name of the field that contains the count. Default is `count`. <br> - `usenull`: Whether to output null values. Default is the value of `plugins.ppl.syntax.legacy.preferred`. |  
  

## Example 1: Find the least common values without showing counts

The following query uses the `rare` command with `showcount=false` to find the least common gender without displaying frequency counts:
  
```ppl
source=accounts
| rare showcount=false gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------+
| gender |
|--------|
| F      |
| M      |
+--------+
```
  

## Example 2: Find the least common values grouped by field

The following query uses the `rare` command with a `by` clause to find the least common age values grouped by gender:
  
```ppl
source=accounts
| rare showcount=false age by gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------+-----+
| gender | age |
|--------+-----|
| F      | 28  |
| M      | 32  |
| M      | 33  |
| M      | 36  |
+--------+-----+
```
  

## Example 3: Find the least common values with frequency counts

The following query uses the `rare` command with default settings to find the least common gender values and display their frequency counts:
  
```ppl
source=accounts
| rare gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------+-------+
| gender | count |
|--------+-------|
| F      | 1     |
| M      | 3     |
+--------+-------+
```
  

## Example 4: Customize the count field name

The following query uses the `rare` command with the `countfield` parameter to specify a custom name for the frequency count field:
  
```ppl
source=accounts
| rare countfield='cnt' gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------+-----+
| gender | cnt |
|--------+-----|
| F      | 1   |
| M      | 3   |
+--------+-----+
```
  

## Example 5: Specify null value handling

The following query uses the `rare` command with `usenull=false` to exclude null values from the results:

```ppl
source=accounts
| rare usenull=false email
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----------------------+-------+
| email                 | count |
|-----------------------+-------|
| amberduke@pyrami.com  | 1     |
| daleadams@boink.com   | 1     |
| hattiebond@netagy.com | 1     |
+-----------------------+-------+
```

The following query uses `usenull=true` to include null values in the results:

```ppl
source=accounts
| rare usenull=true email
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+-------+
| email                 | count |
|-----------------------+-------|
| null                  | 1     |
| amberduke@pyrami.com  | 1     |
| daleadams@boink.com   | 1     |
| hattiebond@netagy.com | 1     |
+-----------------------+-------+
```
  

