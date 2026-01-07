
# dedup

The `dedup` command removes duplicate documents defined by specified fields from the search result.

## Syntax

The `dedup` command has the following syntax:

```syntax
dedup [int] <field-list> [keepempty=<bool>] [consecutive=<bool>]
```

## Parameters

The `dedup` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Required | A comma-delimited list of fields to use for deduplication. At least one field is required. |
| `<int>` | Optional | The number of duplicate documents to retain for each combination. Must be greater than `0`. Default is `1`. |
| `keepempty` | Optional | When set to `true`, keeps documents in which any field in the field list has a `NULL` value or is missing. Default is `false`. |
| `consecutive` | Optional | When set to `true`, removes only consecutive duplicate documents. Default is `false`. Requires the legacy SQL engine (`plugins.calcite.enabled=false`). |
  

## Example 1: Remove duplicates based on a single field  

The following query deduplicates documents based on the `gender` field:
  
```ppl
source=accounts
| dedup gender
| fields account_number, gender
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 1              | M      |
| 13             | F      |
+----------------+--------+
```
  

## Example 2: Retain multiple duplicate documents  

The following query removes duplicate documents based on the `gender` field while keeping two duplicate documents:
  
```ppl
source=accounts
| dedup 2 gender
| fields account_number, gender
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 1              | M      |
| 6              | M      |
| 13             | F      |
+----------------+--------+
```
  

## Example 3: Handle documents with empty field values  

The following query removes duplicate documents while keeping documents with `null` values in the specified field:
  
```ppl
source=accounts
| dedup email keepempty=true
| fields account_number, email
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----------------------+
| account_number | email                 |
|----------------+-----------------------|
| 1              | amberduke@pyrami.com  |
| 6              | hattiebond@netagy.com |
| 13             | null                  |
| 18             | daleadams@boink.com   |
+----------------+-----------------------+
```
  
The following query removes duplicate documents while ignoring documents with empty values in the specified field:
  
```ppl
source=accounts
| dedup email
| fields account_number, email
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------------+-----------------------+
| account_number | email                 |
|----------------+-----------------------|
| 1              | amberduke@pyrami.com  |
| 6              | hattiebond@netagy.com |
| 18             | daleadams@boink.com   |
+----------------+-----------------------+
```
  

## Example 4: Deduplicate consecutive documents  

The following query removes duplicate consecutive documents:
  
```ppl
source=accounts
| dedup gender consecutive=true
| fields account_number, gender
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 1              | M      |
| 13             | F      |
| 18             | M      |
+----------------+--------+
```
  

