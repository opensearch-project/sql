# dedup


The `dedup` command removes duplicate documents defined by specified fields from the search result.

## Syntax

Use the following syntax:

`dedup [int] <field-list> [keepempty=<bool>] [consecutive=<bool>]`
* `int`: optional. The `dedup` command retains multiple events for each combination when you specify `<int>`. The number for `<int>` must be greater than 0. All other duplicates are removed from the results. **Default:** 1  
* `keepempty`: optional. If set to true, keep the document if any field in the field-list has NULL value or field is MISSING. **Default:** false.  
* `consecutive`: optional. If set to true, removes only events with duplicate combinations of values that are consecutive. **Default:** false.  
* `field-list`: mandatory. The comma-delimited field list. At least one field is required.  
  

## Example 1: Dedup by one field  

The following example PPL query shows how to use `dedup` to remove duplicate documents based on the `gender` field:
  
```ppl
source=accounts
| dedup gender
| fields account_number, gender
| sort account_number
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 1              | M      |
| 13             | F      |
+----------------+--------+
```
  

## Example 2: Keep two duplicates documents  

The following example PPL query shows how to use `dedup` to remove duplicate documents based on the `gender` field while keeping two duplicates:
  
```ppl
source=accounts
| dedup 2 gender
| fields account_number, gender
| sort account_number
```
  
Expected output:
  
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
  

## Example 3: Keep or ignore empty fields by default  

The following example PPL query shows how to use `dedup` to remove duplicate documents while keeping documents with null values in the specified field:
  
```ppl
source=accounts
| dedup email keepempty=true
| fields account_number, email
| sort account_number
```
  
Expected output:
  
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
  
The following example PPL query shows how to use `dedup` to remove duplicate documents while ignoring documents with empty values in the specified field:
  
```ppl
source=accounts
| dedup email
| fields account_number, email
| sort account_number
```
  
Expected output:
  
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
  

## Example 4: Dedup in consecutive document  

The following example PPL query shows how to use `dedup` to remove duplicate consecutive documents:
  
```ppl
source=accounts
| dedup gender consecutive=true
| fields account_number, gender
| sort account_number
```
  
Expected output:
  
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
  

## Limitations  

The `dedup` with `consecutive=true` command can only work with `plugins.calcite.enabled=false`.