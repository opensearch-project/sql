# head  

## Description  

The `head` command returns the first N number of specified results after an optional offset in search order.
## Syntax  

head [\<size\>] [from \<offset\>]
* size: optional integer. Number of results to return. **Default:** 10  
* offset: optional integer after `from`. Number of results to skip. **Default:** 0  
  
## Example 1: Get first 10 results  

This example shows getting a maximum of 10 results from accounts index.
  
```ppl
source=accounts
| fields firstname, age
| head
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+-----+
| firstname | age |
|-----------+-----|
| Amber     | 32  |
| Hattie    | 36  |
| Nanette   | 28  |
| Dale      | 33  |
+-----------+-----+
```
  
## Example 2: Get first N results  

This example shows getting the first 3 results from accounts index.
  
```ppl
source=accounts
| fields firstname, age
| head 3
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+-----------+-----+
| firstname | age |
|-----------+-----|
| Amber     | 32  |
| Hattie    | 36  |
| Nanette   | 28  |
+-----------+-----+
```
  
## Example 3: Get first N results after offset M  

This example shows getting the first 3 results after offset 1 from accounts index.
  
```ppl
source=accounts
| fields firstname, age
| head 3 from 1
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+-----------+-----+
| firstname | age |
|-----------+-----|
| Hattie    | 36  |
| Nanette   | 28  |
| Dale      | 33  |
+-----------+-----+
```
  
## Limitations  

The `head` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.