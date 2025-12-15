# head


The `head` command returns the first N number of lines from a search result.

## Syntax

Use the following syntax:

`head [<size>] [from <offset>]`
* `size`: optional integer. The number of results you want to return. **Default:** 10  
* `offset`: optional integer after `from`. Number of results to skip. **Default:** 0  
  

## Example 1: Get the first 10 results 

The following example PPL query shows how to use `head` to return the first 10 search results:
  
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

The following example PPL query shows how to use `head` to get a specified number of search results. In this example, N is equal to 3: 
  
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
  

## Example 3: Get the first N results after offset M  

The following example PPL query example shows getting the first 3 results after offset 1 from the `accounts` index.
  
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

The `head` command is not rewritten to [query domain-specific language (DSL)](https://opensearch.org/docs/latest/query-dsl/index/). It is only run on the coordinating node.