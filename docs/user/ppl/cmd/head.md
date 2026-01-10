
# head

The `head` command returns the first N lines from a search result.

> **Note**: The `head` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/index/). It is only executed on the coordinating node.

## Syntax

The `head` command has the following syntax:

```syntax
head [<size>] [from <offset>]
```

## Parameters

The `head` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<size>` | Optional | The number of results to return. Must be an integer. Default is `10`. |
| `<offset>` | Optional | The number of results to skip (used with the `from` keyword). Must be an integer. Default is `0`. |
  

## Example 1: Retrieve the first set of results using the default size 

The following query returns the default number of search results (10):
  
```ppl
source=accounts
| fields firstname, age
| head
```
  
The query returns the following results:
  
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
  

## Example 2: Retrieve a specified number of results  

The following query returns the first 3 search results:
  
```ppl
source=accounts
| fields firstname, age
| head 3
```
  
The query returns the following results:
  
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
  

## Example 3: Retrieve the first N results after an offset M

The following query demonstrates how to retrieve the first 3 results starting with the second result from the `accounts` index:
  
```ppl
source=accounts
| fields firstname, age
| head 3 from 1
```
  
The query returns the following results:
  
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
  

