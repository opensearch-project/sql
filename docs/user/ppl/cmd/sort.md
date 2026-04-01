
# sort

The `sort` command sorts the search results by the specified fields.

## Syntax

The `sort` command supports two syntax notations. You must use one notation consistently within a single `sort` command.

### Prefix notation

The `sort` command has the following syntax in prefix notation:

```syntax
sort [<count>] [+|-] <field> [, [+|-] <field>]...
```

### Suffix notation

The `sort` command has the following syntax in suffix notation:

```syntax
sort [<count>] <field> [asc|desc|a|d] [, <field> [asc|desc|a|d]]...
```

## Parameters

The `sort` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field used to sort. Use `auto(field)`, `str(field)`, `ip(field)`, or `num(field)` to specify how to interpret field values. Multiple fields can be specified as a comma-separated list. |
| `<count>` | Optional | The number of results to return. A value of `0` or less returns all results. Default is `0`. |
| `[+|-]` | Optional | **Prefix notation only.** The plus sign (`+`) specifies ascending order, and the minus sign (`-`) specifies descending order. Default is ascending order. |
| `[asc|desc|a|d]` | Optional | **Suffix notation only.** Specifies the sort order: `asc`/`a` for ascending, `desc`/`d` for descending. Default is ascending order. |

## Example 1: Sort by one field

The following query sorts logs by severity number in ascending order, showing the least severe entries first:

```ppl
source=otellogs
| sort severityNumber
| fields severityText, severityNumber, `resource.attributes.service.name`
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------+----------------------------------+
| severityText | severityNumber | resource.attributes.service.name |
|--------------+----------------+----------------------------------|
| DEBUG        | 5              | auth-service                     |
| DEBUG        | 5              | inventory-service                |
| DEBUG        | 5              | auth-service                     |
| INFO         | 9              | frontend                         |
+--------------+----------------+----------------------------------+
```


## Example 2: Sort by one field in descending order

The following query sorts logs by severity in descending order to surface the most critical issues first. You can use either prefix notation (`- severityNumber`) or suffix notation (`severityNumber desc`):

```ppl
source=otellogs
| dedup severityText
| sort - severityNumber
| fields severityText, severityNumber
```

This query is equivalent to the following query:

```ppl
source=otellogs
| dedup severityText
| sort severityNumber desc
| fields severityText, severityNumber
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| FATAL        | 21             |
| ERROR        | 17             |
| WARN         | 13             |
| INFO         | 9              |
| DEBUG        | 5              |
+--------------+----------------+
```


## Example 3: Sort by multiple fields in prefix notation

The following query uses prefix notation to sort by severity ascending and service name descending, useful for grouping by severity while controlling the order within each group:
  
```ppl
source=otellogs
| dedup severityText, `resource.attributes.service.name`
| sort + severityNumber, - `resource.attributes.service.name`
| fields severityText, severityNumber, `resource.attributes.service.name`
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------+----------------------------------+
| severityText | severityNumber | resource.attributes.service.name |
|--------------+----------------+----------------------------------|
| DEBUG        | 5              | inventory-service                |
| DEBUG        | 5              | auth-service                     |
| INFO         | 9              | k8s-controller                   |
| INFO         | 9              | frontend                         |
| INFO         | 9              | cart-service                     |
+--------------+----------------+----------------------------------+
```
  

## Example 4: Sort by multiple fields in suffix notation

The following query uses suffix notation to achieve the same result as Example 3:
  
```ppl
source=otellogs
| dedup severityText, `resource.attributes.service.name`
| sort severityNumber asc, `resource.attributes.service.name` desc
| fields severityText, severityNumber, `resource.attributes.service.name`
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------+----------------------------------+
| severityText | severityNumber | resource.attributes.service.name |
|--------------+----------------+----------------------------------|
| DEBUG        | 5              | inventory-service                |
| DEBUG        | 5              | auth-service                     |
| INFO         | 9              | k8s-controller                   |
| INFO         | 9              | frontend                         |
| INFO         | 9              | cart-service                     |
+--------------+----------------+----------------------------------+
```
  

## Example 5: Sort fields with null values

The default ascending order lists null values first. The following query sorts by the `instrumentationScope.name` field, showing that logs without instrumentation metadata appear before instrumented ones:
  
```ppl
source=otellogs
| sort instrumentationScope.name
| fields instrumentationScope.name, severityText
| head 6
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+---------------------------+--------------+
| instrumentationScope.name | severityText |
|---------------------------+--------------|
| null                      | DEBUG        |
| null                      | ERROR        |
| null                      | INFO         |
| null                      | FATAL        |
| null                      | WARN         |
| null                      | INFO         |
+---------------------------+--------------+
```
  

## Example 6: Specify the number of sorted documents to return  

The following query sorts all logs by severity and returns only the 3 least severe entries:
  
```ppl
source=otellogs
| sort 3 severityNumber
| fields severityText, severityNumber
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| DEBUG        | 5              |
| DEBUG        | 5              |
| DEBUG        | 5              |
+--------------+----------------+
```
  

## Example 7: Sort by specifying field type

The following query uses `str()` to sort severity numbers lexicographically instead of numerically. Notice that `5` and `9` appear after `21` because string sorting compares character by character:

```ppl
source=otellogs
| dedup severityText
| sort str(severityNumber)
| fields severityText, severityNumber
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| WARN         | 13             |
| ERROR        | 17             |
| FATAL        | 21             |
| DEBUG        | 5              |
| INFO         | 9              |
+--------------+----------------+
```
  
