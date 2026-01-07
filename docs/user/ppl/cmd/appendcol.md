
# appendcol

The `appendcol` command appends the result of a subsearch as additional columns to the input search results (the main search).

## Syntax

The `appendcol` command has the following syntax:

```syntax
appendcol [override=<boolean>] <subsearch>
```

## Parameters

The `appendcol` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<subsearch>` | Required | Executes PPL commands as a secondary search. The `subsearch` uses the data specified in the `source` clause of the main search results as its input. |
| `override` | Optional | Specifies whether the results of the main search should be overwritten when column names conflict. Default is `false`. |
  
  

## Example 1: Append a count aggregation to existing search results  

This example appends `count by gender` to `sum by gender, state`:
  
```ppl
source=accounts
| stats sum(age) by gender, state
| appendcol [ stats count(age) by gender ]
| head 10
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 10/10
+--------+-------+----------+------------+
| gender | state | sum(age) | count(age) |
|--------+-------+----------+------------|
| F      | AK    | 317      | 493        |
| F      | AL    | 397      | 507        |
| F      | AR    | 229      | NULL       |
| F      | AZ    | 238      | NULL       |
| F      | CA    | 282      | NULL       |
| F      | CO    | 217      | NULL       |
| F      | CT    | 147      | NULL       |
| F      | DC    | 358      | NULL       |
| F      | DE    | 101      | NULL       |
| F      | FL    | 310      | NULL       |
+--------+-------+----------+------------+
```
  

## Example 2: Append a count aggregation to existing search results, overriding the main search results  

This example appends `count by gender` to `sum by gender, state` and overrides the main search results:
  
```ppl
source=accounts
| stats sum(age) by gender, state
| appendcol override=true [ stats count(age) by gender ]
| head 10
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 10/10
+--------+-------+----------+------------+
| gender | state | sum(age) | count(age) |
|--------+-------+----------+------------|
| F      | AK    | 317      | 493        |
| M      | AL    | 397      | 507        |
| F      | AR    | 229      | NULL       |
| F      | AZ    | 238      | NULL       |
| F      | CA    | 282      | NULL       |
| F      | CO    | 217      | NULL       |
| F      | CT    | 147      | NULL       |
| F      | DC    | 358      | NULL       |
| F      | DE    | 101      | NULL       |
| F      | FL    | 310      | NULL       |
+--------+-------+----------+------------+
```
  

## Example 3: Append multiple subsearch results  

The following query chains multiple `appendcol` commands to add columns from different subsearches:
  
```ppl
source=employees
| fields name, dept, age
| appendcol [ stats avg(age) as avg_age ]
| appendcol [ stats max(age) as max_age ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 9/9
+------+-------------+-----+------------------+---------+
| name | dept        | age | avg_age          | max_age |
|------+-------------+-----+------------------+---------|
| Lisa | Sales       | 35  | 31.2222222222222 | 38      |
| Fred | Engineering | 28  | NULL             | NULL    |
| Paul | Engineering | 23  | NULL             | NULL    |
| Evan | Sales       | 38  | NULL             | NULL    |
| Chloe| Engineering | 25  | NULL             | NULL    |
| Tom  | Engineering | 33  | NULL             | NULL    |
| Alex | Sales       | 33  | NULL             | NULL    |
| Jane | Marketing   | 28  | NULL             | NULL    |
| Jeff | Marketing   | 38  | NULL             | NULL    |
+------+-------------+-----+------------------+---------+
```
  

## Example 4: Resolve column name conflicts using the override parameter

The following query shows how to use `appendcol` with the `override` option when column names in the main search and subsearch conflict:
  
```ppl
source=employees
| stats avg(age) as agg by dept
| appendcol override=true [ stats max(age) as agg by dept ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+-------------+
| agg | dept        |
|-----+-------------|
| 38  | Sales       |
| 38  | Engineering |
| 38  | Marketing   |
+-----+-------------+
```
  