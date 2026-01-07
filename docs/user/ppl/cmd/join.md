
# join

The `join` command combines two datasets. The left side can be an index or the results of piped commands, while the right side can be either an index or a subsearch.

## Syntax

The `join` command supports basic and extended syntax options.

### Basic syntax

```syntax
[joinType] join [left = <leftAlias>] [right = <rightAlias>] (on | where) <joinCriteria> <right-dataset>
```

> **Note**: When using aliases, `left` must appear before `right`.

The following are examples of the basic `join` command syntax:

```syntax
source = table1 | inner join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | inner join left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | left join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | right join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | full left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | cross join left = l right = r on 1=1 table2
source = table1 | left semi join left = l right = r on l.a = r.a table2
source = table1 | left anti join left = l right = r on l.a = r.a table2
source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ]
source = table1 | inner join on table1.a = table2.a table2 | fields table1.a, table2.a, table1.b, table1.c
source = table1 | inner join on a = c table2 | fields a, b, c, d
source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields l.a, r.a
source = table1 as t1 | join left = l right = r on l.a = r.a table2 as t2 | fields t1.a, t2.a
source = table1 | join left = l right = r on l.a = r.a [ source = table2 ] as s | fields l.a, s.a
```

#### Basic syntax parameters

The basic `join` syntax supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<joinCriteria>` | Required | A comparison expression specifying how to join the datasets. Must be placed after the `on` or `where` keyword in the query. |
| `<right-dataset>` | Required | The right dataset, which can be an index or a subsearch, with or without an alias. |
| `joinType` | Optional | The type of join to perform. Valid values are `left`, `semi`, `anti`, and performance-sensitive types (`right`, `full`, and `cross`). Default is `inner`. |
| `left` | Optional | An alias for the left dataset (typically a subsearch) used to avoid ambiguous field names. Specify as `left = <leftAlias>`. |
| `right` | Optional | An alias for the right dataset (typically, a subsearch) used to avoid ambiguous field names. Specify as `right = <rightAlias>`. |

### Extended syntax

```syntax
join [type=<joinType>] [overwrite=<bool>] [max=n] (<join-field-list> | [left = <leftAlias>] [right = <rightAlias>] (on | where) <joinCriteria>) <right-dataset>
```

The following are examples of the extended `join` command syntax:

```syntax
source = table1 | join type=outer left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | join type=left left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | join type=inner max=1 left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | join a table2 | fields a, b, c
source = table1 | join a, b table2 | fields a, b, c
source = table1 | join type=outer a b table2 | fields a, b, c
source = table1 | join type=inner max=1 a, b table2 | fields a, b, c
source = table1 | join type=left overwrite=false max=0 a, b [source=table2 | rename d as b] | fields a, b, c
```

#### Extended syntax parameters

The extended `join` syntax supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<joinCriteria>` | Required | A comparison expression specifying how to join the datasets. Must be placed after the `on` or `where` keyword in the query. |
| `<right-dataset>` | Required | The right dataset, which can be an index or a subsearch, with or without an alias. |  
| `type` | Optional | The join type when using extended syntax. Valid values are `left`, `outer` (same as `left`), `semi`, `anti`, and performance-sensitive types (`right`, `full`, and `cross`). Default is `inner`. |
| `<join-field-list>` | Optional | A list of fields used to build the join criteria. These fields must exist in both datasets. If not specified, all fields common to both datasets are used as join keys. |
| `overwrite` | Optional | Applicable only when `join-field-list` is specified. Specifies whether fields from the right dataset with duplicate names should replace corresponding fields in the main search results. Default is `true`. |
| `max` | Optional | The maximum number of subsearch results to join with each row in the main search. Default is `0` (unlimited). |
| `left` | Optional | An alias for the left dataset (typically a subsearch) used to avoid ambiguous field names. Specify as `left = <leftAlias>`. |
| `right` | Optional | An alias for the right dataset (typically, a subsearch) used to avoid ambiguous field names. Specify as `right = <rightAlias>`. |
  

## Configuration

The `join` command behavior is configured using the `plugins.ppl.join.subsearch_maxout` setting, which specifies the maximum number of rows from the subsearch to join against. Default is `50000`. A value of `0` indicates that the restriction is unlimited.

To update the setting, send the following request:
  
```bash ignore
PUT /_plugins/_query/settings
{
  "persistent": {
    "plugins.ppl.join.subsearch_maxout": "5000"
  }
}
```

## Example 1: Join two indexes  

The following query uses the basic `join` syntax to join two indexes:
  
```ppl
source = state_country
| inner join left=a right=b ON a.name = b.name occupation
| stats avg(salary) by span(age, 10) as age_span, b.country
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+-------------+----------+-----------+
| avg(salary) | age_span | b.country |
|-------------+----------+-----------|
| 120000.0    | 40       | USA       |
| 105000.0    | 20       | Canada    |
|  0.0        | 40       | Canada    |
| 70000.0     | 30       | USA       |
| 100000.0    | 70       | England   |
+-------------+----------+-----------+
```
  

## Example 2: Join with a subsearch  

The following query combines a dataset with a subsearch using the basic `join` syntax:
  
```ppl
source = state_country as a
| where country = 'USA' OR country = 'England'
| left join ON a.name = b.name [ source = occupation
| where salary > 0
| fields name, country, salary
| sort salary
| head 3 ] as b
| stats avg(salary) by span(age, 10) as age_span, b.country
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------------+----------+-----------+
| avg(salary) | age_span | b.country |
|-------------+----------+-----------|
| null        | 40       | null      |
| 70000.0     | 30       | USA       |
| 100000.0    | 70       | England   |
+-------------+----------+-----------+
```
  

## Example 3: Join using a field list  

The following query uses the extended syntax and specifies a list of fields for the join criteria:
  
```ppl
source = state_country
| where country = 'USA' OR country = 'England'
| join type=left overwrite=true name [ source = occupation
| where salary > 0
| fields name, country, salary
| sort salary
| head 3 ]
| stats avg(salary) by span(age, 10) as age_span, country
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------------+----------+---------+
| avg(salary) | age_span | country |
|-------------+----------+---------|
| null        | 40       | null    |
| 70000.0     | 30       | USA     |
| 100000.0    | 70       | England |
+-------------+----------+---------+
```
  

## Example 4: Join with additional options  

The following query uses the extended syntax and optional parameters for more control over the join operation:
  
```ppl
source = state_country
| join type=inner overwrite=false max=1 name occupation
| stats avg(salary) by span(age, 10) as age_span, country
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-------------+----------+---------+
| avg(salary) | age_span | country |
|-------------+----------+---------|
| 120000.0    | 40       | USA     |
| 100000.0    | 70       | USA     |
| 105000.0    | 20       | Canada  |
| 70000.0     | 30       | USA     |
+-------------+----------+---------+
```
  

## Limitations

The `join` command has the following limitations:

* **Field name ambiguity in basic syntax** – When fields from the left and right datasets share the same name, the field names in the output are ambiguous. To resolve this, conflicting fields are renamed to `<alias>.id` (or `<tableName>.id` if no alias is specified).

  The following table demonstrates how field name conflicts are resolved when both `table1` and `table2` contain a field named `id`.

  | Query | Output |
  | --- | --- |
  | `source=table1 | join left=t1 right=t2 on t1.id=t2.id table2 | eval a = 1` | `t1.id, t2.id, a` |
  | `source=table1 | join on table1.id=table2.id table2 | eval a = 1` | `table1.id, table2.id, a` |
  | `source=table1 | join on table1.id=t2.id table2 as t2 | eval a = 1` | `table1.id, t2.id, a` |
  | `source=table1 | join right=tt on table1.id=t2.id [ source=table2 as t2 | eval b = id ] | eval a = 1` | `table1.id, tt.id, tt.b, a` |

* **Field deduplication in extended syntax** – When using the extended syntax with a field list, duplicate field names in the output are deduplicated according to the `overwrite` option.

* **Join type availability** – The join types `inner`, `left`, `outer` (alias of `left`), `semi`, and `anti` are enabled by default. The performance-sensitive join types `right`, `full`, and `cross` are disabled by default. To enable these types, set `plugins.calcite.all_join_types.allowed` to `true`.
