# join  

## Description  

The `join` command combines two datasets together. The left side could be an index or results from a piped commands, the right side could be either an index or a subsearch.
## Syntax  


### Basic syntax:  

[joinType] join [leftAlias] [rightAlias] (on \| where) \<joinCriteria\> \<right-dataset\>
* joinType: optional. The type of join to perform. Options: `left`, `semi`, `anti`, and performance-sensitive types `right`, `full`, `cross`. **Default:** `inner`.  
* leftAlias: optional. The subsearch alias to use with the left join side, to avoid ambiguous naming. Pattern: `left = <leftAlias>`  
* rightAlias: optional. The subsearch alias to use with the right join side, to avoid ambiguous naming. Pattern: `right = <rightAlias>`  
* joinCriteria: mandatory. Any comparison expression. Must follow `on` or `where` keyword.  
* right-dataset: mandatory. Right dataset could be either an `index` or a `subsearch` with/without alias.  

### Extended syntax:  

join [type=<joinType>] [overwrite=<bool>] [max=n] (\<join-field-list\> \| [leftAlias] [rightAlias] (on \| where) \<joinCriteria\>) \<right-dataset\>
* type: optional. Join type using extended syntax. Options: `left`, `outer` (alias of `left`), `semi`, `anti`, and performance-sensitive types `right`, `full`, `cross`. **Default:** `inner`.  
* overwrite: optional boolean. Only works with `join-field-list`. Specifies whether duplicate-named fields from right-dataset should replace corresponding fields in the main search results. **Default:** `true`.  
* max: optional integer. Controls how many subsearch results could be joined against each row in main search. **Default:** 0 (unlimited) when plugins.ppl.syntax.legacy.preferred is `true`. When the setting is `false` the default value is `1`.
* join-field-list: optional. The fields used to build the join criteria. The join field list must exist on both sides. If not specified, all fields common to both sides will be used as join keys.  
* leftAlias: optional. Same as basic syntax when used with extended syntax.  
* rightAlias: optional. Same as basic syntax when used with extended syntax.  
* joinCriteria: mandatory. Same as basic syntax when used with extended syntax.  
* right-dataset: mandatory. Same as basic syntax.  
  
## Configuration  

### plugins.ppl.join.subsearch_maxout  

The size configures the maximum of rows from subsearch to join against. The default value is: `50000`. A value of `0` indicates that the restriction is unlimited.
Change the join.subsearch_maxout to 5000
  
```bash ignore
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"persistent" : {"plugins.ppl.join.subsearch_maxout" : "5000"}}'
```
  
```json
{
  "acknowledged": true,
  "persistent": {
    "plugins": {
      "ppl": {
        "join": {
          "subsearch_maxout": "5000"
        }
      }
    }
  },
  "transient": {}
}
```
  
## Usage  

Basic join syntax: 
  
```
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
  
Extended syntax with options:  
  
```
source = table1 | join type=outer left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | join type=left left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | join type=inner max=1 left = l right = r where l.a = r.a table2 | fields l.a, r.a, b, c
source = table1 | join a table2 | fields a, b, c
source = table1 | join a, b table2 | fields a, b, c
source = table1 | join type=outer a b table2 | fields a, b, c
source = table1 | join type=inner max=1 a, b table2 | fields a, b, c
source = table1 | join type=left overwrite=false max=0 a, b [source=table2 | rename d as b] | fields a, b, c  
```
  
## Example 1: Two indices join  

This example shows joining two indices using the basic join syntax.
  
```ppl
source = state_country
| inner join left=a right=b ON a.name = b.name occupation
| stats avg(salary) by span(age, 10) as age_span, b.country
```
  
Expected output:
  
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
  
## Example 2: Join with subsearch  

This example shows joining with a subsearch using the basic join syntax.
  
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
  
Expected output:
  
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
  
## Example 3: Join with field list  

This example shows joining using the extended syntax with field list.
  
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
  
Expected output:
  
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
  
## Example 4: Join with options  

This example shows joining using the extended syntax with additional options.
  
```ppl
source = state_country
| join type=inner overwrite=false max=1 name occupation
| stats avg(salary) by span(age, 10) as age_span, country
```
  
Expected output:
  
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

For basic syntax, if fields in the left outputs and right outputs have the same name. Typically, in the join criteria
`ON t1.id = t2.id`, the names `id` in output are ambiguous. To avoid ambiguous, the ambiguous
fields in output rename to `<alias>.id`, or else `<tableName>.id` if no alias existing.  

Assume table1 and table2 only contain field `id`, following PPL queries and their outputs are:
  
| Query | Output |
| --- | --- |
| source=table1 \| join left=t1 right=t2 on t1.id=t2.id table2 \| eval a = 1 | t1.id, t2.id, a |
| source=table1 \| join on table1.id=table2.id table2 \| eval a = 1 | table1.id, table2.id, a |
| source=table1 \| join on table1.id=t2.id table2 as t2 \| eval a = 1 | table1.id, t2.id, a |
| source=table1 \| join right=tt on table1.id=t2.id [ source=table2 as t2 \| eval b = id ] \| eval a = 1 | table1.id, tt.id, tt.b, a |  
  
For extended syntax (join with field list), when duplicate-named fields in output results are deduplicated, the fields in output determined by the value of 'overwrite' option.  
Join types `inner`, `left`, `outer` (alias of `left`), `semi` and `anti` are supported by default. `right`, `full`, `cross` are performance-sensitive join types which are disabled by default. Set config `plugins.calcite.all_join_types.allowed = true` to enable.