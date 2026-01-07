
# top {#top-command}

The `top` command finds the most common combination of values across all fields specified in the field list.

The `top` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.
{: .note}

## Syntax

The `top` command has the following syntax:

```syntax
top [N] [top-options] <field-list> [by-clause]
```

## Parameters

The `top` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<N>` | Optional | The number of results to return. Default is `10`. |
| `top-options` | Optional | `showcount`: Whether to create a field in the output that represents a count of the tuple of values. Default is `true`.<br>`countfield`: The name of the field that contains the count. Default is `count`.<br>`usenull`: Whether to output `null` values. Default is the value of `plugins.ppl.syntax.legacy.preferred`. |
| `<field-list>` | Required | A comma-delimited list of field names.  |
| `<by-clause>` | Optional | One or more fields to group the results by. |

## Example 1: Display counts in the default count column

The following query finds the most common gender values:

```ppl
source=accounts
| top gender
```

By default, the `top` command automatically includes a `count` column showing the frequency of each value:

```text
fetched rows / total rows = 2/2
+--------+-------+
| gender | count |
|--------+-------|
| M      | 3     |
| F      | 1     |
+--------+-------+
```


## Example 2: Find the most common values without the count display

The following query uses `showcount=false` to hide the `count` column in the results:

```ppl
source=accounts
| top showcount=false gender
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| gender |
|--------|
| M      |
| F      |
+--------+
```

## Example 3: Rename the count column

The following query uses the `countfield` parameter to specify a custom name (`cnt`) for the count column instead of the default `count`:
  
```ppl
source=accounts
| top countfield='cnt' gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------+-----+
| gender | cnt |
|--------+-----|
| M      | 3   |
| F      | 1   |
+--------+-----+
```
  
## Example 4: Limit the number of returned results

The following query returns the top 1 most common gender value:

```ppl
source=accounts
| top 1 showcount=false gender
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+
| gender |
|--------|
| M      |
+--------+
```


## Example 5: Group the results

The following query uses the `by` clause to find the most common age within each gender group and show it separately for each gender:

```ppl
source=accounts
| top 1 showcount=false age by gender
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+-----+
| gender | age |
|--------+-----|
| F      | 28  |
| M      | 32  |
+--------+-----+
```

## Example 6: Specify null value handling

The following query specifies `usenull=false` to exclude null values:

```ppl
source=accounts
| top usenull=false email
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

The following query specifies `usenull=true` to include null values in the results:

```ppl
source=accounts
| top usenull=true email
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
  

