
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
  

## Example 1: Append a different aggregation alongside existing results

This example shows log counts per service alongside error counts per service. Because both queries group by service name in the same sort order, the rows align correctly:

```ppl
source=otellogs
| stats count() as total_logs by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| appendcol [ where severityText = 'ERROR' | stats count() as error_count by `resource.attributes.service.name` | sort `resource.attributes.service.name` ]
| fields `resource.attributes.service.name`, total_logs, error_count
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+----------------------------------+------------+-------------+
| resource.attributes.service.name | total_logs | error_count |
|----------------------------------+------------+-------------|
| cart                             | 3          | 2           |
| checkout                         | 3          | 1           |
| frontend                         | 4          | 2           |
| frontend-proxy                   | 3          | 1           |
| payment                          | 2          | 1           |
| product-catalog                  | 4          | null        |
| recommendation                   | 1          | null        |
+----------------------------------+------------+-------------+
```

## Example 2: Append multiple subsearch results

The following query chains multiple `appendcol` commands to add summary statistics alongside detail rows. The first appendcol adds the total error count, and the second adds the number of affected services:

```ppl
source=otellogs
| where severityText = 'ERROR'
| fields `resource.attributes.service.name`, severityText, body
| sort `resource.attributes.service.name`
| appendcol [ where severityText = 'ERROR' | stats count() as total_errors ]
| appendcol [ where severityText = 'ERROR' | stats distinct_count(`resource.attributes.service.name`) as services_affected ]
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------------------------+--------------+----------------------------------------------------------------------------------------------+--------------+-------------------+
| resource.attributes.service.name | severityText | body                                                                                         | total_errors | services_affected |
|----------------------------------+--------------+----------------------------------------------------------------------------------------------+--------------+-------------------|
| checkout                         | ERROR        | NullPointerException in CheckoutService.placeOrder at line 142                               | 7            | 5                 |
| checkout                         | ERROR        | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) | null         | null              |
| frontend-proxy                   | ERROR        | [2024-02-01T09:20:00.456Z] "POST /api/checkout HTTP/1.1" 503 - 0 30000 checkout-8d4f7b-mk2p9 | null         | null              |
| payment                          | ERROR        | Payment failed: connection timeout to payment gateway after 30000ms                          | null         | null              |
+----------------------------------+--------------+----------------------------------------------------------------------------------------------+--------------+-------------------+
```

## Example 3: Resolve column name conflicts using the override parameter

When the main search and subsearch share a column name, `override=true` replaces the main search values with the subsearch values. In this example, both produce a column named `agg` -- the main search uses it for total log counts, the subsearch for error-only counts. With override, the error counts replace the totals:

```ppl
source=otellogs
| stats count() as agg by severityText
| sort severityText
| appendcol override=true [ where severityText IN ('ERROR', 'WARN') | stats count() as agg by severityText | sort severityText ]
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----+--------------+
| agg | severityText |
|-----+--------------|
| 7   | ERROR        |
| 4   | WARN         |
| 6   | INFO         |
| 4   | WARN         |
+-----+--------------+
```

## Limitations

The `appendcol` command has the following limitations:

* **Row alignment**: The subsearch results are appended positionally (row by row). If the main search and subsearch return different numbers of rows, the shorter result set is padded with `NULL` values.
* **Schema compatibility**: When fields with the same name exist in both the main search and the subsearch but have incompatible types, the query fails with an error.
