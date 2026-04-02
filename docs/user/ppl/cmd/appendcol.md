
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

This example appends the maximum severity number per severity level alongside the count. Because both queries group by `severityText` in the same sort order, the rows align correctly:

```ppl
source=otellogs
| stats count() as log_count by severityText
| sort severityText
| appendcol [ stats max(severityNumber) as max_sev by severityText | sort severityText ]
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------+--------------+---------+
| log_count | severityText | max_sev |
|-----------+--------------+---------|
| 3         | DEBUG        | 5       |
| 5         | ERROR        | 17      |
| 2         | FATAL        | 21      |
| 6         | INFO         | 9       |
| 4         | WARN         | 13      |
+-----------+--------------+---------+
```

## Example 2: Append multiple subsearch results

The following query chains multiple `appendcol` commands to add summary statistics alongside detail rows. The first appendcol adds the total failure count, and the second adds the number of affected services:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| fields `resource.attributes.service.name`, severityText, body
| sort `resource.attributes.service.name`
| appendcol [ where severityText IN ('ERROR', 'FATAL') | stats count() as total_failures ]
| appendcol [ where severityText IN ('ERROR', 'FATAL') | stats distinct_count(`resource.attributes.service.name`) as services_affected ]
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------------------------+--------------+----------------------------------------------------------------------------------------------+----------------+-------------------+
| resource.attributes.service.name | severityText | body                                                                                         | total_failures | services_affected |
|----------------------------------+--------------+----------------------------------------------------------------------------------------------+----------------+-------------------|
| checkout                         | ERROR        | NullPointerException in CheckoutService.placeOrder at line 142                               | 7              | 5                 |
| checkout                         | ERROR        | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) | null           | null              |
| frontend-proxy                   | ERROR        | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     | null           | null              |
| payment                          | ERROR        | Payment failed: connection timeout to payment gateway after 30000ms                          | null           | null              |
+----------------------------------+--------------+----------------------------------------------------------------------------------------------+----------------+-------------------+
```

## Example 3: Resolve column name conflicts using the override parameter

When the main search and subsearch share a column name, `override=true` replaces the main search values with the subsearch values. In this example, both produce a column named `agg` -- the main search uses it for total log counts, the subsearch for error-only counts. With override, the error counts replace the totals:

```ppl
source=otellogs
| stats count() as agg by severityText
| sort severityText
| appendcol override=true [ where severityText IN ('ERROR', 'FATAL') | stats count() as agg by severityText | sort severityText ]
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----+--------------+
| agg | severityText |
|-----+--------------|
| 5   | ERROR        |
| 2   | FATAL        |
| 2   | FATAL        |
| 6   | INFO         |
| 4   | WARN         |
+-----+--------------+
```

## Limitations

The `appendcol` command has the following limitations:

* **Row alignment**: The subsearch results are appended positionally (row by row). If the main search and subsearch return different numbers of rows, the shorter result set is padded with `NULL` values.
* **Schema compatibility**: When fields with the same name exist in both the main search and the subsearch but have incompatible types, the query fails with an error.
