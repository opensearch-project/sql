
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
  

## Example 1: Append a different aggregation to existing results

This example appends per-severity counts alongside per-service totals. Because the subsearch returns fewer rows (5 severity levels) than the main search (9 services), the extra rows get `NULL` values:

```ppl
source=otellogs
| stats count() as total by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| appendcol [ stats count() as sev_count by severityText | sort severityText ]
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+-------+----------------------------------+-----------+--------------+
| total | resource.attributes.service.name | sev_count | severityText |
|-------+----------------------------------+-----------+--------------|
| 2     | api-gateway                      | 3         | DEBUG        |
| 4     | auth-service                     | 5         | ERROR        |
| 3     | cart-service                     | 2         | FATAL        |
| 1     | cert-monitor                     | 6         | INFO         |
| 2     | frontend                         | 4         | WARN         |
| 4     | inventory-service                | null      | null         |
+-------+----------------------------------+-----------+--------------+
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
| api-gateway                      | ERROR        | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     | 7              | 6                 |
| auth-service                     | ERROR        | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      | null           | null              |
| cart-service                     | ERROR        | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) | null           | null              |
| inventory-service                | FATAL        | Database primary node unreachable: connection refused to db-primary-01:5432                  | null           | null              |
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
