# OpenSearch PPL Reference Manual  

## What is PPL?

Piped Processing Language (PPL) is a query language for exploring and analyzing data in OpenSearch. It uses an intuitive pipe (`|`) syntax to chain commands together, making it easy to filter, transform, and aggregate your data.

PPL is designed for developers, DevOps engineers, SREs, and anyone working with log, monitoring, and observability data. If you're familiar with Unix pipes or similar tools, you'll feel right at home.

### Why Use PPL?

- **Intuitive syntax**: Chain commands with pipes, read queries left to right like a story
- **Fast exploration**: Quickly filter and analyze data without complex syntax
- **Powerful for logs**: Built specifically for log analysis and observability use cases
- **Easy to learn**: Start writing useful queries in minutes, master it in hours

### PPL vs Other Query Languages

**PPL** uses a pipeline approach that's easy to read and modify:
```ppl
source=logs | where status = "error" | stats count() by service
```

**SQL** requires understanding of clause order and grouping:
```sql
SELECT service, COUNT(*) FROM logs WHERE status = 'error' GROUP BY service
```

**Query DSL** is powerful but has a steep learning curve with nested JSON structures.

### Quick Example

Here's a simple query that finds errors and counts them by service:

```ppl
source=logs
| where status = "error"
| stats count() by service
```

Sample result:
```
service          | count()
-----------------|--------
payment-api      | 145
user-service     | 89
notification-svc | 34
```

This query:
1. Starts with the `logs` data source
2. Filters to records where status equals "error"
3. Counts the errors grouped by service name

---

## Getting Started

New to PPL? Start here:

**[Getting Started Tutorial](tutorials/getting-started.md)** - Learn PPL basics in 15 minutes with hands-on examples

---

## Quick Command Reference

All PPL commands organized by use case. Commands marked as **stable** are production-ready; **experimental** commands are ready for use but specific parameters may change based on feedback.

### Filter & Search Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [search](cmd/search.md) | 1.0 | stable | Retrieve documents from index | `source=logs` |
| [where](cmd/where.md) | 1.0 | stable | Filter by condition | `where status = "error"` |
| [regex](cmd/regex.md) | 3.3 | experimental | Filter by regex pattern | `regex field="pattern"` |
| [subquery](cmd/subquery.md) | 3.0 | experimental | Embed query for filtering | `where field IN [ source=... \| fields field ]` |

### Transform Data Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [fields](cmd/fields.md) | 1.0 | stable | Select specific fields | `fields timestamp, message` |
| [rename](cmd/rename.md) | 1.0 | stable | Rename fields | `rename old_name as new_name` |
| [eval](cmd/eval.md) | 1.0 | stable | Calculate new fields | `eval duration = end - start` |
| [replace](cmd/replace.md) | 3.4 | experimental | Replace text in fields | `replace field "old" with "new"` |
| [fillnull](cmd/fillnull.md) | 3.0 | experimental | Fill null values | `fillnull with 0 in field` |
| [table](cmd/table.md) | 3.3 | experimental | Enhanced field selection | `table field1, field2` |

### Parse & Extract Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [parse](cmd/parse.md) | 1.3 | stable | Extract with regex | `parse message "Error: %{error}"` |
| [grok](cmd/grok.md) | 2.4 | stable | Extract with grok patterns | `grok message "%{COMMONAPACHELOG}"` |
| [rex](cmd/rex.md) | 3.3 | experimental | Extract with named groups | `rex field=message "(?<code>\\d+)"` |
| [spath](cmd/spath.md) | 3.3 | experimental | Extract from structured text | `spath path=field.subfield` |
| [patterns](cmd/patterns.md) | 2.4 | stable | Discover log patterns | `patterns message` |

### Aggregate & Analyze Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [stats](cmd/stats.md) | 1.0 | stable | Calculate aggregations | `stats count() by service` |
| [eventstats](cmd/eventstats.md) | 3.1 | experimental | Add stats as new fields | `eventstats avg(value) by group` |
| [streamstats](cmd/streamstats.md) | 3.4 | experimental | Cumulative statistics | `streamstats sum(amount) as total` |
| [bin](cmd/bin.md) | 3.3 | experimental | Group into buckets | `bin span=1h timestamp` |
| [timechart](cmd/timechart.md) | 3.3 | experimental | Time-based charts | `timechart span=1h count()` |
| [chart](cmd/chart.md) | 3.4 | experimental | Statistical charts | `chart count() by status, service` |
| [trendline](cmd/trendline.md) | 3.0 | experimental | Moving averages | `trendline sma(5, value)` |

### Sort & Limit Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [sort](cmd/sort.md) | 1.0 | stable | Order results | `sort timestamp desc` |
| [reverse](cmd/reverse.md) | 3.2 | experimental | Reverse order | `reverse` |
| [head](cmd/head.md) | 1.0 | stable | Limit to first N | `head 10` |
| [dedup](cmd/dedup.md) | 1.0 | stable | Remove duplicates | `dedup user_id` |
| [top](cmd/top.md) | 1.0 | stable | Most common values | `top 5 service` |
| [rare](cmd/rare.md) | 1.0 | stable | Least common values | `rare 5 error_code` |

### Join & Combine Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [join](cmd/join.md) | 3.0 | stable | Combine datasets | `join left=l right=r on l.id = r.id` |
| [append](cmd/append.md) | 3.3 | experimental | Append results | `append [ source=other ]` |
| [appendcol](cmd/appendcol.md) | 3.1 | experimental | Append columns | `appendcol [ source=other ]` |
| [lookup](cmd/lookup.md) | 3.0 | experimental | Enrich from lookup | `lookup users user_id` |
| [multisearch](cmd/multisearch.md) | 3.4 | experimental | Multiple searches | `multisearch [ source=logs1 ], [ source=logs2 ]` |

### Array & Multi-Value Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [expand](cmd/expand.md) | 3.1 | experimental | Expand nested arrays | `expand array_field` |
| [flatten](cmd/flatten.md) | 3.1 | experimental | Flatten struct fields | `flatten object_field` |
| [mvexpand](cmd/mvexpand.md) | 3.6 | stable | Expand multi-value field | `mvexpand tags` |
| [mvcombine](cmd/mvcombine.md) | 3.5 | stable | Combine multi-values | `mvcombine field` |
| [nomv](cmd/nomv.md) | 3.6 | stable | Convert to single value | `nomv field` |

### Utility Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [describe](cmd/describe.md) | 2.1 | stable | Show index metadata | `describe logs` |
| [explain](cmd/explain.md) | 3.1 | stable | Show query plan | `explain source=logs` |
| [show datasources](cmd/showdatasources.md) | 2.4 | stable | List datasources | `show datasources` |
| [addtotals](cmd/addtotals.md) | 3.5 | stable | Add row/column totals | `addtotals` |
| [addcoltotals](cmd/addcoltotals.md) | 3.5 | stable | Add column totals | `addcoltotals` |
| [transpose](cmd/transpose.md) | 3.5 | stable | Transpose rows/columns | `transpose` |

### Machine Learning Commands

| Command | Version | Status | Description | Syntax Example |
|---------|---------|--------|-------------|----------------|
| [ml](cmd/ml.md) | 2.5 | stable | Machine learning algorithms | `ml action="train" algorithm="kmeans"` |
| [kmeans](cmd/kmeans.md) | 1.3 | stable | K-means clustering | `kmeans centroids=3` |
| [ad](cmd/ad.md) | 1.3 | deprecated | Anomaly detection (use ml instead) | `ad` |
| [graphlookup](cmd/graphlookup.md) | 3.6 | experimental | Graph traversal | `graphlookup from=nodes connectFromField=id` |

---

## Functions

PPL provides a rich set of functions for data manipulation and analysis:

### Common Functions by Category

**Aggregation Functions**  
`count()`, `sum()`, `avg()`, `min()`, `max()`, `stddev()`, `var()`, `percentile()`, `distinct_count()`

**String Functions**  
`concat()`, `substring()`, `upper()`, `lower()`, `trim()`, `length()`, `replace()`, `split()`

**Date/Time Functions**  
`now()`, `date_format()`, `date_add()`, `date_sub()`, `date_diff()`, `year()`, `month()`, `day()`, `hour()`

**Math Functions**  
`abs()`, `ceil()`, `floor()`, `round()`, `sqrt()`, `pow()`, `log()`, `exp()`

**Condition Functions**  
`if()`, `case()`, `coalesce()`, `nullif()`, `isnull()`, `isnotnull()`

### Complete Function Reference

- **[Aggregation Functions](functions/aggregations.md)** - Statistical calculations
- **[String Functions](functions/string.md)** - Text manipulation
- **[Date and Time Functions](functions/datetime.md)** - Temporal operations
- **[Math Functions](functions/math.md)** - Mathematical operations
- **[Condition Functions](functions/condition.md)** - Conditional logic
- **[Type Conversion Functions](functions/conversion.md)** - Data type conversions
- **[Collection Functions](functions/collection.md)** - Array and list operations
- **[Cryptographic Functions](functions/cryptographic.md)** - Hashing and encryption
- **[IP Address Functions](functions/ip.md)** - IP address manipulation
- **[JSON Functions](functions/json.md)** - JSON parsing and manipulation
- **[Relevance Functions](functions/relevance.md)** - Full-text search scoring
- **[System Functions](functions/system.md)** - System information
- **[Expressions](functions/expressions.md)** - Operators and expression syntax

---

## Documentation & Resources

- **[Optimization Guide](../../user/optimization/optimization.rst)** - Query performance tuning
- **[Limitations](limitations/limitations.md)** - Known limitations and workarounds
- **[OpenSearch Documentation](https://opensearch.org/docs/latest/)** - Main OpenSearch docs

---

## Need Help?

- **New to PPL?** Start with the [Getting Started Tutorial](tutorials/getting-started.md)
- **Looking for a specific command?** Browse the [Quick Command Reference](#quick-command-reference) above
- **Need detailed syntax?** Check individual command pages linked in the reference tables
- **Have questions or issues?** 
  - Submit issues to the [OpenSearch SQL repository](https://github.com/opensearch-project/sql/issues)
  - Join our [public Slack channel](https://opensearch.org/slack.html) for community support  
