# Splunk to OpenSearch PPL Cheat Sheet

This cheat sheet helps Splunk users transition to OpenSearch's PPL. It maps common Splunk Search Processing Language (SPL) commands to their PPL equivalents with examples.

## Structure and Concepts

| Aspect | Splunk SPL | OpenSearch PPL | Notes |
|--------|------------|---------------|-------|
| Query structure | `search terms \| command` | `source = index \| command` | PPL requires explicit source at the beginning |
| Index reference | `index=name*` | `source=name*` | Different command to specify data source, [PPL support refering to multiple indices](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/general/identifiers.rst#multiple-indices)|
| Time field | Special `_time` field | User-specified timestamp field | No default time field in PPL, must reference it explicitly |


## Example Query Conversions

**Simple search:**
- Splunk: `error failed status=500`
- PPL: ```source=`*` multi_match(['*'], 'error failed') status=500```

**Aggregation:**
- Splunk: `... | stats count BY host, status | sort -count`
- PPL: `... | stats count() by host, status | sort - count`

**Time-based query:**
- Splunk: `... earliest=-7d | timechart span=1h count BY host`
- PPL: `... | where timestamp >= date_sub(now(), INTERVAL 1 DAY) | stats count() by span(timestamp, 1h), host`

**Complex calculation:**
- Splunk: `... | eval mb=bytes/1024/1024 | stats avg(mb) AS avg_mb BY host | where avg_mb > 100`
- PPL: `... | eval mb = bytes/1024/1024 | stats avg(mb) as avg_mb by host | where avg_mb > 100`

## Basic Search Syntax

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Basic search | `error` | `multi_match(['*'], 'error')` | PPL requires explicit use `multi_match` function to search any docs have fields match error |
| Multiple keyword search | `error failed` | `multi_match(['*'], 'error failed')` | PPL uses `multi_match` function to search any doc have fields match error or failed |
| Quoted phrases | `"error failed"` | `multi_match(['*'], 'error', type='phrase')` | PPL uses `multi_match` phrase to search any doc have fields match "error failed" phrase |
| Field value equals | `field = 404` | `field = '404'` | PPL requires single quotes for string values |
| Multiple values | `field IN (404, 503)` | `field in (404, 503)` | Same syntax |
| Field doesn't equal | `field != 404` | `field != 404` |  Same syntax |
| Wildcard search | `field = value*` | `like(field, 'value%')` | PPL uses `like()` function with % as wildcard |

## Field Selection and Manipulation

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Select fields | `... \| fields field1, field2` | `... \| fields field1, field2` | Same syntax |
| Exclude fields | `... \| fields - field3` | `... \| fields - field3` | Same syntax |
| Rename fields | `... \| rename field1 AS new_name` | `... \| rename field1 as new_name` | Same syntax |
| Calculate field | `... \| eval new_field=field1 + field2` | `... \| eval new_field = field1 + field2` | Same syntax |

## Filtering

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Filter results | `... \| where field > 100` | `... \| where field > 100` | Same syntax |
| Compound filter | `... \| where field1=200 OR field2=203` | `... \| where field1=200 or field2=203` | Same syntax |


## Aggregation

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Count | `... \| stats count` | `... \| stats count()` | PPL requires parentheses |
| Count by field | `... \| stats count BY field` | `... \| stats count() by field` | Similar syntax |
| Multiple aggregations | `... \| stats count, avg(field1) BY field2` | `... \| stats count(), avg(field1) by field2` | Similar syntax |
| Distinct count | `... \| stats dc(field)` | `... \| stats distinct_count(field)` | Function name difference |
| Min/Max | `... \| stats min(field), max(field)` | `... \| stats min(field), max(field)` | Similar syntax |
| Percentiles | `... \| stats perc95(field)` | `... \| stats percentile(field, 95)` | Different function syntax |

## Sorting and Limiting

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Sort ascending | `... \| sort field` | `... \| sort field` | Same syntax |
| Sort descending | `... \| sort - field` | `... \| sort - field` | Similar syntax |
| Sort multiple | `... \| sort field1, -field2` | `... \| sort field1, - field2` | Similar syntax |
| Limit results | `... \| head 10` | `... \| head 10` | Same syntax |
| Get last results | `... \| tail 10` | `... \| tail 10` | Same syntax |

## Time Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Relative time | `earliest=-1d latest=now()` | `timestamp >= date_sub(now(), INTERVAL 1 DAY) and timestamp <= now()` | PPL uses date functions |
| Time extraction | `... \| eval hour=strftime(_time, "%H")` | `... \| eval hour = date_format(timestamp, 'HH')` | PPL uses `date_format()` |
| Time bucket | `... \| bin _time span=5m \| stats count by _time` | `... \| stats count() by span(@timestamp, 5m)` | PPL uses `span()` |

## Dedup

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Deduplicate | `... \| dedup field1, field2` | `... \| dedup field1, field2` | Same syntax |
| Deduplicate with count | `... \| dedup 2 field1` | `... \| dedup 2 field1` | Same syntax |

## Lookup and Joins

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Lookup | `... \| lookup vendors product_id` | `... \| lookup vendors product_id` | Similar syntax |
| Join | `... \| join type=inner left=a right=b where a.name = b.name [search vendors]` | `... \| inner join left=a right=b ON a.name = b.name vendors` | Similar syntax |

## Common Functions

| Function Type | Splunk SPL | OpenSearch PPL | Notes |
|---------------|------------|---------------|-------|
| String concat | `... \| eval result=field1 + " " + field2` | `... \| eval result = concat(' ', field1, field2)` | PPL uses `concat()` function |
| Substring | `... \| eval result=substr(field, 0, 5)` | `... \| eval result = substring(field, 0, 5)` | Different function name |
| Case statement | `... \| eval result=case(field > 90, "A", field > 80, "B", 1==1, "C")` | `... \| eval result = case(field > 90 then 'A', field > 80, 'B' else 'C')` | Similar syntax |
| Replace | `... \| eval result=replace(field, "pattern", "replacement")` | `... \| eval result = replace(field, 'pattern', 'replacement')` | Similar syntax |
| If statement | `... \| eval result=if(field > 100, "High", "Low")` | `... \| eval result = if(field > 100, 'High', 'Low')` | Same syntax |

## Math Functions

| Function | Splunk SPL | OpenSearch PPL | Notes |
|----------|------------|---------------|-------|
| Absolute | `... \| eval result=abs(field)` | `... \| eval result = abs(field)` | Same syntax |
| Round | `... \| eval result=round(field, 2)` | `... \| eval result = round(field, 2)` | Same syntax |
| Ceiling | `... \| eval result=ceiling(field)` | `... \| eval result = ceil(field)` | Same syntax |
| Floor | `... \| eval result=floor(field)` | `... \| eval result = floor(field)` | Same syntax |
| Power | `... \| eval result=pow(field, 2)` | `... \| eval result = pow(field, 2)` | Same syntax |
| Square root | `... \| eval result=sqrt(field)` | `... \| eval result = sqrt(field)` | Same syntax |
