# Splunk to OpenSearch PPL Cheat Sheet

This cheat sheet helps Splunk users transition to OpenSearch's PPL. It maps common Splunk Search Processing Language (SPL) commands to their PPL equivalents with examples.

## Structure and Concepts

| Aspect | Splunk SPL | OpenSearch PPL | Notes |
|--------|------------|---------------|-------|
| Query structure | `search terms \| command` | `source = index \| command` | PPL requires explicit source at the beginning |
| Index reference | `index=name*` | `source=name*` | Different command to specify data source, [PPL support refering to multiple indices](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/general/identifiers.rst#multiple-indices)|
| Time field | Special `_time` field | User-specified timestamp field | No default time field in PPL, must reference it explicitly |


## Command Reference

This table provides a mapping between Splunk SPL commands and their OpenSearch PPL equivalents:

| Splunk SPL | OpenSearch PPL | Purpose |
|------------|---------------|---------|
| search | search | Basic searching of data |
| stats | stats | Statistical aggregation of data |
| eval | eval | Calculate and create new fields |
| rex | parse | Extract fields using regular expressions |
| table | fields | Select specific fields to display |
| bucket | span | Group numeric or time values into buckets |
| spath | json_extract | Extract fields from structured data (JSON) |
| where | where | Filter results based on conditions |
| dedup | dedup | Remove duplicate results |
| sort | sort | Sort results by specified fields |
| rename | rename | Rename fields in results |
| appendcols | appendcols | Append columns from subsearch |
| lookup | lookup | Enrich data with lookups |
| join | join | Combine results from multiple sources |
| fields | fields | Include or exclude fields |
| fillnull | fillnull | Replace null values |
| eventstats | eventstats | Calculate statistics while preserving events |
| bin | span | Group numeric values into bins |
| head | head | Retrieve the first N results |
| trendline | trendline | Generate trend lines for data |
| top | top | Find the most common values |
| mvexpand | expand | Expand multi-value fields |
| rare | rare | Find the least common values |

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
| Field value equals | `field = 404` | `field = 404` | Same syntax |
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

## Rex vs Parse

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Basic extraction | `... \| rex field=address "(?<streetNumber>\d+) (?<street>.+)"` | `... \| parse address "(?<streetNumber>\d+) (?<street>.+)"` | Similar syntax but different command name |
| Field specification | `... \| rex field=address ...` | `... \| parse address ...` | PPL uses direct field reference without "field=" prefix |
| Default field (_raw) | `... \| rex "(?<streetNumber>\d+) (?<street>.+)"` | Not supported | PPL does not support implicit _raw field and requires explicit field specification |
| Search and replace mode | `... \| rex mode=sed "s/\d+//g"` | Not supported | PPL does not support the search-and-replace mode like SPL's rex with mode=sed |
| Field override | `... \| rex field=address "(?<address>.+)"` | `... \| parse address "(?<address>.+)"` | Both support extracting to field with same name as source |

## Time Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Relative time | `earliest=-1d latest=now()` | `earliest("-1d", timestamp) and latest("now", timestamp)` | PPL supports earliest() and latest() functions |
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
| Basic lookup | `... \| lookup vendors product_id` | `... \| lookup vendors product_id` | Same syntax |
| Multiple mapping fields | `... \| lookup vendors product_id, category` | `... \| lookup vendors product_id, category` | Same syntax |
| Aliased field lookup | `... \| lookup vendors product AS id` | `... \| lookup vendors product as id` | PPL uses lowercase "as" |
| Lookup with append | Not applicable | `... \| lookup vendors product_id append dept as department` | PPL-specific feature |
| Lookup with replace | Not applicable | `... \| lookup vendors product_id replace dept as department` | PPL-specific feature |
| Inner join | `... \| join type=inner vendors [search index=vendors]` | `... \| inner join vendors` | Different syntax format |
| Left join | `... \| join type=left vendors [search index=vendors]` | `... \| left join vendors` | Different syntax format |
| Join with ON clause | `... \| join type=inner left=a right=b where a.id = b.id vendors` | `... \| inner join left=a right=b ON a.id = b.id vendors` | PPL uses "ON" instead of "where" |
| Append columns | `... \| appendcols [search source=other_index \| fields id, status]` | `... \| appendcols [source=other_index \| fields id, status]` | Similar syntax |

## Field Manipulation

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Include fields | `... \| fields field1, field2` | `... \| fields field1, field2` | Same syntax |
| Exclude fields | `... \| fields - field3` | `... \| fields - field3` | Same syntax |
| Rename fields | `... \| rename field1 AS new_name` | `... \| rename field1 as new_name` | PPL uses lowercase "as" |
| Replace null values | `... \| fillnull value=0 field1, field2` | `... \| fillnull value=0 field1, field2` | Same syntax |
| Expand multi-value | `... \| mvexpand field1` | `... \| expand field1` | Different command name |

## Results Limiting

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| First N results | `... \| head 10` | `... \| head 10` | Same syntax |
| Last N results | `... \| tail 10` | `... \| tail 10` | Same syntax |
| Moving average | `... \| trendline sma5(value)` | `... \| trendline sma5(value)` | Same syntax |
| Top values | `... \| top 10 field` | `... \| top 10 field` | Same syntax |
| Rare values | `... \| rare 10 field` | `... \| rare 10 field` | Same syntax |

## String Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| String concatenation | `... \| eval result=field1 + " " + field2` | `... \| eval result = concat(field1, ' ', field2)` | PPL requires `concat()` function |
| Substring | `... \| eval result=substr(field, 0, 5)` | `... \| eval result = substring(field, 0, 5)` | Different function name |
| String length | `... \| eval length=len(field)` | `... \| eval length = length(field)` | Different function name |
| Lowercase | `... \| eval result=lower(field)` | `... \| eval result = lower(field)` | Same syntax |
| Uppercase | `... \| eval result=upper(field)` | `... \| eval result = upper(field)` | Same syntax |
| Replace | `... \| eval result=replace(field, "pattern", "replacement")` | `... \| eval result = replace(field, 'pattern', 'replacement')` | Same syntax |
| Trim whitespace | `... \| eval result=trim(field)` | `... \| eval result = trim(field)` | Same syntax |
| Contains (wildcard) | `... \| eval result=like(field, "%pattern%")` | `... \| eval result = like(field, '%pattern%')` | Same syntax |

## Conditional Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| If condition | `... \| eval result=if(field > 100, "High", "Low")` | `... \| eval result = if(field > 100, 'High', 'Low')` | Same syntax |
| Case statement | `... \| eval grade=case(field > 90, "A", field > 80, "B", 1==1, "C")` | `... \| eval grade = case(field > 90 then 'A', field > 80 then 'B', else 'C')` | PPL uses `then` and `else` keywords |
| NULL check | `... \| eval result=if(isnull(field), "Missing", field)` | `... \| eval result = if(isnull(field), 'Missing', field)` | Same syntax |
| Empty check | `... \| eval result=if(isnotnull(field), field, "Default")` | `... \| eval result = if(isnotnull(field), field, 'Default')` | Same syntax |
| Coalesce (first non-null) | `... \| eval result=coalesce(field1, field2, "default")` | `... \| eval result = coalesce(field1, field2, 'default')` | Same syntax |

## Math Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Addition | `... \| eval sum=field1 + field2` | `... \| eval sum = field1 + field2` | Same syntax |
| Subtraction | `... \| eval diff=field1 - field2` | `... \| eval diff = field1 - field2` | Same syntax |
| Multiplication | `... \| eval product=field1 * field2` | `... \| eval product = field1 * field2` | Same syntax |
| Division | `... \| eval quotient=field1 / field2` | `... \| eval quotient = field1 / field2` | Same syntax |
| Modulo | `... \| eval remainder=field1 % field2` | `... \| eval remainder = field1 % field2` | Same syntax |
| Absolute value | `... \| eval result=abs(field)` | `... \| eval result = abs(field)` | Same syntax |
| Round | `... \| eval result=round(field, 2)` | `... \| eval result = round(field, 2)` | Same syntax |
| Ceiling | `... \| eval result=ceiling(field)` | `... \| eval result = ceil(field)` | Different function name |
| Floor | `... \| eval result=floor(field)` | `... \| eval result = floor(field)` | Same syntax |
| Power | `... \| eval result=pow(field, 2)` | `... \| eval result = pow(field, 2)` | Same syntax |
| Square root | `... \| eval result=sqrt(field)` | `... \| eval result = sqrt(field)` | Same syntax |

## Date and Time Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| Current time | `... \| eval now=now()` | `... \| eval now = now()` | Same syntax |
| Format time | `... \| eval date=strftime(_time, "%Y-%m-%d")` | `... \| eval date = date_format(timestamp, 'yyyy-MM-dd')` | Different function and format |
| Extract part | `... \| eval month=date_part("month", _time)` | `... \| eval month = date_part('month', timestamp)` | Similar syntax |
| Day ago | `... \| eval yesterday=relative_time(now(), "-1d")` | `... \| eval yesterday = date_sub(now(), INTERVAL 1 DAY)` | PPL uses interval syntax |
| Day ahead | `... \| eval tomorrow=relative_time(now(), "+1d")` | `... \| eval tomorrow = date_add(now(), INTERVAL 1 DAY)` | PPL uses interval syntax |
| Time difference | `... \| eval diff=(_time2 - _time1)` | `... \| eval diff = date_diff('second', timestamp1, timestamp2)` | PPL uses function |

## Other Functions

| Operation | Splunk SPL | OpenSearch PPL | Notes |
|-----------|------------|---------------|-------|
| MD5 hash | Not native | `... \| eval hash = md5('string')` | PPL-specific feature |
| SHA1 hash | Not native | `... \| eval hash = sha1('string')` | PPL-specific feature |
| JSON extraction | `... \| spath input=data path=user.name output=username` | `... \| eval username = json_extract(data, '$.user.name')` | Different approach |
