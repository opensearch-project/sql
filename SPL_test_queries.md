# SPL Test Queries for Multisearch Examples

This document contains the SPL queries corresponding to the examples in the gap analysis, using the provided test data files.

## Data Files Created

1. `web_logs_test_data.json` - Web application logs with HTTP status codes
2. `logs_2024_test_data.json` - 2024 application logs with various severity levels
3. `logs_2023_test_data.json` - 2023 application logs with various severity levels  
4. `user_service_test_data.json` - User microservice logs
5. `order_service_test_data.json` - Order microservice logs

## Test Queries

### Example 1: Basic Success Rate Monitoring

**Data File**: `web_logs_test_data.json`

**SPL Query**:
```spl
| multisearch 
    [search source="web_logs_test_data.json" status=2* | eval query_type="good"] 
    [search source="web_logs_test_data.json" status=2* OR status=5* | eval query_type="valid"]
| stats count(eval(query_type="good")) as success_count,
        count(eval(query_type="valid")) as total_count
```


**Actual output**

``` 
_time	status	uri	response_time	index
2024-01-01 10:09:00	200	/api/health	5	main
2024-01-01 10:09:00	200	/api/health	5	main
2024-01-01 10:07:00	200	/api/auth	125	main
2024-01-01 10:07:00	200	/api/auth	125	main
2024-01-01 10:06:00	502	/api/users	3500	main
2024-01-01 10:05:00	201	/api/products	89	main
2024-01-01 10:05:00	201	/api/products	89	main
2024-01-01 10:04:00	200	/api/orders	67	main
2024-01-01 10:04:00	200	/api/orders	67	main
2024-01-01 10:03:00	503	/api/users	5000	main
2024-01-01 10:02:00	200	/api/products	23	main
2024-01-01 10:02:00	200	/api/products	23	main
2024-01-01 10:01:00	500	/api/orders	1200	main
2024-01-01 10:00:00	200	/api/users	45	main
2024-01-01 10:00:00	200	/api/users	45	main

```

``` 
success_count	total_count
6	               9
```

for this input:
``` 
| multisearch 
    [search source="web_logs_test_data_2.json" status=2* | eval query_type="good"] 
    [search source="web_logs_test_data_2.json" status=5* | eval query_type="valid"]
| table _time, status, uri, response_time, index, query_type
```

Got this output:

``` 
_time	status	uri	response_time	index	query_type
2024-01-01 10:09:00	200	/api/health	5	main	good
2024-01-01 10:07:00	200	/api/auth	125	main	good
2024-01-01 10:06:00	502	/api/users	3500	main	valid
2024-01-01 10:05:00	201	/api/products	89	main	good
2024-01-01 10:04:00	200	/api/orders	67	main	good
2024-01-01 10:03:00	503	/api/users	5000	main	valid
2024-01-01 10:02:00	200	/api/products	23	main	good
2024-01-01 10:01:00	500	/api/orders	1200	main	valid
2024-01-01 10:00:00	200	/api/users	45	main	good
```

### Example 2: Event Interleaving Comparison

**Data Files**: `logs_2024_test_data.json`, `logs_2023_test_data.json`

**Multisearch Query**:
```spl
| multisearch 
    [search source="logs_2024_test_data.json" severity=ERROR]
    [search source="logs_2023_test_data.json" severity=ERROR]
| table timestamp severity message index
```

```
_time	severity	message	index
2024-01-01 10:05:00	ERROR	Failed to write to disk	main
2024-01-01 10:04:00	ERROR	Memory threshold exceeded	main
2024-01-01 10:02:00	ERROR	Timeout on API call	main
2024-01-01 10:00:00	ERROR	Database connection failed	main
2023-12-31 23:59:00	ERROR	Rate limit exceeded	main
2023-12-31 23:58:00	ERROR	Service unavailable	main
```
**Append Query (for comparison)**:
```spl
source="logs_2024_test_data.json" severity=ERROR 
| append [search source="logs_2023_test_data.json" severity=ERROR]
| table timestamp severity message index
```

```
_time	severity	message	index
2024-01-01 10:05:00	ERROR	Failed to write to disk	main
2024-01-01 10:04:00	ERROR	Memory threshold exceeded	main
2024-01-01 10:02:00	ERROR	Timeout on API call	main
2024-01-01 10:00:00	ERROR	Database connection failed	main
2023-12-31 23:59:00	ERROR	Rate limit exceeded	main
2023-12-31 23:58:00	ERROR	Service unavailable	main
```

**Expected Difference**: 
- Multisearch: Events interleaved by timestamp
- Append: All 2024 events first, then all 2023 events

### Example 3: Service Health Monitoring

**Data Files**: `user_service_test_data.json`, `order_service_test_data.json`

**SPL Query**:
```spl
| multisearch 
    [search source="user_service_test_data.json" status=2* | eval service_health="healthy", service_name="users"]
    [search source="user_service_test_data.json" status=5* | eval service_health="unhealthy", service_name="users"]
    [search source="order_service_test_data.json" status=2* | eval service_health="healthy", service_name="orders"]
    [search source="order_service_test_data.json" status=5* | eval service_health="unhealthy", service_name="orders"]
| stats count(eval(service_health="healthy")) as healthy_requests,
        count(eval(service_health="unhealthy")) as unhealthy_requests by service_name
```

**Expected Output**:
```
service_name | healthy_requests | unhealthy_requests
users        | 3               | 2
orders       | 3               | 2
```

### Example 4: Invalid Command Test

**Data File**: `web_logs_test_data.json`

**Invalid Query (should fail)**:
```spl
| multisearch 
    [search source="web_logs_test_data.json" | stats count() by status]  # ERROR: stats is non-streaming
    [search source="web_logs_test_data.json" status=2*]
```

**Corrected Query**:
```spl
| multisearch 
    [search source="web_logs_test_data.json" response_time > 1000]  # OK: where-like filtering is streaming
    [search source="web_logs_test_data.json" status=2*]              # OK: search filtering is streaming
| stats count() by status  # Aggregation moved outside multisearch
```

## Additional Test Scenarios

### Performance Monitoring
```spl
| multisearch 
    [search source="user_service_test_data.json" latency_ms > 1000 | eval performance="slow"]
    [search source="order_service_test_data.json" latency_ms > 1000 | eval performance="slow"]
    [search source="user_service_test_data.json" latency_ms <= 100 | eval performance="fast"]
    [search source="order_service_test_data.json" latency_ms <= 100 | eval performance="fast"]
| stats count() by service, performance
```

### Time-based Analysis
```spl
| multisearch 
    [search source="logs_2024_test_data.json" severity=ERROR | eval year="2024"]
    [search source="logs_2023_test_data.json" severity=ERROR | eval year="2023"]
| stats count() by year
| eval error_trend=if(year="2024" AND count>2, "increasing", "stable")
```

### Complex Filtering
```spl
| multisearch 
    [search source="web_logs_test_data.json" status=2* response_time<100 | eval category="fast_success"]
    [search source="web_logs_test_data.json" status=2* response_time>=100 | eval category="slow_success"]
    [search source="web_logs_test_data.json" status=5* | eval category="server_error"]
| stats count() by category
| eval percentage=round((count/sum(count))*100, 2)
```

## How to Test in SPL

1. **Upload Data**: Import each JSON file as a separate index in Splunk
2. **Run Queries**: Execute the provided SPL queries
3. **Verify Results**: Compare outputs with expected results
4. **Compare Behaviors**: Run both multisearch and append versions to see the difference

## Key Validation Points

1. **Event Interleaving**: Verify that multisearch interleaves events by timestamp
2. **Streaming Validation**: Confirm that non-streaming commands (stats, sort) fail in subsearches
3. **Result Tagging**: Check that eval commands in subsearches properly tag results
4. **Aggregation**: Ensure stats operations work correctly on multisearch output
5. **Performance**: Compare execution time between multisearch and append approaches

## Expected SPL Behavior Notes

- **Event Order**: Events should be ordered by `_time` field across all subsearches
- **Field Schema**: All subsearches are automatically projected to a common field set
- **Error Handling**: Non-streaming commands in subsearches should produce clear error messages
- **Performance**: Multiple subsearches should execute concurrently, not sequentially

## Timestamp Overlap Test Cases

These test cases use datasets with overlapping timestamps to verify how SPL handles event ordering when timestamps intersect between different data sources.

### Test Case 1: Overlapping Service Logs

#### Data Files
Create two new test data files with overlapping timestamps:

**service_a_overlap_test_data.json**:

```json
[
  {
    "timestamp": "2024-01-01 10:00:00",
    "service": "service_a",
    "level": "INFO",
    "message": "Service A started",
    "index": "service_a"
  },
  {
    "timestamp": "2024-01-01 10:02:00", 
    "service": "service_a",
    "level": "ERROR",
    "message": "Service A database error",
    "index": "service_a"
  },
  {
    "timestamp": "2024-01-01 10:04:00",
    "service": "service_a", 
    "level": "INFO",
    "message": "Service A recovered",
    "index": "service_a"
  },
  {
    "timestamp": "2024-01-01 10:06:00",
    "service": "service_a",
    "level": "WARN", 
    "message": "Service A memory warning",
    "index": "service_a"
  }
]
```

**service_b_overlap_test_data.json**:

```json
[
  {
    "timestamp": "2024-01-01 10:01:00",
    "service": "service_b",
    "level": "INFO", 
    "message": "Service B started",
    "index": "service_b"
  },
  {
    "timestamp": "2024-01-01 10:03:00",
    "service": "service_b",
    "level": "ERROR",
    "message": "Service B connection failed", 
    "index": "service_b"
  },
  {
    "timestamp": "2024-01-01 10:05:00",
    "service": "service_b",
    "level": "INFO",
    "message": "Service B reconnected",
    "index": "service_b"
  },
  {
    "timestamp": "2024-01-01 10:07:00", 
    "service": "service_b",
    "level": "ERROR",
    "message": "Service B timeout error",
    "index": "service_b"
  }
]
```

#### SPL Test Queries

**Multisearch Query**:

```spl
| multisearch 
    [search source="service_a_overlap_test_data.json" | eval source_type="service_a_errors"]
    [search source="service_b_overlap_test_data.json" | eval source_type="service_b_errors"]
| table timestamp service level message source_type
```

``` 
timestamp	service	level	message	source_type
2024-01-01 10:07:00	service_b	ERROR	Service B timeout error	service_b_errors
2024-01-01 10:06:00	service_a	WARN	Service A memory warning	service_a_errors
2024-01-01 10:05:00	service_b	INFO	Service B reconnected	service_b_errors
2024-01-01 10:04:00	service_a	INFO	Service A recovered	service_a_errors
2024-01-01 10:03:00	service_b	ERROR	Service B connection failed	service_b_errors
2024-01-01 10:02:00	service_a	ERROR	Service A database error	service_a_errors
2024-01-01 10:01:00	service_b	INFO	Service B started	service_b_errors
2024-01-01 10:00:00	service_a	INFO	Service A started	service_a_errors
```

**Append Query**:

```spl
source="service_a_overlap_test_data.json" | eval source_type="service_a_errors"
| append [search source="service_b_overlap_test_data.json" | eval source_type="service_b_errors"]
| table timestamp service level message source_type
```
``` 
timestamp	service	level	message	source_type
2024-01-01 10:07:00	service_b	ERROR	Service B timeout error	service_b_errors
2024-01-01 10:06:00	service_a	WARN	Service A memory warning	service_a_errors
2024-01-01 10:05:00	service_b	INFO	Service B reconnected	service_b_errors
2024-01-01 10:04:00	service_a	INFO	Service A recovered	service_a_errors
2024-01-01 10:03:00	service_b	ERROR	Service B connection failed	service_b_errors
2024-01-01 10:02:00	service_a	ERROR	Service A database error	service_a_errors
2024-01-01 10:01:00	service_b	INFO	Service B started	service_b_errors
2024-01-01 10:00:00	service_a	INFO	Service A started	service_a_errors
```


**Test Question**: Do both commands produce the same ordering, or does multisearch interleave by timestamp?



## Hypothesis Testing

Based on our previous findings, we expect:
- **Hypothesis**: Both multisearch and append will show sequential concatenation, not timestamp interleaving
- **Alternative**: Multisearch might show timestamp-based interleaving when timestamps overlap significantly

These tests will provide definitive evidence of SPL's actual behavior with overlapping timestamps.