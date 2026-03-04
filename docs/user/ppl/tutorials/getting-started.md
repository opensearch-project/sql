# Getting Started with PPL

## What You'll Learn

In this tutorial, you'll learn the fundamentals of Piped Processing Language (PPL) by exploring OpenTelemetry observability data. By the end, you'll be able to:

- Write your first PPL query
- Filter data to find what matters
- Select specific fields to focus your analysis
- Aggregate data to uncover patterns
- Sort and limit results for better insights

**Estimated time:** 15 minutes

---

## What is PPL?

Piped Processing Language (PPL) is a query language designed for exploring and analyzing data in OpenSearch. It uses a simple, intuitive syntax where you chain commands together using the pipe (`|`) operator, similar to Unix shell commands.

Think of PPL as a data pipeline: you start with your data source, then pass it through a series of transformations, with each step refining your results until you get exactly what you need.

### Why Use PPL?

- **Intuitive syntax**: If you've used Unix pipes or similar tools, PPL will feel natural
- **Powerful for logs**: Designed specifically for log analysis and observability data
- **Fast exploration**: Quickly filter, transform, and aggregate data
- **Easy to learn**: Start writing useful queries in minutes

### PPL vs SQL

While OpenSearch also supports SQL, PPL offers advantages for exploratory analysis:

```ppl
// PPL: Read left to right, like a story
source=otel_logs
| where severity = "ERROR"
| stats count() by service.name
```

```sql
-- SQL: More verbose with explicit GROUP BY clause
SELECT service.name, COUNT(*) 
FROM otel_logs 
WHERE severity = 'ERROR' 
GROUP BY service.name
```

---

## Understanding the Dataset

Throughout this tutorial, we'll use OpenTelemetry (OTEL) observability data. OTEL is the industry standard for collecting telemetry data (logs, metrics, and traces) from applications.

### Sample Log Structure

Our `otel_logs` index contains application logs with these key fields:

| Field | Description | Example |
|-------|-------------|---------|
| `@timestamp` | When the log was created | `2024-03-15T10:30:45Z` |
| `severity` | Log level | `INFO`, `WARN`, `ERROR`, `DEBUG` |
| `service.name` | Application or service name | `checkout-service`, `payment-api` |
| `message` | Log message content | `Payment processed successfully` |
| `trace_id` | Distributed trace identifier | `a1b2c3d4e5f6...` |
| `span_id` | Span identifier within trace | `1a2b3c4d...` |
| `http.method` | HTTP request method | `GET`, `POST`, `PUT` |
| `http.status_code` | HTTP response status | `200`, `404`, `500` |
| `http.route` | API endpoint | `/api/checkout`, `/api/payment` |
| `user.id` | User identifier | `user_12345` |
| `duration_ms` | Operation duration | `245` |

### Sample Document

Here's an example document from the `otel_logs` index:

```json
{
  "@timestamp": "2024-03-15T11:30:40.000Z",
  "severity": "ERROR",
  "service.name": "checkout-service",
  "message": "Invalid product ID",
  "trace_id": "a1b2c3d4e5f6g7h8i9j0",
  "span_id": "1a2b3c4d5e6f7g8h",
  "http.method": "POST",
  "http.status_code": 400,
  "http.route": "/api/checkout",
  "user.id": "user_12345",
  "duration_ms": 1250
}
```

You can use this structure to generate similar test data for your own experiments.

---

## Your First Query: Exploring the Data

Let's start by looking at some sample data to understand what we're working with.

### Step 1: Retrieve Sample Records

The simplest PPL query retrieves data from a source:

```ppl
source=otel_logs | fields @timestamp, severity, service.name, message | head 5
```

```text
fetched rows / total rows = 5/5
+---------------------+----------+------------------+---------------------------------+
| @timestamp          | severity | service.name     | message                         |
|---------------------+----------+------------------+---------------------------------|
| 2024-03-15 11:30:40 | ERROR    | checkout-service | Invalid product ID              |
| 2024-03-15 11:30:40 | ERROR    | payment-api      | Insufficient funds check failed |
| 2024-03-15 11:30:50 | ERROR    | checkout-service | Inventory check failed          |
| 2024-03-15 11:30:50 | ERROR    | payment-api      | Transaction timeout             |
| 2024-03-15 11:31:00 | ERROR    | checkout-service | Invalid product ID              |
+---------------------+----------+------------------+---------------------------------+
```

**What this does:**
- `source=otel_logs` - Specifies which index to query
- `fields` - Selects specific fields to display
- `head 5` - Returns only the first 5 results

**💡 Tip:** The `head` command is perfect for previewing data before building complex queries.

---

## Filtering Data: Finding What Matters

Now that we've seen the data, let's find specific information. The `where` command filters results based on conditions.

### Step 2: Find All Errors

Let's find all error logs to investigate issues:

```ppl
source=otel_logs | where severity = "ERROR" | fields @timestamp, service.name, message | head 5
```

```text
fetched rows / total rows = 5/5
+---------------------+------------------+---------------------------------+
| @timestamp          | service.name     | message                         |
|---------------------+------------------+---------------------------------|
| 2024-03-15 11:30:40 | checkout-service | Invalid product ID              |
| 2024-03-15 11:30:40 | payment-api      | Insufficient funds check failed |
| 2024-03-15 11:30:50 | checkout-service | Inventory check failed          |
| 2024-03-15 11:30:50 | payment-api      | Transaction timeout             |
| 2024-03-15 11:31:00 | checkout-service | Invalid product ID              |
+---------------------+------------------+---------------------------------+
```

**What this does:**
- Filters to show only logs where `severity` equals `"ERROR"`
- Selects specific fields to display
- Limits results to 5 records

### Step 3: Filter by Multiple Conditions

Let's narrow down to errors from a specific service:

```ppl
source=otel_logs | where severity = "ERROR" AND service.name = "checkout-service" | fields @timestamp, service.name, message
```

**What this does:**
- Uses `AND` to combine multiple conditions
- Shows only errors from the checkout service

```text
fetched rows / total rows = 523/523
+---------------------+------------------+-----------------------------+
| @timestamp          | service.name     | message                     |
|---------------------+------------------+-----------------------------|
| 2024-03-15 11:30:40 | checkout-service | Invalid product ID          |
| 2024-03-15 11:30:50 | checkout-service | Inventory check failed      |
| 2024-03-15 11:31:00 | checkout-service | Invalid product ID          |
| 2024-03-15 11:31:10 | checkout-service | Invalid product ID          |
| 2024-03-15 11:31:20 | checkout-service | Invalid product ID          |
| 2024-03-15 11:31:30 | checkout-service | Payment processing error    |
| 2024-03-15 11:31:40 | checkout-service | Invalid product ID          |
| 2024-03-15 11:31:50 | checkout-service | Order validation failed     |
| 2024-03-15 11:32:00 | checkout-service | Payment processing error    |
| 2024-03-15 11:32:10 | checkout-service | Database connection timeout |
| 2024-03-15 11:32:20 | checkout-service | Order validation failed     |
| 2024-03-15 11:32:30 | checkout-service | Database connection timeout |
| 2024-03-15 11:32:40 | checkout-service | Payment processing error    |
| 2024-03-15 11:32:50 | checkout-service | Invalid product ID          |
| 2024-03-15 11:33:00 | checkout-service | Invalid product ID          |
...
+---------------------+------------------+-----------------------------+
```

### Step 4: Filter by Time Range

Most log analysis focuses on recent data. Let's find errors from a specific time period:

```ppl
source=otel_logs
| where severity = "ERROR" 
  AND @timestamp >= "2024-03-15 11:30:00"
  AND @timestamp < "2024-03-15 11:31:00"
| fields @timestamp, service.name, message
| head 5
```

```text
fetched rows / total rows = 5/5
+---------------------+------------------+---------------------------------+
| @timestamp          | service.name     | message                         |
|---------------------+------------------+---------------------------------|
| 2024-03-15 11:30:40 | checkout-service | Invalid product ID              |
| 2024-03-15 11:30:40 | payment-api      | Insufficient funds check failed |
| 2024-03-15 11:30:50 | checkout-service | Inventory check failed          |
| 2024-03-15 11:30:50 | payment-api      | Transaction timeout             |
| 2024-03-15 11:30:00 | checkout-service | Inventory check failed          |
+---------------------+------------------+---------------------------------+
```

**What this does:**
- Filters to errors within a specific time window
- Uses string literals for timestamp comparison
- Shows the first 5 matching results

**💡 Tip:** For relative time ranges, use functions like `date_sub(now(), INTERVAL 24 HOUR)` to calculate "24 hours ago"

---

## Aggregating Data: Uncover Patterns

Aggregation helps you understand trends and patterns in your data.

### Step 5: Count Total Errors

How many errors occurred?

```ppl
source=otel_logs
| where severity = "ERROR"
| stats count()
```

**What this does:**
- `stats count()` counts all matching records
- Returns a single number

**Expected output:**

```text
fetched rows / total rows = 1/1
+---------+
| count() |
|---------|
| 1247    |
+---------+
```

### Step 6: Count Errors by Service

Which services have the most errors?

```ppl
source=otel_logs
| where severity = "ERROR"
| stats count() by service.name
```

**What this does:**
- `by service.name` groups results by service
- Counts errors for each service

**Expected output:**

```text
fetched rows / total rows = 4/4
+---------+------------------+
| count() | service.name     |
|---------+------------------|
| 523     | checkout-service |
| 198     | inventory-svc    |
| 412     | payment-api      |
| 114     | user-service     |
+---------+------------------+
```

### Step 7: Multiple Aggregations

Let's get more insights with multiple aggregations:

```ppl
source=otel_logs
| where severity = "ERROR"
| stats count() as error_count, 
        avg(duration_ms) as avg_duration,
        max(duration_ms) as max_duration
  by service.name
| eval avg_duration = round(avg_duration, 2)
| fields service.name, error_count, avg_duration, max_duration
```

**What this does:**
- Calculates multiple metrics per service
- Uses `as` to name the calculated fields
- Uses `eval` with `round()` function to limit decimal places to 2
- Uses `fields` to control the column order in output
- Shows service name, error count, average duration, and max duration

**Expected output:**

```text
fetched rows / total rows = 4/4
+------------------+-------------+--------------+--------------+
| service.name     | error_count | avg_duration | max_duration |
|------------------+-------------+--------------+--------------|
| checkout-service | 523         | 1267.99      | 5000         |
| inventory-svc    | 198         | 466.71       | 1800         |
| payment-api      | 412         | 894.33       | 3200         |
| user-service     | 114         | 609.54       | 1094         |
+------------------+-------------+--------------+--------------+
```

**💡 Insight:** The checkout-service has both the most errors and the longest durations - a clear area for investigation!

---

## Sorting and Limiting: Prioritize Your Findings

### Step 8: Sort Results

Let's find which services have the most errors:

```ppl
source=otel_logs
| where severity = "ERROR"
| stats count() as error_count by service.name
| sort error_count desc
```

**What this does:**
- `sort error_count desc` sorts by error count, highest first
- `desc` means descending (use `asc` for ascending)

**Expected output:**

```text
fetched rows / total rows = 4/4
+-------------+------------------+
| error_count | service.name     |
|-------------+------------------|
| 523         | checkout-service |
| 412         | payment-api      |
| 198         | inventory-svc    |
| 114         | user-service     |
+-------------+------------------+
```

### Step 9: Top N Results

Focus on the top problem areas:

```ppl
source=otel_logs
| where severity = "ERROR"
| stats count() as error_count by service.name
| sort error_count desc
| head 3
```

**What this does:**
- Shows only the top 3 services with the most errors
- Perfect for prioritizing investigation efforts

**Expected output:**

```text
fetched rows / total rows = 3/3
+-------------+------------------+
| error_count | service.name     |
|-------------+------------------|
| 523         | checkout-service |
| 412         | payment-api      |
| 198         | inventory-svc    |
+-------------+------------------+
```

---

## Putting It All Together: A Real-World Query

Let's combine everything we've learned to answer: "What are the top error patterns by service and HTTP status code?"

```ppl
source=otel_logs
| where severity = "ERROR" 
  AND http.status_code >= 400
| stats count() as error_count,
        avg(duration_ms) as avg_response_time,
        max(duration_ms) as max_response_time
  by service.name, http.status_code
| eval avg_response_time = round(avg_response_time, 2)
| sort error_count desc
| head 10
| fields service.name, http.status_code, error_count, avg_response_time, max_response_time
```

**What this query does:**

1. **Filters** to errors with HTTP status codes 400+
2. **Aggregates** error counts and response times by service and status code
3. **Rounds** average response time to 2 decimal places
4. **Sorts** by error count to find the biggest problems
5. **Limits** to top 10 results
6. **Selects** fields in a logical order for analysis

**Expected output:**

```text
fetched rows / total rows = 10/10
+------------------+------------------+-------------+-------------------+-------------------+
| service.name     | http.status_code | error_count | avg_response_time | max_response_time |
|------------------+------------------+-------------+-------------------+-------------------|
| checkout-service | 400              | 265         | 1258.26           | 2354              |
| checkout-service | 500              | 258         | 1278.0            | 5000              |
| payment-api      | 503              | 143         | 921.71            | 3200              |
| payment-api      | 400              | 140         | 882.92            | 1593              |
| payment-api      | 500              | 129         | 876.36            | 1525              |
| inventory-svc    | 500              | 108         | 481.69            | 1800              |
| inventory-svc    | 400              | 90          | 448.73            | 785               |
| user-service     | 401              | 42          | 631.93            | 1094              |
| user-service     | 403              | 37          | 568.08            | 972               |
| user-service     | 500              | 35          | 626.49            | 1015              |
+------------------+------------------+-------------+-------------------+-------------------+
```

**💡 Key Insights from this query:**

- The checkout-service has the most errors with 265 errors for status code 400 and 258 for status code 500
- Status code 400 (Bad Request) and 500 (Internal Server Error) are the most common across services
- Checkout-service has the slowest average response times (~1258-1278ms)
- User-service shows 401 (Unauthorized) and 403 (Forbidden) errors, suggesting authentication/authorization issues
- This data suggests prioritizing checkout-service for both error handling and performance optimization

---

## Query Building Tips

### Start Simple, Then Refine

1. **Start broad**: `source=otel_logs | head 10`
2. **Add filters**: `| where severity = "ERROR"`
3. **Select fields**: `| fields @timestamp, service.name, message`
4. **Aggregate**: `| stats count() by service.name`
5. **Sort and limit**: `| sort count() desc | head 5`

### Common Patterns

**Pattern 1: Error Investigation**
```ppl
source=otel_logs
| where severity = "ERROR" AND service.name = "my-service"
| fields @timestamp, message, trace_id
| sort @timestamp desc
```

**Pattern 2: Performance Analysis**
```ppl
source=otel_logs
| where duration_ms > 1000
| stats avg(duration_ms), max(duration_ms), count() by http.route
| sort avg(duration_ms) desc
```

**Pattern 3: Traffic Analysis**
```ppl
source=otel_logs
| where http.method = "POST"
| stats count() by http.route, http.status_code
| sort count() desc
```

---

## Next Steps

Congratulations! You now know the fundamentals of PPL. Here's what to explore next:

### Continue Learning

- **[Learn Common Commands](learn-common-commands.md)** - Master the 10 most-used PPL commands
- **[Quick Reference](../quick-reference.md)** - Handy cheat sheet for all commands
- **[Functions Reference](../functions/index.md)** - Explore available functions for calculations

### Explore Advanced Features

- **[Join Command](../cmd/join.md)** - Combine data from multiple sources
- **[Parse Command](../cmd/parse.md)** - Extract structured data from text
- **[Timechart Command](../cmd/timechart.md)** - Create time-based visualizations
- **[Patterns Command](../cmd/patterns.md)** - Discover log patterns automatically

### Practice Exercises

Try these queries on your own data:

1. Find the slowest API endpoints in the last hour
2. Count log messages by severity level
3. Identify which users encountered the most errors
4. Calculate average response time by HTTP method
5. Find services with error rates above 5%

---

## Getting Help

- **[PPL Command Reference](../cmd/index.md)** - Complete command documentation
- **[PPL Syntax Guide](../cmd/syntax.md)** - Detailed syntax rules
- **[Data Types](../general/datatypes.md)** - Understanding PPL data types
- **[Limitations](../limitations/limitations.md)** - Known limitations and workarounds
- **Have questions or issues?** 
  - Submit issues to the [OpenSearch SQL repository](https://github.com/opensearch-project/sql/issues)
  - Join our [public Slack channel](https://opensearch.org/slack.html) for community support  
