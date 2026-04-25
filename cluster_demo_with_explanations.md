# Cluster Command Demo - OpenSearch PPL

## Algorithm Reference

Select the method used to determine the similarity between events:
- **termlist**: breaks down the field into words and requires the exact same ordering of terms
- **termset**: allows for an unordered set of terms
- **ngramset**: compares sets of trigram (3-character substrings). ngramset is significantly slower on large field values and is most useful for short non-textual fields, like punct.

## What is the Cluster Command?

The `cluster` command is a new feature in OpenSearch PPL that automatically groups similar text documents together using machine learning clustering algorithms. Think of it like "smart grouping" for log analysis - instead of manually writing complex regex patterns to find similar errors, the cluster command does it automatically by analyzing text similarity.

## Why Use Clustering?

**Traditional Problem**:
- You have thousands of log entries with slight variations: "Database connection failed to server1", "Database connection failed to server2", "DB connection error on server3"
- Finding patterns requires writing complex queries or manual analysis
- Similar issues get scattered across results

**Cluster Solution**:
- Automatically groups these similar messages together
- Shows you the most common error patterns
- Reduces noise and highlights important trends
- Perfect for log analysis, error monitoring, and anomaly detection

## Setup
1. Start your debug server on localhost:9200
2. Load the sample data: `cluster_demo_data.json` (24 diverse log messages designed to show clustering)

## Demo Scenarios

### 1. Basic Clustering - "Show me similar error patterns"

**What you're doing**: Finding log messages that are similar to each other without knowing what to look for ahead of time.

```ppl
source=logs
| cluster message match=termset t=0.4
| fields message, cluster_label
| head 10
```

**Why this works**: The algorithm compares each log message and groups similar ones together. Messages about "database connection failed" will get the same cluster_label, even if the exact wording varies.

**What you'll see**: Each message gets a cluster_label (1, 2, 3, etc.). Messages with the same label are considered similar.

---

### 2. Cluster Analysis with Count - "What are my biggest problems?"

**What you're doing**: Not only grouping similar messages, but counting how many messages are in each group to identify the most frequent issues.

```ppl
source=logs
| cluster message match=termset t=0.3 showcount=true
| fields message, cluster_label, cluster_count
| sort -cluster_count
| head 5
```

**Why this matters**: In production logs, the biggest clusters usually represent your most critical recurring issues. A cluster with 500 messages is more urgent than one with 2 messages.

**What you'll see**:
- `cluster_count` shows how many messages are in each cluster
- Results sorted by count show biggest problems first
- Actual results: Database errors (count=3), Authentication messages (count=3), Payment issues (count=3)

---

### 3. Algorithm Comparison - "Different ways to find similarity"

**What you're doing**: Comparing three different algorithms that find similarity in different ways.

**Termlist (default - word order matters):**
```ppl
source=logs
| cluster message match=termlist t=0.5 showcount=true
| fields message, cluster_label, cluster_count
| sort -cluster_count
```
*"Database connection failed" vs "Connection failed database" = different clusters*

**Termset (word order ignored):**
```ppl
source=logs
| cluster message match=termset t=0.3 showcount=true
| fields message, cluster_label, cluster_count
| sort -cluster_count
```
*"Database connection failed" vs "Connection failed database" = same cluster*

**NGramset (fuzzy character matching):**
```ppl
source=logs
| cluster message match=ngramset t=0.4 showcount=true
| fields message, cluster_label, cluster_count
| sort -cluster_count
```
*"Database" vs "Databse" (typo) = same cluster*

**Why different algorithms**:
- **termlist**: Best for structured logs where word order matters
- **termset**: Good for finding conceptually similar messages regardless of phrasing
- **ngramset**: Handles typos and fuzzy matching (but slower on large fields)

---

### 4. Threshold Tuning - "How picky should the clustering be?"

**What you're doing**: Adjusting how similar messages need to be to group together.

**Strict clustering (t=0.9) - "Only group nearly identical messages":**
```ppl
source=logs
| cluster message t=0.9 showcount=true
| stats count() by cluster_label
| sort -count()
```

**Loose clustering (t=0.3) - "Group anything remotely similar":**
```ppl
source=logs
| cluster message t=0.3 showcount=true
| stats count() by cluster_label
| sort -count()
```

**Why threshold matters**:
- **High threshold (0.9)**: Very specific clusters, might miss related issues
- **Low threshold (0.3)**: Broader clusters, might group unrelated things
- **Sweet spot**: Usually 0.6-0.8 depending on your data

**What you'll see**:
- t=0.9 creates many small clusters (more precise)
- t=0.3 creates fewer large clusters (more general)

---

### 5. Label Only Mode - "Tag everything, don't lose data"

**What you're doing**: Adding cluster labels to ALL your data without removing any records.

```ppl
source=logs
| cluster message match=termset t=0.4 labelonly=true showcount=true
| fields timestamp, level, message, cluster_label, cluster_count
| sort cluster_label, timestamp
| head 15
```

**Default behavior**: Only keeps one representative message per cluster (deduplication)
**labelonly=true**: Keeps all messages but adds cluster information

**Why use this**:
- When you need to see all occurrences, not just examples
- For time-series analysis of clustered events
- When building dashboards that need complete data

---

### 6. Production Integration - "Make it work with my existing queries"

**What you're doing**: Using custom field names that fit your existing monitoring setup.

```ppl
source=logs
| cluster message labelfield=error_group countfield=group_size showcount=true
| fields level, message, error_group, group_size
| where level="ERROR"
| sort -group_size
```

**Why custom fields**:
- Integrates with existing dashboards and alerts
- Makes the output self-documenting
- Follows your team's naming conventions

---

## Real-World Use Cases

### 1. **Error Monitoring Dashboard**
```ppl
source=application_logs
| where level="ERROR"
| cluster message match=termset t=0.7 showcount=true
| sort -cluster_count
| head 10
```
*"Show me the top 10 most frequent error types this hour"*

### 2. **Security Analysis**
```ppl
source=security_logs
| cluster user_agent match=ngramset t=0.6 labelonly=true
| stats count() by cluster_label
| where count > 100
```
*"Find suspicious user agents that appear frequently (potential bot activity)"*

### 3. **Performance Investigation**
```ppl
source=slow_query_log
| cluster query_text match=termset t=0.8 showcount=true
| sort -cluster_count
| fields query_text, cluster_label, cluster_count
```
*"Group similar slow queries to identify performance patterns"*

## Expected Demo Results

With the provided sample data, you should see clusters like:

1. **Database Connection Issues** (cluster_count=3)
   - "Database connection failed: Connection timeout to postgresql://prod-db:5432"
   - "Database connection failed: Connection timeout to postgresql://backup-db:5432"
   - "DB connection error: timeout connecting to mysql://main-db:3306"

2. **User Authentication** (cluster_count=3)
   - Various successful login messages with different usernames

3. **Payment Problems** (cluster_count=3)
   - Different payment failure messages about insufficient funds

4. **And more clusters for**: Order processing, Rate limiting, Cache operations, File uploads, Backups

## Key Takeaways

1. **Automatic Pattern Discovery**: No need to know what patterns exist beforehand
2. **Scalable**: Works with millions of log entries
3. **Flexible**: Multiple algorithms and parameters for different use cases
4. **Production Ready**: Custom field names and integration options
5. **Actionable**: Immediately shows you what problems are most frequent

The cluster command transforms log analysis from "searching for known patterns" to "discovering unknown patterns automatically."