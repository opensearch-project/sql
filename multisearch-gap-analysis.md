# SPL Multisearch to PPL: Gap Analysis

## Executive Summary
This document provides a comprehensive gap analysis between Splunk's SPL `multisearch` command and OpenSearch PPL's current capabilities. PPL currently has an append command that provides sequential result combination, but lacks the multisearch functionality entirely. This analysis identifies the requirements for implementing SPL-compatible multisearch in PPL.

## SPL Multisearch Requirements

### Key SPL Characteristics
- **Command Type**: Generating command (must be first in search pipeline)
- **Minimum Requirements**: At least 2 subsearches required
- **Result Handling**: Final results sorted chronologically by timestamp
- **Search Type**: Only streaming operations allowed in subsearches

### SPL Syntax

```spl  
| multisearch [search <search1>] [search <search2>] ... [search <searchN>]  
```  

### SPL Example

```spl  
| multisearch   
    [search index=web_logs status=2* | eval query_type="good"]   
    [search index=web_logs status=2* OR status=5* | eval query_type="valid"]  
| stats count(eval(query_type="good")) as success_count  
```  

## Examples with Input and Output

### Example 1: Basic Success Rate Monitoring

#### Sample Input Data

**web_logs index data:**

```  
timestamp         | status | uri           | response_time  
2024-01-01 10:00 | 200    | /api/users    | 45  
2024-01-01 10:01 | 500    | /api/orders   | 1200  
2024-01-01 10:02 | 200    | /api/products | 23  
2024-01-01 10:03 | 503    | /api/users    | 5000  
2024-01-01 10:04 | 200    | /api/orders   | 67  
```  

#### SPL Multisearch Query

```spl  
| multisearch   
    [search index=web_logs status=2* | eval query_type="good"]   
    [search index=web_logs status=2* OR status=5* | eval query_type="valid"]  
| stats count(eval(query_type="good")) as success_count,  
 count(eval(query_type="valid")) as total_count
 ```  

#### Intermediate Result (After Multisearch)

```  
timestamp         | status | uri           | response_time | query_type  
2024-01-01 10:00 | 200    | /api/users    | 45           | good  
2024-01-01 10:00 | 200    | /api/users    | 45           | valid  
2024-01-01 10:01 | 500    | /api/orders   | 1200         | valid  
2024-01-01 10:02 | 200    | /api/products | 23           | good  
2024-01-01 10:02 | 200    | /api/products | 23           | valid  
2024-01-01 10:03 | 503    | /api/users    | 5000         | valid  
2024-01-01 10:04 | 200    | /api/orders   | 67           | good  
2024-01-01 10:04 | 200    | /api/orders   | 67           | valid  
```  

#### Final Output (After Stats)

```  
success_count | total_count  
3            | 5  
```  

### Example 2: Timestamp Interleaving Behavior Comparison

#### Input Data with Overlapping Timestamps

**Service A events:**

```  
timestamp         | level | message               | service  
2024-01-01 10:00 | INFO  | Service A started     | service_a  
2024-01-01 10:02 | ERROR | Service A database error | service_a  
2024-01-01 10:04 | INFO  | Service A recovered   | service_a  
2024-01-01 10:06 | WARN  | Service A memory warning | service_a  
```  

**Service B events:**

```  
timestamp         | level | message                | service  
2024-01-01 10:01 | INFO  | Service B started      | service_b  
2024-01-01 10:03 | ERROR | Service B connection failed | service_b  
2024-01-01 10:05 | INFO  | Service B reconnected  | service_b  
2024-01-01 10:07 | ERROR | Service B timeout error | service_b  
```  

#### SPL Multisearch Query

```spl  
| multisearch   
    [search source="service_a_overlap_test_data.json"]  
    [search source="service_b_overlap_test_data.json"]
```  

#### SPL Result (Timestamp Interleaving)

```  
_time                | level | message                | service  
2024-01-01 10:07:00 | ERROR | Service B timeout error | service_b  
2024-01-01 10:06:00 | WARN  | Service A memory warning | service_a  
2024-01-01 10:05:00 | INFO  | Service B reconnected  | service_b  
2024-01-01 10:04:00 | INFO  | Service A recovered   | service_a  
2024-01-01 10:03:00 | ERROR | Service B connection failed | service_b  
2024-01-01 10:02:00 | ERROR | Service A database error | service_a  
2024-01-01 10:01:00 | INFO  | Service B started      | service_b  
2024-01-01 10:00:00 | INFO  | Service A started     | service_a  
```  

#### PPL Append Query

```ppl  
source=service_a | append [ source=service_b ]
```  

#### PPL Result (Sequential Concatenation)

```  
timestamp         | level | message               | service  
2024-01-01 10:00 | INFO  | Service A started     | service_a  
2024-01-01 10:02 | ERROR | Service A database error | service_a  
2024-01-01 10:04 | INFO  | Service A recovered   | service_a  
2024-01-01 10:06 | WARN  | Service A memory warning | service_a  
2024-01-01 10:01 | INFO  | Service B started      | service_b  
2024-01-01 10:03 | ERROR | Service B connection failed | service_b  
2024-01-01 10:05 | INFO  | Service B reconnected  | service_b  
2024-01-01 10:07 | ERROR | Service B timeout error | service_b  
```  

**Key Finding**: SPL multisearch performs **timestamp-based interleaving** while PPL append uses **sequential concatenation**.

#### Technical Explanation: Different Result Combination Approaches

SPL multisearch and PPL append use fundamentally different approaches for combining results:

**PPL Append Implementation**:
- PPL append is explicitly documented as "the interface for union all columns in queries" (`Append.java:18`)
- The Calcite implementation uses `context.relBuilder.union(true)` which creates `LogicalUnion(all=[true])` (`CalciteRelNodeVisitor.java`)
- Test verification shows PPL append generates Spark SQL with `UNION ALL` syntax (`CalcitePPLAppendTest.java:32-39`)
- **CONFIRMED BEHAVIOR**: PPL append performs **sequential concatenation**, not timestamp interleaving
- **Runtime Testing**: Successfully tested with overlapping timestamps - Service A events (10:00, 10:02, 10:04, 10:06) followed by Service B events (10:01, 10:03, 10:05, 10:07)
- **Schema Issues Resolved**: Previous ClassCastException was due to mapping conflicts, resolved with explicit identical mappings

**SPL Multisearch Behavior**:
- **Timestamp-sorted results**: Final output shows all events ordered chronologically by `_time` field (newest to oldest)
- **Cross-source ordering**: Events from different sources intermixed based on timestamp values
- **Confirmed behavior**: Testing shows chronological ordering regardless of source execution order

**Conclusion**: SPL multisearch produces **timestamp-sorted results** across all sources. PPL append uses sequential concatenation (UNION ALL), creating a **major behavioral gap** between the two approaches.

### Example 3: Invalid Multisearch (Non-Streaming Command Error)

#### Attempting to Use Non-Streaming Commands

```spl  
| multisearch   
    [search index=logs | stats count() by host]  # ERROR: stats is non-streaming  
 [search index=metrics | where cpu > 80]
 ```  

#### Error Message

```  
Error: Non-streaming command 'stats' is not supported in multisearch subsearches. Commands like 'stats', 'sort', and other aggregating operations require all events before producing output, which conflicts with multisearch's streaming requirement.  
```  

#### Corrected Query

```spl  
| multisearch   
    [search index=logs | where error_count > 0]  # OK: where is streaming  
 [search index=metrics | where cpu > 80]       # OK: where is streaming| stats count() by host  # Aggregation moved outside multisearch  
```  


## Current PPL Capabilities

### Existing Related Commands

#### Append Command
- **Status**: Available only in Calcite engine
- **Syntax**: `source=index1 | append [search source=index2]`
- **Execution**: Sequential (primary search completes, then secondary search)
- **Result Combination**: Secondary results appended to bottom of primary results
- **Position**: Can be used anywhere in pipeline (streaming command)
- **Validation**: No restrictions on command types in subsearches

#### Search Command
- **Status**: Fully implemented in PPL
- **Functionality**: Basic search with filtering capabilities
- **Limitations**: Single data source per query


### Missing Functionality
- **No multisearch command**: PPL does not have any equivalent to SPL's multisearch
- **No streaming command validation**: PPL does not enforce streaming-only restrictions in subsearches

## Gap Analysis: SPL Multisearch vs PPL Append

### Core Differences

| Aspect | SPL Multisearch | PPL Append | Gap |  
|--------|----------------|------------|-----|  
| **Execution Model** | Unknown | Sequential execution | Unknown |
| **Result Combination** | Timestamp-sorted output | Sequential concatenation | Major |
| **Command Position** | Must be first | Can be anywhere | Minor |  
| **Subsearch Validation** | Only streaming commands allowed | No command restrictions | Major |  
| **Minimum Subsearches** | At least 2 required | Single subsearch allowed | Minor |  
| **Schema Handling** | Auto-unification across subsearches | Simple append operation | Minor |  

### Major Architectural Gaps

#### 1. No Multisearch Command
**Requirement**: PPL needs a multisearch command equivalent to SPL
- **Current State**: PPL has no multisearch command at all
- **Required**: Complete implementation from grammar to execution
- **Impact**: Fundamental missing functionality

#### 2. Streaming Command Validation
**SPL Requirement**: Only streaming commands allowed in multisearch subsearches
- **SPL Behavior**: Commands like `stats`, `sort`, `rare` are prohibited in subsearches
- **PPL Current State**: No such validation exists
- **Impact**: Critical for maintaining SPL compatibility

#### 3. Timestamp-Based Result Ordering
**SPL Requirement**: Results sorted chronologically by timestamp
- **SPL Behavior**: Final output shows events ordered by `_time` field across all sources
- **PPL Current State**: Append only provides sequential concatenation
- **Impact**: Core behavioral difference preventing SPL compatibility


## Streaming vs Non-Streaming Commands

### What is a Streaming Command?
Streaming commands operate on each event as it is returned by a search, processing events independently without needing to see the entire dataset.

**Streaming Commands**: eval, where, fields, search, head, limit, reverse, rename, regex, rex, parse, expand, flatten, fillnull

**Non-Streaming Commands**: stats, sort, bin, timechart, rare, top, window, trendline, join, lookup



## Conclusion

### Status Summary
**Major Architectural Gaps** (REQUIRES IMPLEMENTATION):
- **No multisearch command exists in PPL**: Complete command missing from grammar to execution
- **No streaming command validation**: PPL lacks framework to restrict non-streaming commands in subsearches  
- **No timestamp-based result ordering**: PPL append provides sequential concatenation instead of timestamp sorting

**Critical Behavioral Gaps**:
- **PPL append uses sequential concatenation**: Results from first source followed by results from second source
- **SPL multisearch produces timestamp-sorted results**: Events ordered chronologically across all sources by `_time` field
- **No timestamp-based ordering in PPL**: PPL append lacks the core SPL multisearch behavior
- **Append command behavioral gap**: Even the existing append command doesn't match SPL multisearch semantics


## Appendix

### A. Streaming vs Non-Streaming Commands: Detailed Examples

#### Streaming Commands (Event-by-Event Processing)

**Characteristics**: Process each event independently without needing other events.

**1. eval Command**

```ppl  
| eval full_name = first_name + " " + last_name  
```  
- **Why Streaming**: Each event's `full_name` is calculated using only fields from that same event
- **No Dependencies**: Doesn't need to see other events to perform the calculation

**2. where/filter Command**

```ppl  
| where age > 30 AND status = "active"  
```  
- **Why Streaming**: Each event is evaluated against the criteria independently
- **No Dependencies**: Decision to include/exclude an event depends only on that event's fields

**3. fields/project Command**

```ppl  
| fields name, age, department  
```  
- **Why Streaming**: Selects specified fields from each event independently
- **No Dependencies**: Field selection doesn't require comparing across events

**4. rename Command**

```ppl  
| rename old_field as new_field  
```  
- **Why Streaming**: Renames fields in each event independently
- **No Dependencies**: Field renaming doesn't depend on other events

#### Non-Streaming Commands (Require All Events)

**Characteristics**: Must see the complete dataset before producing output.

**1. stats/aggregation Command**

```ppl  
| stats count() by department, avg(salary) by department  
```  
- **Why Non-Streaming**: Must see ALL events to calculate count and average
- **Dependencies**: Cannot produce final count until all department events are processed

**2. sort Command**

```ppl  
| sort age desc, name asc  
```  
- **Why Non-Streaming**: Must see ALL events to determine correct ordering
- **Dependencies**: Cannot place any event in final position until all events are compared

**3. rare/top Command**

```ppl  
| rare department  
```  
- **Why Non-Streaming**: Must see ALL events to determine which departments are least common
- **Dependencies**: Cannot identify "rare" values until complete frequency analysis

### B. SPL Multisearch Behavior Analysis

Based on real SPL testing with overlapping timestamps:

#### Confirmed SPL Behavior
- **Timestamp-sorted output**: Final results show all events ordered chronologically by `_time` field (newest to oldest)
- **Cross-source ordering**: Events from different sources intermixed based on timestamp values
- **NOT simple concatenation**: Results ordered by `_time` field, not source execution order
- **Observable behavior**: Testing confirms chronological ordering across all subsearch results
- **Major behavioral difference**: Timestamp-based ordering that PPL append lacks

#### Implementation Requirements
1. **Timestamp-based result ordering**: Implement chronological sorting by `_time` field across all subsearch results
2. **Schema unification**: Project all subsearches to common field set (already implemented in append)
3. **Streaming validation**: Enforce streaming-only commands in subsearches (new requirement)
4. **Multiple subsearch syntax**: Support `[search ...] [search ...]` syntax (new requirement)

**Key Insight**: SPL multisearch produces **timestamp-sorted output** across all sources. PPL will need to implement chronological ordering to match this behavior, which requires adding timestamp sorting capability to the existing append functionality.

## Testing Results and Limitations

### Successfully Verified SPL Behavior

✅ **SPL Multisearch with Overlapping Timestamps**:
- Performs timestamp-based interleaving when timestamps overlap across datasets
- Results ordered chronologically by `_time` field (newest to oldest in test case)
- Behavior confirmed with real SPL environment using overlap test data

### Confirmed PPL Append Behavior

✅ **PPL Append Command**: 
- **Status**: Successfully tested with overlapping timestamps
- **Behavior**: Sequential concatenation - first source results followed by second source results
- **Schema Handling**: Works correctly with identical field mappings across indices
- **Implementation**: Uses standard `UNION ALL` semantics without timestamp ordering
- **Gap Identified**: Does not perform timestamp-based interleaving like SPL multisearch

✅ **Append Command Limitations**:
- **No timestamp ordering**: Events not sorted chronologically across sources
- **Sequential only**: Cannot achieve SPL multisearch timestamp interleaving behavior
- **Major behavioral difference**: Fundamental incompatibility with SPL multisearch semantics

### Gap Analysis Confidence Level

- **SPL Behavior**: **High confidence** - verified with real testing showing timestamp-sorted output
- **PPL Behavior**: **High confidence** - confirmed sequential concatenation with runtime testing
- **Implementation Strategy**: **Clear requirements** - need timestamp-based ordering mechanism

### Recommendations

1. **Implement multisearch with timestamp sorting**: Create new command that combines PPL append logic with chronological ordering
2. **Extend append command**: Add optional timestamp ordering mode to achieve multisearch behavior  
3. **Leverage existing append infrastructure**: Reuse PPL append's schema unification and result combination logic
4. **Add timestamp-based sorting**: Implement chronological ordering by `_time` field across all subsearch results
5. **Performance considerations**: Timestamp sorting requires collecting all results before ordering

### Evidence of PPL Capability Gap

**Confirmed behavioral differences**:
- PPL append: Service A (10:00, 10:02, 10:04, 10:06) then Service B (10:01, 10:03, 10:05, 10:07)
- SPL multisearch: Chronological order (10:00, 10:01, 10:02, 10:03, 10:04, 10:05, 10:06, 10:07)
- **Missing functionality**: PPL lacks timestamp-based event merging capability