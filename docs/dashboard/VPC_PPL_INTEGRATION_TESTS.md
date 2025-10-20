# VPC PPL Integration Tests

This document describes the integration tests created for VPC (Virtual Private Cloud) PPL (Piped Processing Language) dashboard queries to ensure they don't break the SQL plugin.

## Overview

The VPC PPL integration tests validate that VPC-related PPL queries can be parsed and executed without causing errors in the OpenSearch SQL plugin. These tests are designed to catch any regressions that might break VPC dashboard functionality.

## Test Files Created

### 1. VpcPplDashboardIT.java
**Location:** `/integ-test/src/test/java/org/opensearch/sql/ppl/VpcPplDashboardIT.java`

This is the main integration test class that contains test methods for all VPC PPL queries. Each test method validates a specific VPC query pattern:

- `testTotalRequests()` - Tests basic count aggregation
- `testTotalFlowsByActions()` - Tests count by VPC action field
- `testRequestHistory()` - Tests count by timestamp field
- `testRequestsByDirection()` - Tests count by flow direction with sorting
- `testBytes()` - Tests sum of bytes by timestamp
- `testPackets()` - Tests sum of packets by timestamp
- `testTopSourceAwsServices()` - Tests count by source AWS service
- `testTopDestinationAwsServices()` - Tests count by destination AWS service
- `testRequestsByDirectionMetric()` - Tests count requests by flow direction
- `testTopDestinationBytes()` - Tests sum bytes by destination address with sorting
- `testTopSourceBytes()` - Tests sum bytes by source address with sorting
- `testTopSources()` - Tests count requests by source address
- `testTopDestinations()` - Tests count requests by destination address
- `testTopTalkersByPackets()` - Tests sum packets by source address (top talkers)
- `testTopDestinationsByPackets()` - Tests sum packets by destination address
- `testHeatMap()` - Tests count by both destination and source addresses
- `testVpcLiveRawSearch()` - Tests field selection with many VPC fields
- `testFlow()` - Tests flow analysis with large result set handling

### 2. Test Data Files

#### vpc_logs.json
**Location:** `/integ-test/src/test/resources/vpc_logs.json`

Sample VPC log data in OpenSearch bulk format containing realistic VPC flow log entries with fields like:
- `@timestamp`, `start_time`, `end_time`
- `aws.vpc.srcaddr`, `aws.vpc.dstaddr`
- `aws.vpc.srcport`, `aws.vpc.dstport`
- `aws.vpc.action`, `aws.vpc.flow-direction`
- `aws.vpc.bytes`, `aws.vpc.packets`
- `aws.vpc.pkt-src-aws-service`, `aws.vpc.pkt-dst-aws-service`
- Various other VPC-related fields

#### vpc_logs_index_mapping.json
**Location:** `/integ-test/src/test/resources/indexDefinitions/vpc_logs_index_mapping.json`

OpenSearch index mapping for VPC logs with proper field types:
- Date fields for timestamps
- IP fields for addresses
- Keyword fields for categorical data
- Long fields for numeric data like bytes and packets

## VPC Queries Tested

The integration tests cover all the VPC PPL queries from the dashboard requirements:

1. **Basic Count Query:**
   ```
   source=vpc_logs | stats count()
   ```

2. **Count by Action:**
   ```
   source=vpc_logs | stats count() as Count by `aws.vpc.action` | head 5
   ```

3. **Count by Timestamp:**
   ```
   source=vpc_logs | stats count() by `@timestamp`
   ```

4. **Flow Direction Analysis:**
   ```
   source=vpc_logs | stats count() as flow_direction by `aws.vpc.flow-direction` | sort - flow_direction | head 5
   ```

5. **Bytes and Packets Aggregation:**
   ```
   source=vpc_logs | stats sum(`aws.vpc.bytes`) by `@timestamp`
   source=vpc_logs | stats sum(`aws.vpc.packets`) by `@timestamp`
   ```

6. **AWS Service Analysis:**
   ```
   source=vpc_logs | stats count() as `src-aws-service` by `aws.vpc.pkt-src-aws-service` | sort - `src-aws-service` | head 10
   ```

7. **Top Sources/Destinations by Bytes:**
   ```
   source=vpc_logs | stats sum(`aws.vpc.bytes`) as Bytes by `aws.vpc.srcaddr` | sort - Bytes | head 10
   ```

8. **Top Sources/Destinations by Packets:**
   ```
   source=vpc_logs | stats sum(`aws.vpc.packets`) as Packets by `aws.vpc.srcaddr` | sort - Packets | head 10
   ```

9. **Heat Map Analysis:**
   ```
   source=vpc_logs | stats count() as Count by `aws.vpc.dstaddr`, `aws.vpc.srcaddr` | sort - Count | head 20
   ```

10. **Field Selection (Live Raw Search):**
    ```
    source=vpc_logs | fields `@timestamp`, `start_time`, `interval_start_time`, `end_time`, `aws.vpc.srcport`, `aws.vpc.pkt-src-aws-service`, `aws.vpc.srcaddr` | sort - `@timestamp`
    ```

## Test Strategy

The tests use actual VPC test data loaded into a test index to verify end-to-end functionality. Each test validates:

1. **Query Execution:** Ensures queries execute successfully without parsing errors
2. **Schema Validation:** Verifies correct field types and names in results
3. **Data Validation:** Confirms expected result counts and values

## Running the Tests

To run the VPC PPL integration tests:

```bash
# Compile the tests
./gradlew :integ-test:compileTestJava

# Run all PPL integration tests (includes VPC tests)
./gradlew :integ-test:test --tests "*PPL*"

# Run only VPC PPL tests
./gradlew :integ-test:test --tests "*VpcPplDashboardIT*"
```

## Expected Behavior

- **All queries** should execute successfully and return valid results
- **No parsing errors** should occur for any of the VPC PPL query patterns
- **Schema validation** should pass with correct field types
- **Data validation** should confirm expected result counts from test data

## Benefits

These integration tests provide:

1. **Regression Protection:** Ensures VPC dashboard queries continue to work as the SQL plugin evolves
2. **Query Validation:** Validates that all VPC PPL query patterns are syntactically correct
3. **Field Compatibility:** Ensures VPC field names and types are properly handled
4. **Performance Baseline:** Provides a baseline for VPC query performance testing
5. **Documentation:** Serves as living documentation of supported VPC query patterns

## Maintenance

When adding new VPC query patterns to dashboards:

1. Add the new query pattern to the test class
2. Update test data if new fields are required
3. Update the index mapping if new field types are needed
4. Run the tests to ensure compatibility

This ensures that all VPC dashboard functionality remains stable and functional.