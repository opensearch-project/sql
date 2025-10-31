# VPC Flow Logs PPL Integration Tests

This document describes the integration tests created for VPC Flow Logs PPL (Piped Processing Language) dashboard queries to ensure they don't break the SQL plugin.

## Overview

The VPC Flow Logs PPL integration tests validate that VPC Flow Logs-related PPL queries can be parsed and executed without causing errors in the OpenSearch SQL plugin. These tests are designed to catch any regressions that might break VPC Flow Logs dashboard functionality.

## Test Files Created

### 1. VpcFlowLogsPplDashboardIT.java
**Location:** `/integ-test/src/test/java/org/opensearch/sql/ppl/dashboard/VpcFlowLogsPplDashboardIT.java`

This is the main integration test class that contains test methods for all VPC Flow Logs PPL queries. Each test method validates a specific VPC query pattern:

- `testTotalRequests()` - Tests basic count aggregation
- `testTotalFlowsByActions()` - Tests count by action field with sorting
- `testFlowsOvertime()` - Tests count by span function over time
- `testRequestsByDirection()` - Tests count by flow direction with sorting
- `testBytesTransferredOverTime()` - Tests sum of bytes by span function over time
- `testPacketsTransferredOverTime()` - Tests sum of packets by span function over time
- `testTopSourceAwsServices()` - Tests count by source AWS service
- `testTopDestinationAwsServices()` - Tests count by destination AWS service
- `testTopDestinationByBytes()` - Tests sum bytes by destination address with sorting
- `testTopTalkersByBytes()` - Tests sum bytes by source address with sorting
- `testTopTalkersByPackets()` - Tests sum packets by source address with sorting
- `testTopDestinationsByPackets()` - Tests sum packets by destination address with sorting
- `testTopTalkersByIPs()` - Tests count by source address with sorting
- `testTopDestinationsByIPs()` - Tests count by destination address with sorting
- `testTopTalkersByHeatMap()` - Tests count by both destination and source addresses

### 2. Test Data Files

#### vpc_logs.json
**Location:** `/integ-test/src/test/resources/vpc_logs.json`

Sample VPC flow log data in OpenSearch bulk format containing 3 test records with fields:
- `start`, `end` - Unix timestamp fields
- `srcaddr`, `dstaddr` - Source and destination IP addresses
- `srcport`, `dstport` - Source and destination ports
- `action` - VPC flow action (ACCEPT/REJECT)
- `flow-direction` - Flow direction (ingress/egress)
- `bytes`, `packets` - Traffic volume metrics
- `pkt-src-aws-service`, `pkt-dst-aws-service` - AWS service identifiers
- Various other VPC flow log fields

#### vpc_logs_index_mapping.json
**Location:** `/integ-test/src/test/resources/indexDefinitions/vpc_logs_index_mapping.json`

OpenSearch index mapping for VPC flow logs with proper field types:
- Long fields for timestamps and numeric data
- IP fields for addresses
- Keyword fields for categorical data
- Integer fields for ports and protocol numbers

## VPC Flow Logs Queries Tested

The integration tests cover the following VPC Flow Logs PPL queries:

1. **Total Requests:**
   ```
   source=vpc_flow_logs | stats count()
   ```

2. **Total Flows by Actions:**
   ```
   source=vpc_flow_logs | STATS count() as Count by action | SORT - Count | HEAD 5
   ```

3. **Flows Over Time:**
   ```
   source=vpc_flow_logs | STATS count() by span(`start`, 30d)
   ```

4. **Requests by Direction:**
   ```
   source=vpc_flow_logs | STATS count() as Count by `flow-direction` | SORT - Count | HEAD 5
   ```

5. **Bytes Transferred Over Time:**
   ```
   source=vpc_flow_logs | STATS sum(bytes) by span(`start`, 30d)
   ```

6. **Packets Transferred Over Time:**
   ```
   source=vpc_flow_logs | STATS sum(packets) by span(`start`, 30d)
   ```

7. **Top Source AWS Services:**
   ```
   source=vpc_flow_logs | STATS count() as Count by `pkt-src-aws-service` | SORT - Count | HEAD 10
   ```

8. **Top Destination AWS Services:**
   ```
   source=vpc_flow_logs | STATS count() as Count by `pkt-dst-aws-service` | SORT - Count | HEAD 10
   ```

9. **Top Destination by Bytes:**
   ```
   source=vpc_flow_logs | stats sum(bytes) as Bytes by dstaddr | sort - Bytes | head 10
   ```

10. **Top Talkers by Bytes:**
    ```
    source=vpc_flow_logs | stats sum(bytes) as Bytes by srcaddr | sort - Bytes | head 10
    ```

11. **Top Talkers by Packets:**
    ```
    source=vpc_flow_logs | stats sum(packets) as Packets by srcaddr | sort - Packets | head 10
    ```

12. **Top Destinations by Packets:**
    ```
    source=vpc_flow_logs | stats sum(packets) as Packets by dstaddr | sort - Packets | head 10
    ```

13. **Top Talkers by IPs:**
    ```
    source=vpc_flow_logs | STATS count() as Count by srcaddr | SORT - Count | HEAD 10
    ```

14. **Top Destinations by IPs:**
    ```
    source=vpc_flow_logs | stats count() as Requests by dstaddr | sort - Requests | head 10
    ```

15. **Top Talkers by Heat Map:**
    ```
    source=vpc_flow_logs | stats count() as Count by dstaddr, srcaddr | sort - Count | head 100
    ```

## Test Strategy

The tests use actual VPC test data loaded into a test index to verify end-to-end functionality. Each test validates:

1. **Query Execution:** Ensures queries execute successfully without parsing errors
2. **Schema Validation:** Verifies correct field types and names in results
3. **Data Validation:** Confirms expected result counts and values

## Running the Tests

To run the VPC Flow Logs PPL integration tests:

```bash
# Compile the tests
./gradlew :integ-test:compileTestJava

# Run all PPL integration tests (includes VPC tests)
./gradlew :integ-test:integTest --tests "*PPL*"

# Run only VPC Flow Logs PPL tests
./gradlew :integ-test:integTest --tests "*VpcFlowLogsPplDashboardIT*"

# Run a specific test method
./gradlew :integ-test:integTest --tests "VpcFlowLogsPplDashboardIT.testBytesTransferredOverTime"
```

## Expected Behavior

- **All queries** should execute successfully and return valid results
- **No parsing errors** should occur for any of the VPC PPL query patterns
- **Schema validation** should pass with correct field types
- **Data validation** should confirm expected result counts from test data

## Benefits

These integration tests provide:

1. **Regression Protection:** Ensures VPC Flow Logs dashboard queries continue to work as the SQL plugin evolves
2. **Span Function Validation:** Validates time-based aggregation functionality critical for dashboards
3. **Query Validation:** Validates that all VPC PPL query patterns are syntactically correct
4. **Field Compatibility:** Ensures VPC Flow Logs field names and types are properly handled
5. **Documentation:** Serves as living documentation of supported VPC Flow Logs query patterns

## Maintenance

When adding new VPC Flow Logs query patterns to dashboards:

1. Add the new query pattern to the `VpcFlowLogsPplDashboardIT` test class
2. Update test data in `vpc_logs.json` if new fields are required
3. Update the index mapping in `vpc_logs_index_mapping.json` if new field types are needed
4. Run the tests to ensure compatibility

This ensures that all VPC Flow Logs dashboard functionality remains stable and functional.