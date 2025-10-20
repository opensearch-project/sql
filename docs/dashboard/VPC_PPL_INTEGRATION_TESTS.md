# VPC PPL Integration Tests

This document describes the integration tests created for VPC (Virtual Private Cloud) PPL (Piped Processing Language) dashboard queries to ensure they don't break the SQL plugin.

## Overview

The VPC PPL integration tests validate that VPC-related PPL queries can be parsed and executed without causing errors in the OpenSearch SQL plugin. These tests are designed to catch any regressions that might break VPC dashboard functionality.

## Test Files Created

### 1. VpcPplDashboardIT.java
**Location:** `/integ-test/src/test/java/org/opensearch/sql/ppl/VpcPplDashboardIT.java`

This is the main integration test class that contains test methods for all VPC PPL queries. Each test method validates a specific VPC query pattern:

- `testBasicCountQuery()` - Tests basic count aggregation
- `testCountByActionQuery()` - Tests count by VPC action field
- `testCountByTimestampQuery()` - Tests count by timestamp field
- `testCountByFlowDirectionQuery()` - Tests count by flow direction with sorting
- `testSumBytesByTimestampQuery()` - Tests sum of bytes by timestamp
- `testSumPacketsByTimestampQuery()` - Tests sum of packets by timestamp
- `testCountBySrcAwsServiceQuery()` - Tests count by source AWS service
- `testCountByDstAwsServiceQuery()` - Tests count by destination AWS service
- `testCountRequestsByFlowDirectionQuery()` - Tests count requests by flow direction
- `testSumBytesByDstAddrQuery()` - Tests complex query with eval and fields commands
- `testSumBytesBySrcAddrQuery()` - Tests sum bytes by source address
- `testCountRequestsBySrcAddrQuery()` - Tests count requests by source address
- `testCountRequestsByDstAddrQuery()` - Tests count requests by destination address
- `testCountByDstAndSrcAddrQuery()` - Tests count by both destination and source addresses
- `testFieldsSelectionQuery()` - Tests field selection with many VPC fields
- `testCountByDstAndSrcAddrLargeResultQuery()` - Tests large result set handling
- `testVpcQueryPatternsWithRealData()` - Tests with actual VPC test data
- `testVpcBytesAggregation()` - Tests bytes aggregation patterns
- `testVpcComplexQueryWithEvalAndFields()` - Tests complex query patterns

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
   SOURCE = [ds:logGroup] | STATS count()
   ```

2. **Count by Action:**
   ```
   SOURCE = [ds:logGroup] | STATS count() as `Count` by `aws.vpc.action` | HEAD 5
   ```

3. **Count by Timestamp:**
   ```
   SOURCE = [ds:logGroup] | stats count() by `@timestamp`
   ```

4. **Flow Direction Analysis:**
   ```
   SOURCE = [ds:logGroup] | STATS count() as flow_direction by `aws.vpc.flow-direction` | SORT - flow_direction | HEAD 5
   ```

5. **Bytes and Packets Aggregation:**
   ```
   SOURCE = [ds:logGroup] | STATS sum(`aws.vpc.bytes`) by `@timestamp`
   SOURCE = [ds:logGroup] | STATS sum(`aws.vpc.packets`) by `@timestamp`
   ```

6. **AWS Service Analysis:**
   ```
   SOURCE = [ds:logGroup] | STATS count() as src-aws-service by `aws.vpc.pkt-src-aws-service` | SORT - src-aws-service | HEAD 10
   SOURCE = [ds:logGroup] | STATS count() as dst-aws-service by `aws.vpc.pkt-dst-aws-service` | SORT - dst-aws-service | HEAD 10
   ```

7. **Complex Queries with EVAL and FIELDS:**
   ```
   source = [ds:logGroup] | stats sum(`aws.vpc.bytes`) as Bytes by `aws.vpc.dstaddr` | eval Address = `aws.vpc.dstaddr` | fields Address, Bytes | sort - Bytes | head 10
   ```

8. **Field Selection:**
   ```
   SOURCE = [ds:logGroup] | FIELDS `@timestamp`, `start_time`, ... | SORT - `@timestamp`
   ```

## Test Strategy

The tests use a dual approach:

1. **Datasource Query Testing:** Tests the original queries with `[ds:logGroup]` datasource syntax to ensure parsing works correctly. These are expected to fail gracefully with datasource-related errors, but should not have parsing errors.

2. **Real Data Testing:** Tests similar query patterns using actual VPC test data loaded into a test index to verify end-to-end functionality.

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

- **Datasource queries** should fail gracefully with datasource-related error messages, not parsing errors
- **Real data queries** should execute successfully and return valid results
- **No parsing errors** should occur for any of the VPC PPL query patterns
- **Schema validation** should pass for queries that execute successfully

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