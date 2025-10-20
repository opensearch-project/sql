# WAF PPL Integration Tests

This document describes the integration tests created for WAF (Web Application Firewall) PPL (Piped Processing Language) dashboard queries to ensure they don't break the SQL plugin.

## Overview

The WAF PPL integration tests validate that WAF-related PPL queries can be parsed and executed without causing errors in the OpenSearch SQL plugin. These tests are designed to catch any regressions that might break WAF dashboard functionality.

## Test Files Created

### 1. WafPplDashboardIT.java
**Location:** `/integ-test/src/test/java/org/opensearch/sql/ppl/WafPplDashboardIT.java`

This is the main integration test class that contains test methods for all WAF PPL queries. Each test method validates a specific WAF query pattern:

- `testTotalRequests()` - Tests basic count aggregation
- `testRequestsHistory()` - Tests count by timestamp and action
- `testRequestsToWebACLs()` - Tests count by WebACL ID with sorting
- `testRequestsByTerminatingRules()` - Tests count by terminating rule ID (legacy test)
- `testSources()` - Tests count by HTTP source ID
- `testTopClientIPs()` - Tests count by client IP addresses
- `testTopCountries()` - Tests count by country field
- `testTopTerminatingRules()` - Tests count by terminating rule ID with sorting
- `testTopRequestURIs()` - Tests count by URI with sorting
- `testTotalBlockedRequests()` - Tests conditional aggregation for blocked requests
- `testWafRules()` - Tests top WAF rules by count

### 2. Test Data Files

#### waf_logs.json
**Location:** `/integ-test/src/test/resources/waf_logs.json`

Sample WAF log data in OpenSearch bulk format containing realistic WAF log entries with fields like:
- `@timestamp`
- `aws.waf.webaclId`, `aws.waf.terminatingRuleId`
- `aws.waf.action`, `aws.waf.httpSourceId`
- `aws.waf.httpRequest.clientIp`, `aws.waf.httpRequest.country`
- `aws.waf.httpRequest.uri`, `aws.waf.httpRequest.method`
- Nested `httpRequest` object structure

#### waf_logs_index_mapping.json
**Location:** `/integ-test/src/test/resources/indexDefinitions/waf_logs_index_mapping.json`

OpenSearch index mapping for WAF logs with proper field types:
- Date fields for timestamps
- Keyword fields for categorical data
- Nested object mapping for `httpRequest`
- Both raw and `aws.waf` prefixed fields

## WAF Queries Tested

The integration tests cover all the WAF PPL queries from the dashboard requirements:

1. **Total Requests:**
   ```
   source=waf_logs | stats count() as Count
   ```

2. **Requests History:**
   ```
   source=waf_logs | stats count() by `@timestamp`, `aws.waf.action`
   ```

3. **Requests to WebACLs:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.webaclId` | sort - Count | head 10
   ```

4. **Requests by Terminating Rules:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.terminatingRuleId` | sort - Count | head 5
   ```

5. **Sources Analysis:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.httpSourceId` | sort - Count | head 5
   ```

6. **Top Client IPs:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.httpRequest.clientIp` | sort - Count | head 10
   ```

7. **Top Countries:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.httpRequest.country` | sort - Count
   ```

8. **Top Terminating Rules:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.terminatingRuleId` | sort - Count | head 10
   ```

9. **Top Request URIs:**
   ```
   source=waf_logs | stats count() as Count by `aws.waf.httpRequest.uri` | sort - Count | head 10
   ```

10. **Total Blocked Requests:**
    ```
    source=waf_logs | stats sum(if(`aws.waf.action` = 'BLOCK', 1, 0)) as Count
    ```

11. **WAF Rules Analysis:**
    ```
    source=waf_logs | stats count() as Count by `aws.waf.terminatingRuleId` | sort - Count | head 5
    ```

## Test Strategy

The tests use the Calcite engine and validate:

1. **Schema Validation:** Ensures proper field types and names in query results
2. **Data Validation:** Uses `verifyDataRows` to validate expected result counts
3. **Real Data Testing:** Tests query patterns using actual WAF test data loaded into a test index

## Running the Tests

To run the WAF PPL integration tests:

```bash
# Compile the tests
./gradlew :integ-test:compileTestJava

# Run all PPL integration tests (includes WAF tests)
./gradlew :integ-test:test --tests "*PPL*"

# Run only WAF PPL tests
./gradlew :integ-test:test --tests "*WafPplDashboardIT*"
```

## Expected Behavior

- **All queries** should execute successfully and return valid results
- **Schema validation** should pass with correct field types
- **Data validation** should confirm expected result counts (3 records in test data)
- **No parsing errors** should occur for any of the WAF PPL query patterns

## Benefits

These integration tests provide:

1. **Regression Protection:** Ensures WAF dashboard queries continue to work as the SQL plugin evolves
2. **Query Validation:** Validates that all WAF PPL query patterns are syntactically correct
3. **Field Compatibility:** Ensures WAF field names and nested structures are properly handled
4. **Calcite Engine Testing:** Validates WAF queries work correctly with the Calcite engine
5. **Documentation:** Serves as living documentation of supported WAF query patterns

## Maintenance

When adding new WAF query patterns to dashboards:

1. Add the new query pattern to the test class
2. Update test data if new fields are required
3. Update the index mapping if new field types are needed
4. Run the tests to ensure compatibility

This ensures that all WAF dashboard functionality remains stable and functional.