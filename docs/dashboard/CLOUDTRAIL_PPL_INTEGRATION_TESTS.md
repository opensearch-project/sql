# CloudTrail PPL Integration Tests

This document describes the integration tests created for CloudTrail PPL (Piped Processing Language) dashboard queries to ensure they don't break the SQL plugin.

## Overview

The CloudTrail PPL integration tests validate that CloudTrail-related PPL queries can be parsed and executed without causing errors in the OpenSearch SQL plugin. These tests are designed to catch any regressions that might break CloudTrail dashboard functionality.

## Test Files Created

### 1. CloudTrailPplDashboardIT.java
**Location:** `/integ-test/src/test/java/org/opensearch/sql/ppl/CloudTrailPplDashboardIT.java`

This is the main integration test class that contains test methods for all CloudTrail PPL queries. Each test method validates a specific CloudTrail query pattern:

- `testTotalEventsCount()` - Tests basic count aggregation for total events
- `testEventsOverTime()` - Tests count by timestamp for event history
- `testEventsByAccountIds()` - Tests count by account ID with null filtering
- `testEventsByCategory()` - Tests count by event category with sorting
- `testEventsByRegion()` - Tests count by AWS region with sorting
- `testTopEventAPIs()` - Tests count by event name (API calls)
- `testTopServices()` - Tests count by event source (AWS services)
- `testTopSourceIPs()` - Tests count by source IP addresses
- `testTopUsersGeneratingEvents()` - Tests complex user analysis with multiple fields
- `testS3AccessDenied()` - Tests S3 access denied events with filtering
- `testS3Buckets()` - Tests S3 bucket analysis
- `testTopS3ChangeEvents()` - Tests S3 change events excluding read operations
- `testEC2ChangeEventCount()` - Tests EC2 instance change events
- `testErrorEvents()` - Tests field selection for error event analysis

### 2. Test Data Files

#### cloudtrail_logs.json
**Location:** `/integ-test/src/test/resources/cloudtrail_logs.json`

Sample CloudTrail log data in OpenSearch bulk format containing realistic CloudTrail log entries with fields like:
- `@timestamp` - Event timestamp
- `aws.cloudtrail.eventName` - API operation name
- `aws.cloudtrail.eventSource` - AWS service source
- `aws.cloudtrail.eventCategory` - Event category (Management/Data)
- `aws.cloudtrail.awsRegion` - AWS region
- `aws.cloudtrail.sourceIPAddress` - Source IP address
- `aws.cloudtrail.userIdentity.*` - User identity information
- `aws.cloudtrail.requestParameters.*` - Request parameters
- `errorCode` - Error code for failed operations

#### cloudtrail_logs_index_mapping.json
**Location:** `/integ-test/src/test/resources/indexDefinitions/cloudtrail_logs_index_mapping.json`

OpenSearch index mapping for CloudTrail logs with proper field types:
- Date fields for timestamps
- IP fields for source addresses
- Keyword fields for categorical data
- Nested object mapping for complex CloudTrail structure

## CloudTrail Queries Tested

The integration tests cover all the CloudTrail PPL queries from the dashboard requirements:

1. **Total Events Count:**
   ```
   source=cloudtrail_logs | stats count() as `Event Count`
   ```

2. **Events Over Time/Event History:**
   ```
   source=cloudtrail_logs | stats count() by `@timestamp`
   ```

3. **Events by Account IDs:**
   ```
   source=cloudtrail_logs | where isnotnull(`userIdentity.accountId`) | stats count() as Accounts by `userIdentity.accountId` | sort - Accounts | head 10
   ```

4. **Events by Category:**
   ```
   source=cloudtrail_logs | stats count() as Category by `eventCategory` | sort - Category | head 5
   ```

5. **Events by Region:**
   ```
   source=cloudtrail_logs | stats count() as Region by `awsRegion` | sort - Region | head 10
   ```

6. **Top 10 Event APIs:**
   ```
   source=cloudtrail_logs | stats count() as Count by `eventName` | sort - Count | head 10
   ```

7. **Top 10 Services:**
   ```
   source=cloudtrail_logs | stats count() as Count by `eventSource` | sort - Count | head 10
   ```

8. **Top 10 Source IPs:**
   ```
   source=cloudtrail_logs | stats count() as Count by `sourceIPAddress` | sort - Count | head 10
   ```

9. **Top 10 Users Generating Events:**
   ```
   source=cloudtrail_logs | where isnotnull(`userIdentity.sessionContext.sessionIssuer.userName`) and isnotnull(`userIdentity.sessionContext.sessionIssuer.arn`) and isnotnull(`userIdentity.accountId`) and isnotnull(`userIdentity.sessionContext.sessionIssuer.type`) | stats count() as Count by `userIdentity.sessionContext.sessionIssuer.userName`, `userIdentity.accountId`, `userIdentity.sessionContext.sessionIssuer.type`, `userIdentity.sessionContext.sessionIssuer.arn` | sort - Count | head 10
   ```

10. **S3 Access Denied:**
    ```
    source=cloudtrail_logs | where `eventSource` like 's3%' and `errorCode`='AccessDenied' | stats count() as Count
    ```

11. **S3 Buckets:**
    ```
    source=cloudtrail_logs | where `eventSource` like 's3%' and isnotnull(`requestParameters.bucketName`) | stats count() as Bucket by `requestParameters.bucketName` | sort - Bucket | head 10
    ```

12. **Top S3 Change Events:**
    ```
    source=cloudtrail_logs | where `eventSource` = 's3.amazonaws.com' and isnotnull(`requestParameters.bucketName`) and not like(`eventName`, 'Get%') and not like(`eventName`, 'Describe%') and not like(`eventName`, 'List%') and not like(`eventName`, 'Head%') | stats count() as Count by `eventName`, `requestParameters.bucketName` | sort - Count | head 10
    ```

13. **EC2 Change Event Count:**
    ```
    source=cloudtrail_logs | where `eventSource` = 'ec2.amazonaws.com' and (`eventName` = 'RunInstances' or `eventName` = 'TerminateInstances' or `eventName` = 'StopInstances') and not like(`eventName`, 'Get%') and not like(`eventName`, 'Describe%') and not like(`eventName`, 'List%') and not like(`eventName`, 'Head%') | stats count() as Count by `eventName` | sort - Count
    ```

14. **Error Events:**
    ```
    source=cloudtrail_logs | fields `@timestamp`, `errorCode`, `eventName`, `eventSource`, `userIdentity.sessionContext.sessionIssuer.userName`, `userIdentity.sessionContext.sessionIssuer.accountId`, `userIdentity.sessionContext.sessionIssuer.arn`, `userIdentity.sessionContext.sessionIssuer.type`, `awsRegion`, `sourceIPAddress`, `userIdentity.accountId` | sort - `@timestamp`
    ```

## Test Strategy

The tests use actual CloudTrail test data loaded into a test index to verify end-to-end functionality. Each test validates:

1. **Query Execution:** Ensures queries execute successfully without parsing errors
2. **Schema Validation:** Verifies correct field types and names in results
3. **Data Validation:** Confirms expected result counts and values
4. **Complex Filtering:** Tests null checks, string matching, and logical operations

## Running the Tests

To run the CloudTrail PPL integration tests:

```bash
# Compile the tests
./gradlew :integ-test:compileTestJava

# Run all PPL integration tests (includes CloudTrail tests)
./gradlew :integ-test:test --tests "*PPL*"

# Run only CloudTrail PPL tests
./gradlew :integ-test:test --tests "*CloudTrailPplDashboardIT*"
```

## Expected Behavior

- **All queries** should execute successfully and return valid results
- **No parsing errors** should occur for any of the CloudTrail PPL query patterns
- **Schema validation** should pass with correct field types
- **Data validation** should confirm expected result counts from test data
- **Complex filtering** should work correctly with null checks and pattern matching

## Benefits

These integration tests provide:

1. **Regression Protection:** Ensures CloudTrail dashboard queries continue to work as the SQL plugin evolves
2. **Query Validation:** Validates that all CloudTrail PPL query patterns are syntactically correct
3. **Field Compatibility:** Ensures CloudTrail field names and nested structures are properly handled
4. **Complex Query Testing:** Validates advanced filtering, grouping, and aggregation patterns
5. **Documentation:** Serves as living documentation of supported CloudTrail query patterns

## Maintenance

When adding new CloudTrail query patterns to dashboards:

1. Add the new query pattern to the test class
2. Update test data if new fields are required
3. Update the index mapping if new field types are needed
4. Run the tests to ensure compatibility

This ensures that all CloudTrail dashboard functionality remains stable and functional.