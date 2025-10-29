# Dashboard Integration Tests

This directory contains documentation and integration tests for OpenSearch dashboard-related PPL queries.

## Overview

Dashboard integration tests ensure that PPL queries used in various OpenSearch dashboards continue to work correctly as the SQL plugin evolves. These tests provide regression protection and validate query compatibility.

## Dashboard Test Documentation

### VPC Dashboard
- **[VPC PPL Integration Tests](VPC_PPL_INTEGRATION_TESTS.md)** - Tests for VPC flow log dashboard queries
  - Covers 18 VPC-specific PPL query patterns
  - Tests network traffic analysis with bytes, packets, and flow direction
  - Validates top talkers and destination analysis

### WAF Dashboard
- **[WAF PPL Integration Tests](WAF_PPL_INTEGRATION_TESTS.md)** - Tests for WAF log dashboard queries
  - Covers 11 WAF-specific PPL query patterns
  - Tests web application firewall analysis with nested httpRequest objects
  - Validates blocked requests and rule analysis

### CloudTrail Dashboard
- **[CloudTrail PPL Integration Tests](CLOUDTRAIL_PPL_INTEGRATION_TESTS.md)** - Tests for CloudTrail log dashboard queries
  - Covers 14 CloudTrail-specific PPL query patterns
  - Tests AWS API call monitoring with real CloudTrail log structure
  - Validates complex user identity, session analysis, and service-specific filtering

### Network Firewall Dashboard
- **[NFW PPL Integration Tests](NFW_PPL_INTEGRATION_TESTS.md)** - Tests for Network Firewall log dashboard queries
  - Covers 36 NFW-specific PPL query patterns
  - Tests network firewall analysis with netflow data and TCP flow tracking
  - Validates traffic analysis by source/destination IPs, ports, protocols, and application layer data

## Adding New Dashboard Tests

When creating tests for new dashboard types:

1. Create a new test class in `/integ-test/src/test/java/org/opensearch/sql/ppl/`
2. Add test data files in `/integ-test/src/test/resources/`
3. Create index mappings in `/integ-test/src/test/resources/indexDefinitions/`
4. Document the tests in this directory

## Test Structure

Each dashboard test should include:
- **Query Pattern Validation** - Ensure all dashboard queries parse correctly
- **Real Data Testing** - Test with realistic sample data
- **Schema Validation** - Verify field types and query results
- **Data Validation** - Confirm expected result counts and values

## Running Dashboard Tests

```bash
# Run all dashboard-related PPL tests
./gradlew :integ-test:test --tests "*Dashboard*"

# Run specific dashboard tests
./gradlew :integ-test:test --tests "*VpcPplDashboardIT*"
./gradlew :integ-test:test --tests "*WafPplDashboardIT*"
./gradlew :integ-test:test --tests "*CloudTrailPplDashboardIT*"
./gradlew :integ-test:test --tests "*NfwPplDashboardIT*"
```