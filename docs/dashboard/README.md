# Dashboard Integration Tests

This directory contains documentation and integration tests for OpenSearch dashboard-related PPL queries.

## Overview

Dashboard integration tests ensure that PPL queries used in various OpenSearch dashboards continue to work correctly as the SQL plugin evolves. These tests provide regression protection and validate query compatibility.

## Dashboard Test Documentation

### VPC Dashboard
- **[VPC PPL Integration Tests](VPC_PPL_INTEGRATION_TESTS.md)** - Tests for VPC flow log dashboard queries
  - Covers 16+ VPC-specific PPL query patterns
  - Includes test data and index mappings
  - Validates network traffic analysis queries

### WAF Dashboard
- **[WAF PPL Integration Tests](WAF_PPL_INTEGRATION_TESTS.md)** - Tests for WAF log dashboard queries
  - Covers 9+ WAF-specific PPL query patterns
  - Includes nested httpRequest object handling
  - Validates web application firewall analysis queries

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
- **Graceful Failure Testing** - Ensure datasource queries fail appropriately

## Running Dashboard Tests

```bash
# Run all dashboard-related PPL tests
./gradlew :integ-test:test --tests "*Dashboard*"

# Run specific dashboard tests
./gradlew :integ-test:test --tests "*VpcPplDashboardIT*"
./gradlew :integ-test:test --tests "*WafPplDashboardIT*"
```