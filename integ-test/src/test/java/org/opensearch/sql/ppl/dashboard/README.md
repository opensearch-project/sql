# Dashboard Integration Tests

This directory contains documentation and integration tests for OpenSearch dashboard-related PPL queries.

## Overview

Dashboard integration tests ensure that PPL queries used in various OpenSearch dashboards continue to work correctly as the SQL plugin evolves. These tests provide regression protection and validate query compatibility.

## Dashboard Test Documentation

### CloudTrail Dashboard
- **[CloudTrail PPL Integration Tests](../../resources/doctest/templates/dashboard/cloudtrail.rst)** - Tests for CloudTrail log dashboard queries
  - Validates AWS API call analysis queries
  - Tests user activity and security monitoring

### Network Firewall (NFW) Dashboard
- **[NFW PPL Integration Tests](../../resources/doctest/templates/dashboard/nfw.rst)** - Tests for Network Firewall log dashboard queries
  - Validates network security analysis queries
  - Tests firewall rule and traffic monitoring

### VPC Dashboard
- **[VPC PPL Integration Tests](../../resources/doctest/templates/dashboard/vpc.rst)** - Tests for VPC flow log dashboard queries
  - Validates network traffic analysis queries
  - Tests top talkers, destinations, bytes, and packets analysis

### WAF Dashboard
- **[WAF PPL Integration Tests](../../resources/doctest/templates/dashboard/waf.rst)** - Tests for WAF log dashboard queries
  - Includes nested httpRequest object handling
  - Validates web application firewall analysis queries
  - Tests blocked requests and rule analysis

## Adding New Dashboard Tests

When creating tests for new dashboard types:

1. Create a new test class in `/integ-test/src/test/java/org/opensearch/sql/ppl/dashboard/`
2. Add test data files in `/integ-test/src/test/resources/doctest/testdata/`
3. Add index mappings in `/integ-test/src/test/resources/doctest/mappings/`
4. Add test template files in `/integ-test/src/test/resources/doctest/templates/dashboard/`
5. Document the tests in this directory

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
```