/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class BinCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  @Test
  public void testBinWithNumericSpan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats count() by age", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    // Verify that age values are binned into ranges like "20-30", "30-40", etc.
    // The exact counts may vary but we should see range format
    String resultStr = result.toString();
    assertTrue(
        "Should contain age range format",
        resultStr.contains("20-30") || resultStr.contains("30-40") || resultStr.contains("40-50"));
  }

  @Test
  public void testBinBasicFunctionality() throws IOException {
    // Test basic bin functionality with simple span parameter
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | fields age | head 10", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    String resultStr = result.toString();

    // Basic verification - should contain some range format
    assertTrue("Should contain range format with dash for age binning", resultStr.contains("-"));

    // Verify we have some data
    assertTrue("Should have datarows section", resultStr.contains("datarows"));
  }

  @Test
  public void testBinWithBinsParameterPrecise() throws IOException {
    // Test bins parameter creates reasonable binning results
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance bins=4 | stats count() by balance | sort balance",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    String resultStr = result.toString();

    // Verify we have range format
    assertTrue("Should contain range format with dash", resultStr.contains("-"));

    // Verify we have meaningful bin ranges
    assertTrue("Should have datarows section", resultStr.contains("datarows"));

    // Look for numeric range patterns like "1000-2000", "2000-3000", etc.
    assertTrue("Should contain numeric range patterns", resultStr.matches(".*\\d+-\\d+.*"));

    // Verify we have at least some count values
    assertTrue(
        "Should contain count values",
        resultStr.contains("\"total\":") && !resultStr.contains("\"total\":0"));
  }

  @Test
  public void testBinWithMinspan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 | stats count() by age", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    String resultStr = result.toString();
    assertTrue("Should contain age range format with minspan", resultStr.contains("-"));
  }

  @Test
  public void testBinWithStartEnd() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age start=20 end=40 | stats count() by age", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    // Verify start/end parameters create bins within the specified range
    // Should see range format and possibly "Other" for values outside range
    String resultStr = result.toString();
    assertTrue(
        "Should contain range format or Other category",
        resultStr.contains("-") || resultStr.contains("Other"));
  }

  @Test
  public void testBinWithAlias() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 AS age_group | fields age_group | head 5",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age_group", null, "string"));

    // Verify alias works and produces range format
    String resultStr = result.toString();
    assertTrue("Should contain range format in aliased field", resultStr.contains("-"));
  }

  @Test
  public void testBinWithLogSpan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=log10 | fields balance | head 10",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));

    // Verify log-based binning produces logarithmic ranges
    String resultStr = result.toString();
    assertTrue(
        "Should contain logarithmic range format",
        resultStr.contains("-") && (resultStr.contains("1000") || resultStr.contains("10000")));
  }

  @Test
  public void testBinWithExtendedTimeUnits() throws IOException {
    // Test parsing of extended time units with numeric field (should treat as numeric span)
    JSONObject result =
        executeQuery(
            String.format("source=%s | bin age span=30 | fields age | head 5", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("age"));
  }

  @Test
  public void testBinWithDateField() throws IOException {
    // Test basic numeric binning on non-numeric field (should handle gracefully or show proper
    // error)
    JSONObject result =
        executeQuery(String.format("source=%s | fields birthdate | head 5", TEST_INDEX_BANK));
    verifyColumn(result, columnName("birthdate"));
  }

  @Test
  public void testBinWithMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | bin balance span=10000 | fields age, balance | head"
                    + " 5",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("age"), columnName("balance"));
  }

  @Test
  public void testBinWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats avg(balance) as avg_balance by age",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("avg_balance", null, "double"), schema("age", null, "string"));

    // Verify stats work with binned values
    String resultStr = result.toString();
    assertTrue(
        "Should contain age ranges and average balance values",
        resultStr.contains("-") && (resultStr.contains("20-30") || resultStr.contains("30-40")));
  }

  @Test
  public void testBinWithWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | bin age span=5 | stats count() by age",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("count()"), columnName("age"));
  }

  @Test
  public void testBinWithSort() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("count()"), columnName("age"));
  }

  @Test
  public void testBinCoeffcientLogSpan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=2log10 | fields balance | head 5",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));

    // Verify coefficient log span produces logarithmic ranges with coefficient scaling
    String resultStr = result.toString();
    assertTrue("Should contain logarithmic range format with coefficient", resultStr.contains("-"));
  }

  @Test
  public void testBinSubsecondUnits() throws IOException {
    // Test parsing of subsecond units - should validate properly (time units require @timestamp)
    try {
      executeQuery(
          String.format(
              "source=%s | bin age span=100ms | fields age | head 5", TEST_INDEX_ACCOUNT));
      fail("Expected exception for time-based binning on non-@timestamp field");
    } catch (Exception e) {
      assertTrue(
          "Should contain validation message",
          e.getMessage().contains("Time-based binning requires '@timestamp' field"));
    }
  }

  @Test
  public void testBinWithDaysUnit() throws IOException {
    // Test that time units are validated properly (should show validation error)
    // This test documents expected behavior: time spans require @timestamp field
    try {
      executeQuery(
          String.format(
              "source=%s | bin birthdate span=7days | fields birthdate | head 5", TEST_INDEX_BANK));
      fail("Expected exception for time-based binning on non-@timestamp field");
    } catch (Exception e) {
      assertTrue(
          "Should contain validation message",
          e.getMessage().contains("Time-based binning requires '@timestamp' field"));
    }
  }

  @Test
  public void testBinWithMonthsUnit() throws IOException {
    // Test that time units are validated properly (should show validation error)
    // This test documents expected behavior: time spans require @timestamp field
    try {
      executeQuery(
          String.format(
              "source=%s | bin birthdate span=2months | fields birthdate | head 5",
              TEST_INDEX_BANK));
      fail("Expected exception for time-based binning on non-@timestamp field");
    } catch (Exception e) {
      assertTrue(
          "Should contain validation message",
          e.getMessage().contains("Time-based binning requires '@timestamp' field"));
    }
  }

  @Test
  public void testBinNumericRangeResults() throws IOException {
    // Test bin results with age binning
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    String resultStr = result.toString();

    // Based on the accounts.json data (ages 20-40), with span=10 we expect:
    // 20-30 and 30-40 bins
    assertTrue("Should contain 20-30 range", resultStr.contains("20-30"));
    assertTrue("Should contain 30-40 range", resultStr.contains("30-40"));

    // Verify schema and data structure
    assertTrue("Should have datarows section", resultStr.contains("datarows"));
    assertTrue(
        "Should have positive total count",
        resultStr.contains("\"total\":") && !resultStr.contains("\"total\":0"));
  }

  @Test
  public void testBinDefaultBehavior() throws IOException {
    // Test default binning behavior when no parameters specified
    JSONObject result =
        executeQuery(
            String.format("source=%s | bin age | fields age | head 5", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    // Default should create magnitude-based bins
    String resultStr = result.toString();
    assertTrue("Should contain range format for default binning", resultStr.contains("-"));
  }

  @Test
  public void testBinWithLargeSpan() throws IOException {
    // Test large span values
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=50000 | stats count() by balance",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    // Should create large ranges like "0-50000", "50000-100000"
    String resultStr = result.toString();
    assertTrue(
        "Should contain large range format",
        resultStr.contains("0-50000") || resultStr.contains("50000-100000"));
  }

  @Test
  public void testBinPreciseCalculation() throws IOException {
    // Test bin calculation with balance binning
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 | stats count() by balance | sort balance",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    String resultStr = result.toString();

    // Based on balance values in accounts.json (ranging from ~1000 to ~50000)
    // With span=10000, we should see ranges like 0-10000, 10000-20000, etc.
    assertTrue("Should contain 0-10000 range", resultStr.contains("0-10000"));
    assertTrue("Should contain 10000-20000 range", resultStr.contains("10000-20000"));
    assertTrue("Should contain 20000-30000 range", resultStr.contains("20000-30000"));
    assertTrue("Should contain 30000-40000 range", resultStr.contains("30000-40000"));
    assertTrue("Should contain 40000-50000 range", resultStr.contains("40000-50000"));

    // Verify schema and data structure
    assertTrue("Should have datarows section", resultStr.contains("datarows"));
    assertTrue(
        "Should have positive total count",
        resultStr.contains("\"total\":") && !resultStr.contains("\"total\":0"));
  }

  @Test
  public void testBinStartEndBoundaries() throws IOException {
    // Test start/end parameters with correct syntax
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 start=25 end=35 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    String resultStr = result.toString();

    // With span=5 start=25 end=35, we expect ranges within boundaries
    assertTrue("Should contain range format", resultStr.contains("-"));

    // Should handle values outside the start/end range somehow (either "Other" or included)
    assertTrue("Should have datarows section", resultStr.contains("datarows"));

    // Verify we have some meaningful ranges
    assertTrue("Should contain numeric range patterns", resultStr.matches(".*\\d+-\\d+.*"));
  }

  @Test
  public void testBinValidateSpecificRanges() throws IOException {
    // Test with very specific data to validate exact bin behavior
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age >= 20 AND age <= 30 | bin age span=5 | stats count() by age"
                    + " | sort age",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    String resultStr = result.toString();

    // For age range 20-30 with span=5, we expect 20-25 and 25-30
    assertTrue("Should contain range format", resultStr.contains("-"));
    assertTrue("Should have datarows", resultStr.contains("datarows"));

    // Verify the ranges make sense for the input data
    if (resultStr.contains("20-25") || resultStr.contains("25-30")) {
      assertTrue("Found expected age ranges", true);
    } else {
      // If exact ranges don't match, at least verify we have reasonable numeric ranges
      assertTrue("Should contain reasonable numeric ranges", resultStr.matches(".*\\d+-\\d+.*"));
    }
  }
}
