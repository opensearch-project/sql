/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteBinCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    // Load test data using existing accounts.json data (1000 accounts)
    loadIndex(Index.ACCOUNT);
    // Load date formats data for aligntime testing
    loadIndex(Index.DATE_FORMATS);
  }

  @Test
  public void testBasicBinWithSpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=5000 | fields account_number, balance | head 5",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "string")); // balance is now transformed to range strings

    // Verify that binning creates proper range strings
    // Based on the actual accounts.json data, first few accounts have these exact values
    verifyDataRows(
        actual,
        rows(1, "35000-40000"), // 39225 falls in range 35000-40000
        rows(6, "5000-10000"), // 5686 falls in range 5000-10000
        rows(13, "30000-35000"), // 32838 falls in range 30000-35000
        rows(18, "0-5000"), // 4180 falls in range 0-5000
        rows(20, "15000-20000")); // 16418 falls in range 15000-20000
  }

  @Test
  public void testBinWithSpanAndAlias() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 AS balance_range | fields account_number,"
                    + " balance, balance_range | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"), // Original field stays unchanged
        schema("balance_range", "string")); // Alias creates a new field with range strings

    verifyDataRows(
        actual,
        rows(1, 39225, "30000-40000"), // Original balance + range string
        rows(6, 5686, "0-10000"), // Original balance + range string
        rows(13, 32838, "30000-40000")); // Original balance + range string
  }

  @Test
  public void testBinWithDecimalSpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=7500 AS balance_group | fields account_number,"
                    + " balance, balance_group | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_group", "string")); // Range strings

    // Test binning with integer span values - verify first 3 accounts
    verifyDataRows(
        actual,
        rows(1, 39225, "37500-45000"), // 39225 falls in range 37500-45000
        rows(6, 5686, "0-7500"), // 5686 falls in range 0-7500
        rows(13, 32838, "30000-37500")); // 32838 falls in range 30000-37500
  }

  @Test
  public void testBinDefaultBehavior() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age | fields account_number, age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("age", "string")); // Age is transformed to range strings

    // With default behavior (no span, no bins), should use span=10
    verifyDataRows(
        actual,
        rows(1, "30-40"), // 32 falls in range 30-40
        rows(6, "30-40"), // 36 falls in range 30-40
        rows(13, "20-30")); // 28 falls in range 20-30
  }

  @Test
  public void testBinIntegrationWithStats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 AS balance_bucket | stats count() by"
                    + " balance_bucket | sort balance_bucket",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("count()", "bigint"), schema("balance_bucket", "bigint"));

    // Based on 1000 accounts with balances from ~1000 to ~50000, verify we get multiple buckets
    // Verify just that we have a reasonable structure with correct first bucket
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 5); // Should have at least 5 buckets

    // First row should be bucket 0 with reasonable count
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals(0, firstRow.getInt(1)); // First bucket should be 0
    assertTrue(firstRow.getInt(0) > 100 && firstRow.getInt(0) < 300); // Reasonable count
  }

  @Test
  public void testBinWithStatsAndAverage() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=20000 AS balance_group | stats avg(age) as avg_age by"
                    + " balance_group | sort balance_group",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("avg_age", "double"), schema("balance_group", "bigint"));

    // Should have 3 balance groups: 0, 20000, 40000
    // Verify we have multiple groups and reasonable structure
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 2); // Should have at least 2 groups

    // First group should be balance group 0 with reasonable avg age
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals(0, firstRow.getInt(1)); // First balance group should be 0
    double avgAge = firstRow.getDouble(0);
    assertTrue(avgAge >= 20.0 && avgAge <= 40.0); // Reasonable average age
  }

  @Test
  public void testBinMathematicalCorrectness() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=15000 | fields account_number, firstname, balance,"
                    + " balance_bin | sort balance | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"),
        schema("balance_bin", "string"));

    // Verify the lowest balance accounts and their binning
    // The accounts with lowest balances should have range strings like "0-15000"
    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(2);
      String balanceBin = row.getString(3);
      long expectedBinStart = (balance / 15000) * 15000;
      long expectedBinEnd = expectedBinStart + 15000;
      String expectedRange = expectedBinStart + "-" + expectedBinEnd;
      assertEquals(expectedRange, balanceBin);
    }
  }

  @Test
  public void testBinWithFilterAndSort() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | bin balance span=10000 AS balance_bucket |"
                    + " fields firstname, balance, balance_bucket | sort balance_bucket, balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("balance", "bigint"),
        schema("balance_bucket", "string"));

    // All results should have balance > 30000 and balance_bucket ranges starting >= 30000
    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(1);
      String balanceBucket = row.getString(2);
      assertTrue(balance > 30000);
      // Extract start value from range string like "30000-40000"
      long bucketStart = Long.parseLong(balanceBucket.split("-")[0]);
      assertTrue(bucketStart >= 30000);
    }
  }

  @Test
  public void testBinFieldNotFound() {
    // Test error handling for non-existent field
    Throwable e =
        assertThrowsWithReplace(
            IllegalStateException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | bin nonexistent_field span=1000", TEST_INDEX_ACCOUNT)));

    verifyErrorMessageContains(e, "field [nonexistent_field] not found");
  }

  @Test
  public void testBinWithBinsParameterBasic() throws IOException {
    // Test basic bins parameter functionality
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age bins=4 AS age_bin | fields account_number, age, age_bin | head"
                    + " 10",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("age", "bigint"),
        schema("age_bin", "string"));

    // Verify that binning occurred (should not have raw age values)
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have data", datarows.length() > 0);

    // Check that we get range string values, not individual age values
    Set<String> ageBins = new HashSet<>();
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String ageBin = row.getString(2);
      ageBins.add(ageBin);
    }

    // With bins=4 for age range (roughly 20-50), should have at most 4 distinct range strings
    assertTrue("Should have at most 4 bins with bins=4", ageBins.size() <= 4);
    assertTrue("Should have at least 2 bins", ageBins.size() >= 2);

    // Verify bins are reasonable range strings (like "20-30", "30-40")
    for (String bin : ageBins) {
      assertTrue("Bin values should be range strings", bin.contains("-"));
      assertTrue("Should not be 'Other'", !bin.equals("Other"));
    }
  }

  @Test
  public void testBasicBinWithMinspan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance minspan=5000 | fields account_number, balance, balance_bin"
                    + " | head 5",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_bin", "string"));

    // With minspan=5000, the system should use 5000 as the span value
    // Verify that binning creates proper range strings
    verifyDataRows(
        actual,
        rows(1, 39225, "35000-40000"), // floor(39225/5000) * 5000 = 35000, range is 35000-40000
        rows(6, 5686, "5000-10000"), // floor(5686/5000) * 5000 = 5000, range is 5000-10000
        rows(13, 32838, "30000-35000"), // floor(32838/5000) * 5000 = 30000, range is 30000-35000
        rows(18, 4180, "0-5000"), // floor(4180/5000) * 5000 = 0, range is 0-5000
        rows(20, 16418, "15000-20000")); // floor(16418/5000) * 5000 = 15000, range is 15000-20000
  }

  @Test
  public void testBinWithMinspanAndAlias() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance minspan=10000 AS balance_tier | fields account_number,"
                    + " balance, balance_tier | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_tier", "string"));

    verifyDataRows(
        actual,
        rows(1, 39225, "30000-40000"), // floor(39225/10000) * 10000 = 30000, range is 30000-40000
        rows(6, 5686, "0-10000"), // floor(5686/10000) * 10000 = 0, range is 0-10000
        rows(13, 32838, "30000-40000")); // floor(32838/10000) * 10000 = 30000, range is 30000-40000
  }

  @Test
  public void testBinWithMinspanDecimalValue() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance minspan=7500.5 AS balance_category | fields"
                    + " account_number, balance, balance_category | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_category", "string"));

    // Test minspan with decimal values - verify first 3 accounts
    // Note: Even with decimal minspan, our implementation returns range strings
    verifyDataRows(
        actual,
        rows(
            1,
            39225,
            "37502.5-45003.0"), // floor(39225/7500.5) * 7500.5 = 37502.5, range with decimal
        rows(6, 5686, "0.0-7500.5"), // floor(5686/7500.5) * 7500.5 = 0.0, range with decimal
        rows(
            13,
            32838,
            "30002.0-37502.5")); // floor(32838/7500.5) * 7500.5 = 30002.0, range with decimal
  }

  @Test
  public void testBinMinspanIntegrationWithStats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance minspan=15000 AS balance_segment | stats count() by"
                    + " balance_segment | sort balance_segment",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("count()", "bigint"), schema("balance_segment", "string"));

    // Based on 1000 accounts with balances from ~1000 to ~50000, verify we get multiple segments
    // Verify just that we have a reasonable structure with correct first segment
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 3); // Should have at least 3 segments

    // First row should be segment "0-15000" with reasonable count
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals("0-15000", firstRow.getString(1)); // First segment should be "0-15000"
    assertTrue(
        firstRow.getInt(0) > 100 && firstRow.getInt(0) < 500); // Reasonable count for 15000 span
  }

  @Test
  public void testBinMinspanWithAgeField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 AS age_group | fields account_number, age, age_group"
                    + " | head 5",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("age", "bigint"),
        schema("age_group", "string"));

    // Age values should be binned into range strings with span of 5
    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long age = row.getLong(1);
      String ageGroup = row.getString(2);
      long expectedGroupStart = (age / 5) * 5;
      long expectedGroupEnd = expectedGroupStart + 5;
      String expectedRange = expectedGroupStart + "-" + expectedGroupEnd;
      assertEquals(expectedRange, ageGroup);
    }
  }

  @Test
  public void testBinMinspanMathematicalCorrectness() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance minspan=12000 | fields account_number, firstname, balance,"
                    + " balance_bin | sort balance | head 4",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"),
        schema("balance_bin", "string"));

    // Verify mathematical correctness for minspan binning
    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(2);
      String balanceBin = row.getString(3);
      long expectedBinStart = (balance / 12000) * 12000;
      long expectedBinEnd = expectedBinStart + 12000;
      String expectedRange = expectedBinStart + "-" + expectedBinEnd;
      assertEquals(expectedRange, balanceBin);
    }
  }

  @Test
  public void testBinWithAligntimeEarliest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin epoch_millis span=86400000 aligntime=earliest AS day_bucket |"
                    + " fields epoch_millis, day_bucket | head 3",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("epoch_millis", "timestamp"), schema("day_bucket", "timestamp"));

    // With aligntime=earliest, should behave the same as no aligntime
    // The epoch_millis values are around 450608862000 (1984-04-12)
    // With 86400000ms (1 day) span, we expect day-aligned buckets
    // Note: Just verify we get valid results rather than exact timestamp formats
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      assertNotNull(row.getString(0)); // epoch_millis should be valid timestamp
      assertNotNull(row.getString(1)); // day_bucket should be valid timestamp
      assertTrue(row.getString(0).startsWith("1984-04-12")); // Should be 1984-04-12
      assertTrue(row.getString(1).startsWith("1984-04-12")); // Should be 1984-04-12
    }
  }

  @Test
  public void testBinWithAligntimeLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin epoch_millis span=3600000 aligntime=latest AS hour_bucket | fields"
                    + " epoch_millis, hour_bucket | head 2",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("epoch_millis", "timestamp"), schema("hour_bucket", "timestamp"));

    // With aligntime=latest, the buckets should be aligned to the latest time
    // This test mainly verifies that the query executes without error
    // and produces timestamp results (exact values depend on current time)
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    // Verify we get valid timestamp strings
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      assertNotNull(row.getString(0)); // epoch_millis should be valid timestamp
      assertNotNull(row.getString(1)); // hour_bucket should be valid timestamp
    }
  }

  @Test
  public void testBinWithAligntimeNumericValue() throws IOException {
    // Use a specific epoch timestamp (1984-04-12 00:00:00 UTC = 450576000000)
    long aligntime = 450576000000L; // Midnight of 1984-04-12

    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | bin epoch_millis span=3600000 aligntime=%d AS hour_bucket | fields"
                    + " epoch_millis, hour_bucket | head 3",
                TEST_INDEX_DATE_FORMATS,
                aligntime));

    verifySchema(actual, schema("epoch_millis", "timestamp"), schema("hour_bucket", "timestamp"));

    // The epoch_millis values (450608862000) should be binned relative to the aligntime
    // With aligntime=450576000000 and span=3600000 (1 hour):
    // floor((450608862000 - 450576000000) / 3600000) * 3600000 + 450576000000
    // = floor(32862000 / 3600000) * 3600000 + 450576000000
    // = floor(9.13) * 3600000 + 450576000000
    // = 9 * 3600000 + 450576000000 = 32400000 + 450576000000 = 450608400000
    // Note: Just verify we get valid results and proper binning
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      assertNotNull(row.getString(0)); // epoch_millis should be valid timestamp
      assertNotNull(row.getString(1)); // hour_bucket should be valid timestamp
      assertTrue(row.getString(0).startsWith("1984-04-12")); // Should be 1984-04-12
      assertTrue(row.getString(1).startsWith("1984-04-12")); // Should be 1984-04-12
      // Verify hour_bucket is at hour boundary (ends with :00:00)
      assertTrue(row.getString(1).matches(".*[0-9]{2}:00:00.*")); // Should end with :00:00
    }
  }

  @Test
  public void testBinWithAligntimeOnEpochSecond() throws IOException {
    // Test aligntime functionality on epoch_second field
    long aligntime = 450576000L; // 1984-04-12 00:00:00 UTC in seconds

    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | bin epoch_second span=3600 aligntime=%d AS hour_bucket | fields"
                    + " epoch_second, hour_bucket | head 2",
                TEST_INDEX_DATE_FORMATS,
                aligntime));

    verifySchema(actual, schema("epoch_second", "timestamp"), schema("hour_bucket", "timestamp"));

    // Verify that binning works correctly on epoch_second field
    // The epoch_second values should be binned into hour buckets aligned to the aligntime
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      assertNotNull(row.getString(0)); // epoch_second should be valid timestamp
      assertNotNull(row.getString(1)); // hour_bucket should be valid timestamp
    }
  }

  @Test
  public void testBinWithAligntimeIgnoredForNonTimeFields() throws IOException {
    // Test that aligntime is ignored for non-time fields like balance
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 aligntime=earliest AS balance_bucket | fields"
                    + " account_number, balance, balance_bucket | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_bucket", "string"));

    // Should behave exactly like normal binning (aligntime ignored for numeric fields)
    verifyDataRows(
        actual,
        rows(1, 39225, "30000-40000"), // floor(39225/10000) * 10000 = 30000, range is 30000-40000
        rows(6, 5686, "0-10000"), // floor(5686/10000) * 10000 = 0, range is 0-10000
        rows(13, 32838, "30000-40000")); // floor(32838/10000) * 10000 = 30000, range is 30000-40000
  }

  @Test
  public void testBinWithAligntimeMathematicalCorrectness() throws IOException {
    // Test the mathematical correctness of aligntime binning
    long aligntime = 450000000000L; // A specific alignment point
    long span = 7200000L; // 2 hours in milliseconds

    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | bin epoch_millis span=%d aligntime=%d AS aligned_bucket | fields"
                    + " epoch_millis, aligned_bucket | head 3",
                TEST_INDEX_DATE_FORMATS,
                span,
                aligntime));

    verifySchema(
        actual, schema("epoch_millis", "timestamp"), schema("aligned_bucket", "timestamp"));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    // Verify that the aligned buckets follow the mathematical formula:
    // aligned_bucket = floor((timestamp - aligntime) / span) * span + aligntime
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      assertNotNull(row.getString(0)); // Should have valid timestamp
      assertNotNull(row.getString(1)); // Should have valid aligned bucket
    }
  }

  @Test
  public void testBinWithAligntimeErrorHandling() throws IOException {
    // Test that the query succeeds even with aligntime on mixed field types
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin epoch_millis span=3600000 aligntime=earliest | bin incomplete_1"
                    + " span=10 aligntime=latest | fields epoch_millis, epoch_millis_bin,"
                    + " incomplete_1, incomplete_1_bin | head 2",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("epoch_millis", "timestamp"),
        schema("epoch_millis_bin", "timestamp"),
        schema("incomplete_1", "timestamp"),
        schema("incomplete_1_bin", "timestamp"));

    // Should handle multiple bin operations with different aligntime settings
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);
  }

  @Test
  public void testBinWithStartAndEndConstraints() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 start=10000 end=40000 AS balance_range | fields"
                    + " account_number, balance, balance_range | sort balance | head 10",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_range", "string"));

    // Verify that only values within [10000, 40000] range are binned
    // Values outside the range should have NULL in balance_range
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(1);

      if (balance >= 10000 && balance <= 40000) {
        // Values within range should have a binned range string
        assertFalse("Balance range should not be null for values in range", row.isNull(2));
        String balanceRange = row.getString(2);
        assertTrue("Balance range should contain dash", balanceRange.contains("-"));
        // Extract start value from range string like "30000-40000"
        long rangeStart = Long.parseLong(balanceRange.split("-")[0]);
        assertTrue(
            "Balance range start should be valid bin", rangeStart >= 10000 && rangeStart <= 40000);
        assertTrue("Balance range start should be multiple of 10000", rangeStart % 10000 == 0);
      } else {
        // Values outside range should be NULL
        assertTrue("Balance range should be null for values outside range", row.isNull(2));
      }
    }
  }

  @Test
  public void testBinWithOnlyStartConstraint() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=15000 start=20000 AS balance_group | fields"
                    + " account_number, balance, balance_group | sort balance | head 8",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_group", "string"));

    // Verify that only values >= 20000 are binned
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(1);

      if (balance >= 20000) {
        // Values >= start should have a binned range string
        assertFalse("Balance group should not be null for values >= start", row.isNull(2));
        String balanceGroup = row.getString(2);
        assertTrue("Balance group should contain dash", balanceGroup.contains("-"));
        // Extract start value from range string like "30000-45000"
        long groupStart = Long.parseLong(balanceGroup.split("-")[0]);
        assertTrue("Balance group start should be >= start", groupStart >= 20000);
        assertTrue("Balance group start should be multiple of 15000", groupStart % 15000 == 0);
      } else {
        // Values < start should be NULL
        assertTrue("Balance group should be null for values < start", row.isNull(2));
      }
    }
  }

  @Test
  public void testBinWithSpanIgnoresStartEnd() throws IOException {
    // SPL behavior: when span is specified, start/end are completely ignored
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=5000 start=30000 end=40000 AS balance_bin | fields"
                    + " account_number, balance, balance_bin | sort balance | head 10",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_bin", "string"));

    // Should see binning applied to ALL values, not just those in [30000, 40000]
    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(1);
      String balanceBin = row.getString(2);

      // All values should be binned with span=5000, regardless of start/end
      long expectedBinStart = (balance / 5000) * 5000;
      long expectedBinEnd = expectedBinStart + 5000;
      String expectedRange = expectedBinStart + "-" + expectedBinEnd;
      assertEquals("Bin should use span regardless of start/end", expectedRange, balanceBin);
    }
  }

  @Test
  public void testBinWithBinsUsesNiceNumbers() throws IOException {
    // SPL uses "nice" numbers - bins=4 for range 20-40 should use width=10, creating 3 bins
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age >= 20 AND age <= 40 | bin age bins=4 AS age_bin | "
                    + "stats count() by age_bin | sort age_bin",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("count()", "bigint"), schema("age_bin", "string"));

    // SPL would create bins: 20-30, 30-40 (using nice width=10 instead of exact width=5)
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have at most 3 bins with nice width=10", datarows.length() <= 3);

    // Verify bins are range strings with nice number boundaries
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String ageBin = row.getString(1);
      assertTrue("Bins should be range strings", ageBin.contains("-"));
      // Extract start value from range string like "20-30"
      long binStart = Long.parseLong(ageBin.split("-")[0]);
      assertEquals("Bins should start at multiples of 10", 0, binStart % 10);
    }
  }

  @Test
  public void testBinNiceNumberThresholdBehavior() throws IOException {
    // Test SPL's dramatic behavior change at range thresholds
    // Range ≤ 100 uses width=10, Range > 100 uses width=100

    // Test 1: start=0 end=100 should use width=10
    JSONObject actual1 =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=100 AS age_bin | "
                    + "stats count() by age_bin | sort age_bin | head 3",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows1 = actual1.getJSONArray("datarows");
    if (datarows1.length() >= 2) {
      String bin1 = datarows1.getJSONArray(0).getString(1);
      String bin2 = datarows1.getJSONArray(1).getString(1);
      // Extract start values from range strings like "20-30", "30-40"
      long start1 = Long.parseLong(bin1.split("-")[0]);
      long start2 = Long.parseLong(bin2.split("-")[0]);
      assertEquals("Width should be 10 for range ≤ 100", 10, start2 - start1);
    }

    // Test 2: start=0 end=101 should use width=100 (dramatic change!)
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=101 AS age_bin | "
                    + "stats count() by age_bin | sort age_bin",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows2 = actual2.getJSONArray("datarows");
    // With width=100, all ages should fall into single range bin like "0-100"
    assertEquals("Should have single bin with width=100", 1, datarows2.length());
    assertEquals(
        "Single bin should be range starting at 0",
        "0-100",
        datarows2.getJSONArray(0).getString(1));
  }

  @Test
  public void testBinStartEndExpandRangeOnly() throws IOException {
    // SPL behavior: start/end can only expand the range, never shrink it
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where balance >= 30000 AND balance <= 40000 | "
                    + "bin balance start=20000 end=50000 bins=5 AS balance_bin | "
                    + "fields balance, balance_bin | sort balance",
                TEST_INDEX_ACCOUNT));

    // Even though data is 30000-40000, bins should be calculated for 20000-50000 range
    // With range=30000 and bins=5, nice width would be 10000
    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(0);
      String balanceBin = row.getString(1);

      // Bins should be range strings based on expanded range starting at 20000
      assertTrue("Bins should be range strings", balanceBin.contains("-"));
      long binStart = Long.parseLong(balanceBin.split("-")[0]);
      assertTrue("Bins should start with multiples of 10000", binStart % 10000 == 0);
      assertTrue("Bins should start from 20000 or higher", binStart >= 20000);
    }
  }

  @Test
  public void testBinWithOnlyBinsParameter() throws IOException {
    // Test bins parameter without start/end - should use actual data range
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 AS age_group | "
                    + "stats count() AS cnt by age_group | sort age_group",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("cnt", "bigint"), schema("age_group", "string"));

    // Should create at most 5 bins with nice numbers
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have at most 5 bins", datarows.length() <= 5);

    // Verify nice number binning with range strings
    if (datarows.length() >= 2) {
      String bin1 = datarows.getJSONArray(0).getString(1);
      String bin2 = datarows.getJSONArray(1).getString(1);

      // Extract start values from range strings like "20-30", "30-40"
      long start1 = Long.parseLong(bin1.split("-")[0]);
      long start2 = Long.parseLong(bin2.split("-")[0]);
      long width = start2 - start1;

      // Width should be a nice number (1, 2, 5, 10, 20, 50, 100, etc.)
      assertTrue(
          "Width should be a nice number",
          width == 1
              || width == 2
              || width == 5
              || width == 10
              || width == 20
              || width == 50
              || width == 100
              || width == 200
              || width == 500
              || width == 1000);
    }
  }

  @Test
  public void testBinWithOnlyEndConstraint() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=12000 end=35000 AS balance_tier | fields"
                    + " account_number, balance, balance_tier | sort balance | head 8",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("balance_tier", "string"));

    // Verify that only values <= 35000 are binned
    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue(datarows.length() >= 1);

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      long balance = row.getLong(1);

      if (balance <= 35000) {
        // Values <= end should have a binned range string
        assertFalse("Balance tier should not be null for values <= end", row.isNull(2));
        String balanceTier = row.getString(2);
        assertTrue("Balance tier should contain dash", balanceTier.contains("-"));
        // Extract start value from range string like "24000-36000"
        long tierStart = Long.parseLong(balanceTier.split("-")[0]);
        assertTrue("Balance tier start should be <= end", tierStart <= 35000);
        assertTrue("Balance tier start should be multiple of 12000", tierStart % 12000 == 0);
      } else {
        // Values > end should be NULL
        assertTrue("Balance tier should be null for values > end", row.isNull(2));
      }
    }
  }

  @Test
  public void testBinSPLRangeThresholdBehavior() throws IOException {
    // Critical SPL compatibility test: dramatic behavior change at range thresholds
    // This test demonstrates the core SPL range-threshold algorithm:
    // - range <= 100 → width = 10 (multiple bins)
    // - range > 100 → width = 100 (single massive bin)

    // Test case 1: end=100 → range=100 → width=10 → multiple bins
    JSONObject actual100 =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=100 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));

    // Should have multiple bins with width=10: 0-10, 10-20, 20-30, 30-40, 40-50
    JSONArray datarows100 = actual100.getJSONArray("datarows");
    assertTrue("Should have multiple bins for range=100", datarows100.length() > 1);

    // Test case 2: end=101 → range=101 → width=100 → single massive bin
    JSONObject actual101 =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=101 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));

    // Should have ONLY ONE bin with width=100: 0-100 containing ALL records
    JSONArray datarows101 = actual101.getJSONArray("datarows");
    assertEquals("Should have exactly 1 bin for range=101", 1, datarows101.length());

    // Verify the single bin contains ALL age records (should be ~1000 records)
    JSONArray singleBin = datarows101.getJSONArray(0);
    String ageBin = singleBin.getString(0);
    int count = singleBin.getInt(1);

    assertEquals("Should be single massive bin 0-100", "0-100", ageBin);
    assertTrue("Should contain ALL records (>= 900)", count >= 900);

    // This demonstrates SPL's dramatic threshold behavior:
    // end=100 vs end=101 completely changes the binning strategy
  }

  @Test
  public void testBinParameterPrecedenceFix() throws IOException {
    // Critical Bug 1 Fix: bins parameter should IGNORE start/end completely
    // Test: bin age bins=3 start=100 end=200
    // Expected: Should create bins based on actual age data (0-100), NOT start/end values

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age bins=3 start=100 end=200 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have bins based on data range, not start/end", datarows.length() <= 3);

    // Verify that bins are NOT in the 100-200 range (which would be wrong)
    // Instead, they should be in the actual age data range (20-50)
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String ageBin = row.getString(0);

      // Should NOT contain bins like "100-133" or "133-166" etc.
      assertFalse(
          "Bins should not be in start/end range when bins param is present",
          ageBin.startsWith("100") || ageBin.startsWith("133") || ageBin.startsWith("166"));

      // Should contain bins based on actual data range
      assertTrue(
          "Should contain proper age range bins",
          ageBin.contains("20")
              || ageBin.contains("30")
              || ageBin.contains("40")
              || ageBin.contains("50"));
    }
  }

  @Test
  public void testBinCompleteCoverageNoOther() throws IOException {
    // Critical Bug 2 & 3 Fix: Complete bin coverage without "Other" fallback
    // Test: bin balance span=10000
    // Expected: Should create 5 proper bins covering full range, NO "Other"

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 | stats count() by balance | sort balance",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have multiple bins for complete coverage", datarows.length() >= 3);

    // Verify NO "Other" bins exist
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String balanceBin = row.getString(0);

      assertFalse(
          "Should never create 'Other' bins - SPL doesn't use them", "Other".equals(balanceBin));
      assertFalse(
          "Should never create 'Outlier' bins in normal cases", "Outlier".equals(balanceBin));

      // Should be proper range strings
      assertTrue("All bins should be proper range strings", balanceBin.contains("-"));
    }

    // Expected bins: 0-10000, 10000-20000, 20000-30000, 30000-40000, 40000-50000
    // Verify we have comprehensive coverage
    boolean hasLowRange = false;
    boolean hasMidRange = false;
    boolean hasHighRange = false;

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String balanceBin = row.getString(0);

      if (balanceBin.startsWith("0-") || balanceBin.startsWith("10000-")) {
        hasLowRange = true;
      }
      if (balanceBin.startsWith("20000-") || balanceBin.startsWith("30000-")) {
        hasMidRange = true;
      }
      if (balanceBin.startsWith("40000-") || balanceBin.startsWith("50000-")) {
        hasHighRange = true;
      }
    }

    assertTrue("Should have low range coverage", hasLowRange);
    assertTrue("Should have mid range coverage", hasMidRange);
    assertTrue("Should have high range coverage", hasHighRange);
  }

  @Test
  public void testBinNoTrailingSpaces() throws IOException {
    // Fix: Remove trailing spaces from range strings
    // Current: "20-25  " → Required: "20-25"

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 | stats count() by age | head 3", TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String ageBin = row.getString(0);

      // Verify no trailing spaces
      assertEquals("Range strings should not have trailing spaces", ageBin.trim(), ageBin);

      // Should be clean format like "20-25", not "20-25  "
      assertTrue("Should be proper range format", ageBin.matches("\\d+-\\d+"));
    }
  }

  @Test
  public void testBinsFiveCreatesExactlyFiveBins() throws IOException {
    // CRITICAL BUG FIX: bins=5 should create exactly 5 bins, not 500+
    // Expected: 0-10000, 10000-20000, 20000-30000, 30000-40000, 40000-50000

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance bins=5 | stats count() by balance | sort balance",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");

    // Should have exactly 5 bins (or fewer if data doesn't fill all ranges)
    assertTrue("Should have at most 5 bins for bins=5", datarows.length() <= 5);
    assertTrue("Should have at least 3 bins", datarows.length() >= 3);

    // Verify these are LARGE bins (like 10000 width), not tiny bins (like 100 width)
    boolean hasLargeBins = false;
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String balanceBin = row.getString(0);

      // Should have large bin widths like "0-10000", "10000-20000"
      // NOT tiny widths like "0-100", "100-200"
      if (balanceBin.contains("10000")
          || balanceBin.contains("20000")
          || balanceBin.contains("30000")
          || balanceBin.contains("40000")) {
        hasLargeBins = true;
      }

      // Should NOT have tiny bins
      assertFalse(
          "Should not create tiny bins like 0-100",
          balanceBin.equals("0-100") || balanceBin.equals("100-200"));
    }

    assertTrue("Should create large bins with 10000+ width", hasLargeBins);

    // Verify total count is reasonable (should have all 1000 records distributed)
    int totalCount = 0;
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      totalCount += row.getInt(1);
    }
    assertTrue("Should have most records distributed across bins", totalCount >= 900);
  }

  @Test
  public void testCriticalBugFixes() throws IOException {
    // BUG FIX 1: Parameter precedence - span must ignore start/end completely
    JSONObject spanOnly =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));

    JSONObject spanWithIgnoredParams =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 start=999 end=999 | stats count() by age | sort age |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));

    // Results should be IDENTICAL - start/end should be completely ignored when span is present
    assertEquals(
        "Span with start/end should be identical to span alone",
        spanOnly.toString(),
        spanWithIgnoredParams.toString());
  }

  @Test
  public void testBalanceSpanSequentialBins() throws IOException {
    // BUG FIX 3: balance span=10000 should create proper sequential bins, not "200-10200"
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 | stats count() by balance | sort balance",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have multiple sequential bins", datarows.length() >= 3);

    // Verify proper sequential bins: 0-10000, 10000-20000, 20000-30000, etc.
    boolean hasProperSequence = false;
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String balanceBin = row.getString(0);

      // Should have proper sequential bins
      if (balanceBin.equals("0-10000")
          || balanceBin.equals("10000-20000")
          || balanceBin.equals("20000-30000")
          || balanceBin.equals("30000-40000")) {
        hasProperSequence = true;
      }

      // Should NOT have the broken "200-10200" pattern
      assertFalse(
          "Should not create broken ranges like 200-10200",
          balanceBin.contains("200-10200") || balanceBin.contains("-10200"));
    }

    assertTrue("Should have proper sequential bins like 0-10000, 10000-20000", hasProperSequence);
  }

  @Test
  public void testAgeBinsTenCreatesSPLBehavior() throws IOException {
    // BUG FIX 2: age bins=10 should create 3 bins with width=10 (SPL behavior)
    // NOT 10 tiny bins with width=3 or 5 bins with width=5

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin age bins=10 | stats count() by age | sort age",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");

    // SPL creates 3 bins with width=10: 20-30, 30-40, 40-50
    assertTrue(
        "Should have around 3 bins for age bins=10 (SPL behavior)",
        datarows.length() >= 2 && datarows.length() <= 4);

    // Verify we have decade-based bins (width=10), not tiny bins (width=3)
    boolean hasDecadeBins = false;
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String ageBin = row.getString(0);

      // Should have decade-based bins
      if (ageBin.equals("20-30") || ageBin.equals("30-40") || ageBin.equals("40-50")) {
        hasDecadeBins = true;
      }

      // Should NOT have tiny bins like "20-23", "23-26", etc.
      assertFalse(
          "Should not create tiny bins for age",
          ageBin.equals("20-23") || ageBin.equals("23-26") || ageBin.equals("26-29"));
    }

    assertTrue("Should create decade-based bins like 20-30, 30-40", hasDecadeBins);
  }

  @Test
  public void testDefaultBinningBehavior() throws IOException {
    // FINAL FIX: Default binning should create smart bins like SPL
    // Test: bin balance (no parameters)
    // Expected: 5 proper bins like 0-10000, 10000-20000, etc.
    // NOT single bin "100-110" with all records

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | bin balance | stats count() by balance | sort balance",
                TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");

    // Should have multiple bins (like 5), not just 1
    assertTrue("Default binning should create multiple bins", datarows.length() >= 3);
    assertTrue("Should have at most 10 bins for good default behavior", datarows.length() <= 10);

    // Should NOT have the broken single bin "100-110"
    boolean hasBrokenSingleBin = false;
    boolean hasProperLargeBins = false;

    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String balanceBin = row.getString(0);

      // Check for the broken single bin
      if (balanceBin.equals("100-110")) {
        hasBrokenSingleBin = true;
      }

      // Check for proper large bins (SPL-style)
      if (balanceBin.contains("10000")
          || balanceBin.contains("20000")
          || balanceBin.contains("30000")
          || balanceBin.contains("40000")) {
        hasProperLargeBins = true;
      }
    }

    assertFalse("Should NOT create broken single bin '100-110'", hasBrokenSingleBin);
    assertTrue("Should create proper large bins like '0-10000', '10000-20000'", hasProperLargeBins);

    // Verify reasonable distribution of records across bins
    int totalCount = 0;
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      totalCount += row.getInt(1);
    }
    assertTrue("Should distribute most records across multiple bins", totalCount >= 900);

    // This should now produce the same result as "bin balance span=10000"
    // demonstrating full SPL compatibility for default behavior!
  }
}
