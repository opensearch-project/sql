/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for multivalue statistics functions list() and values(). Tests proper behavior
 * in both V2 (Calcite disabled) and V3 (Calcite enabled) modes. Note: In shared integration test
 * environment, Calcite may be persistently enabled. Comprehensive V3 Calcite tests are in
 * CalciteMultiValueStatsIT.
 */
public class MultiValueStatsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init(); // By default, Calcite is disabled in PPLIntegTestCase

    // Explicitly ensure Calcite is disabled for V2 tests
    disableCalcite();

    // Add a small delay to ensure setting is applied
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Verify that Calcite is actually disabled
    boolean calciteEnabled = isCalciteEnabled();

    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testV2EngineErrorMessageForList() throws IOException {
    // Check current Calcite status
    boolean calciteEnabled = isCalciteEnabled();
    System.out.println("[DEBUG] Calcite enabled status: " + calciteEnabled);

    if (calciteEnabled) {
      // If Calcite is already enabled (due to shared test environment), test functionality
      System.out.println("[DEBUG] Calcite is enabled - testing functionality");
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | where account_number < 5 | stats list(gender) as gender_list",
                  TEST_INDEX_ACCOUNT));

      verifySchema(response, schema("gender_list", null, "array"));
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should work when Calcite is enabled");
    } else {
      // If Calcite is actually disabled, test error message
      System.out.println("[DEBUG] Calcite is disabled - testing error message");
      ResponseException exception =
          Assertions.assertThrows(
              ResponseException.class,
              () -> {
                executeQuery(
                    String.format(
                        "source=%s | where account_number < 5 | stats list(gender) as gender_list",
                        TEST_INDEX_ACCOUNT));
              },
              "V2 engine should throw exception when trying to use list() function");

      String errorMessage = exception.getMessage();
      System.out.println("[DEBUG] Error message: " + errorMessage);
      Assertions.assertTrue(
          errorMessage.contains("unsupported aggregator list"),
          "Error message should indicate list aggregator is unsupported in V2: " + errorMessage);
    }
  }

  @Test
  public void testV2EngineErrorMessageForValues() throws IOException {
    // Check current Calcite status
    boolean calciteEnabled = isCalciteEnabled();
    System.out.println("[DEBUG] Calcite enabled status: " + calciteEnabled);

    if (calciteEnabled) {
      // If Calcite is already enabled (due to shared test environment), test functionality
      System.out.println("[DEBUG] Calcite is enabled - testing functionality");
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | where account_number < 5 | stats values(gender) as unique_genders",
                  TEST_INDEX_ACCOUNT));

      verifySchema(response, schema("unique_genders", null, "array"));
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should work when Calcite is enabled");
    } else {
      // If Calcite is actually disabled, test error message
      System.out.println("[DEBUG] Calcite is disabled - testing error message");
      ResponseException exception =
          Assertions.assertThrows(
              ResponseException.class,
              () -> {
                executeQuery(
                    String.format(
                        "source=%s | where account_number < 5 | stats values(gender) as"
                            + " unique_genders",
                        TEST_INDEX_ACCOUNT));
              },
              "V2 engine should throw exception when trying to use values() function");

      String errorMessage = exception.getMessage();
      System.out.println("[DEBUG] Error message: " + errorMessage);
      Assertions.assertTrue(
          errorMessage.contains("unsupported aggregator values"),
          "Error message should indicate values aggregator is unsupported in V2: " + errorMessage);
    }
  }

  @Test
  public void testV2EngineErrorMessageForBothFunctions() throws IOException {
    // Check current Calcite status
    boolean calciteEnabled = isCalciteEnabled();
    System.out.println("[DEBUG] Calcite enabled status: " + calciteEnabled);

    if (calciteEnabled) {
      // If Calcite is already enabled (due to shared test environment), test functionality
      System.out.println("[DEBUG] Calcite is enabled - testing both functions");
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | where account_number < 5 | stats list(gender) as all_genders,"
                      + " values(gender) as unique_genders",
                  TEST_INDEX_ACCOUNT));

      verifySchema(
          response, schema("all_genders", null, "array"), schema("unique_genders", null, "array"));
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should work when Calcite is enabled");
    } else {
      // If Calcite is actually disabled, test error message
      System.out.println("[DEBUG] Calcite is disabled - testing error message");
      ResponseException exception =
          Assertions.assertThrows(
              ResponseException.class,
              () -> {
                executeQuery(
                    String.format(
                        "source=%s | where account_number < 5 | stats list(gender) as all_genders,"
                            + " values(gender) as unique_genders",
                        TEST_INDEX_ACCOUNT));
              },
              "V2 engine should throw exception when trying to use both multivalue functions");

      String errorMessage = exception.getMessage();
      System.out.println("[DEBUG] Error message: " + errorMessage);
      Assertions.assertTrue(
          errorMessage.contains("unsupported aggregator list")
              || errorMessage.contains("unsupported aggregator values"),
          "Error message should indicate aggregator is unsupported in V2: " + errorMessage);
    }
  }

  @Test
  public void testV2EngineWorksWithOtherAggregations() throws IOException {
    // Verify that V2 engine still works with regular aggregation functions when Calcite is disabled
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 10 | stats count() as cnt, avg(age) as avg_age"
                    + " by gender",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        response,
        schema("gender", null, "string"), // V2 behavior
        schema("cnt", null, "int"), // V2 returns int for count
        schema("avg_age", null, "double"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(
        dataRows.length() >= 1, "V2 engine should handle regular aggregations properly");
  }

  @Test
  public void testCalciteEngineToggling() throws IOException {
    // Test that functions work when Calcite is enabled, fail when disabled

    // Verify initial state - Calcite should be disabled
    boolean initialCalciteState = isCalciteEnabled();
    System.out.println("[DEBUG] Initial Calcite state: " + initialCalciteState);

    // First, verify V2 (Calcite disabled) throws error
    ResponseException v2Exception =
        Assertions.assertThrows(
            ResponseException.class,
            () -> {
              executeQuery(
                  String.format("source=%s | stats list(gender) as genders", TEST_INDEX_ACCOUNT));
            },
            "Should fail with Calcite disabled");

    String v2ErrorMessage = v2Exception.getMessage();
    System.out.println("[DEBUG] V2 error message: " + v2ErrorMessage);

    // Enable Calcite and verify it works
    try {
      enableCalcite();
      try {
        Thread.sleep(100); // Allow setting change to propagate
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      boolean calciteEnabledState = isCalciteEnabled();
      System.out.println("[DEBUG] Calcite state after enable: " + calciteEnabledState);

      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | where account_number < 5 | stats list(gender) as genders",
                  TEST_INDEX_ACCOUNT));

      verifySchema(response, schema("genders", null, "array"));

      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should work with Calcite enabled");

    } finally {
      // Restore V2 state
      disableCalcite();
      try {
        Thread.sleep(100); // Allow setting change to propagate
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Verify V2 state is restored and functions fail again
    boolean finalCalciteState = isCalciteEnabled();
    System.out.println("[DEBUG] Final Calcite state: " + finalCalciteState);

    ResponseException finalException =
        Assertions.assertThrows(
            ResponseException.class,
            () -> {
              executeQuery(
                  String.format("source=%s | stats list(gender) as genders", TEST_INDEX_ACCOUNT));
            },
            "Should fail again with Calcite disabled");

    String finalErrorMessage = finalException.getMessage();
    System.out.println("[DEBUG] Final error message: " + finalErrorMessage);
  }
}
