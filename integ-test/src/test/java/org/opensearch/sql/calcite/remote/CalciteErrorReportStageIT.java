/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for error report builder with stage tracking. Validates that errors include
 * stage information and user-friendly messages.
 */
public class CalciteErrorReportStageIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    enableCalcite();
  }

  @Test
  public void testFieldNotFoundErrorIncludesStage() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields nonexistent_field"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // Verify error has context with stage information
    assertTrue("Error should have context", error.has("context"));
    JSONObject context = error.getJSONObject("context");

    assertTrue("Context should have stage", context.has("stage"));
    assertEquals("Stage should be 'analyzing'", "analyzing", context.getString("stage"));

    assertTrue("Context should have stage_description", context.has("stage_description"));
    String stageDescription = context.getString("stage_description");
    assertTrue(
        "Stage description should be user-friendly",
        stageDescription.toLowerCase().contains("checking")
            || stageDescription.toLowerCase().contains("query"));

    // Verify error has location chain
    assertTrue("Error should have location", error.has("location"));
    assertTrue("Location should be an array", error.get("location") instanceof org.json.JSONArray);

    // Verify location message is user-friendly (not technical)
    org.json.JSONArray locationArray = error.getJSONArray("location");
    assertTrue("Location array should not be empty", locationArray.length() > 0);
    String location = locationArray.getString(0);
    assertFalse(
        "Location should not mention internal terms like 'Calcite'", location.contains("Calcite"));
    assertFalse(
        "Location should not mention internal terms like 'RelNode'", location.contains("RelNode"));
  }

  @Test
  public void testIndexNotFoundErrorIncludesStage() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class, () -> executeQuery("source=nonexistent_index | fields age"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // Verify error has context with stage
    assertTrue("Error should have context", error.has("context"));
    JSONObject context = error.getJSONObject("context");
    assertTrue("Context should have stage", context.has("stage"));

    // Verify error has location
    assertTrue("Error should have location", error.has("location"));
  }

  @Test
  public void testMultipleFieldErrorsIncludeStage() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source="
                        + TEST_INDEX_ACCOUNT
                        + " | fields nonexistent1, nonexistent2, nonexistent3"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // Verify stage information is present
    assertTrue("Error should have context", error.has("context"));
    JSONObject context = error.getJSONObject("context");
    assertTrue("Context should have stage", context.has("stage"));
    assertTrue("Context should have stage_description", context.has("stage_description"));
  }

  @Test
  public void testErrorReportTypeMatchesExceptionType() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields bad_field_name"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // Verify error has type field
    assertTrue("Error should have type", error.has("type"));

    // Verify error has details
    assertTrue("Error should have details", error.has("details"));
  }

  @Test
  public void testFieldNotFoundIncludesErrorCode() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields missing_field"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    String code = error.getString("code");
    assertFalse("Error code should not be empty", code.isEmpty());
    assertFalse("Error code should not be UNKNOWN", code.equals("UNKNOWN"));
  }

  @Test
  public void testLocationMessagesAreUserFriendly() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields xyz123"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    assertTrue("Error should have location", error.has("location"));
    org.json.JSONArray locationArray = error.getJSONArray("location");

    // Verify all location messages are user-friendly
    for (int i = 0; i < locationArray.length(); i++) {
      String location = locationArray.getString(i);

      // Should not contain technical terms
      assertFalse(
          "Location should not contain 'AST'",
          location.toLowerCase().contains("ast") && !location.toLowerCase().contains("last"));
      assertFalse("Location should not contain 'RelNode'", location.contains("RelNode"));
      assertFalse(
          "Location should not contain 'semantic analysis' (too technical)",
          location.contains("semantic analysis"));

      // Should use user-friendly language
      assertTrue(
          "Location should mention query, fields, data, cluster, or execution",
          location.toLowerCase().contains("query")
              || location.toLowerCase().contains("field")
              || location.toLowerCase().contains("data")
              || location.toLowerCase().contains("cluster")
              || location.toLowerCase().contains("execut"));
    }
  }

  @Test
  public void testStageDescriptionIsUserFriendly() throws IOException {
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields undefined_field"));

    String responseBody = getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    assertTrue("Error should have context", error.has("context"));
    JSONObject context = error.getJSONObject("context");
    assertTrue("Context should have stage_description", context.has("stage_description"));

    String stageDescription = context.getString("stage_description");

    // Stage description should not use compiler/technical terminology
    assertFalse(
        "Stage description should not contain 'Semantic'", stageDescription.contains("Semantic"));
    assertFalse(
        "Stage description should not contain 'Calcite'", stageDescription.contains("Calcite"));
    assertFalse(
        "Stage description should not contain 'AST'",
        stageDescription.contains("AST") && !stageDescription.contains("Last"));

    // Should use analyst-friendly language
    assertTrue(
        "Stage description should be user-friendly",
        stageDescription.toLowerCase().contains("check")
            || stageDescription.toLowerCase().contains("validat")
            || stageDescription.toLowerCase().contains("prepar")
            || stageDescription.toLowerCase().contains("run")
            || stageDescription.toLowerCase().contains("query"));
  }
}
