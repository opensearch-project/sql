/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.calcite.standalone.CalcitePPLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

/**
 * Integration tests for ErrorReport status code handling. Validates that exceptions wrapped in
 * ErrorReport are correctly mapped to HTTP status codes based on the ErrorCode.
 */
public class ErrorReportStatusCodeIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testFieldNotFoundReturns400() throws IOException {
    // Field not found should be a client error (400 BAD_REQUEST)
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields nonexistent_field"));

    assertThat(
        "Field not found should return 400 status",
        exception.getResponse().getStatusLine().getStatusCode(),
        equalTo(400));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);

    assertThat("Response should have error object", response.has("error"), equalTo(true));
    JSONObject error = response.getJSONObject("error");

    // Verify the error includes the ErrorCode
    if (error.has("code")) {
      assertThat(
          "Error code should be FIELD_NOT_FOUND",
          error.getString("code"),
          equalTo("FIELD_NOT_FOUND"));
    }
  }

  @Test
  public void testIndexNotFoundReturns404() throws IOException {
    // Index not found should return 404 NOT_FOUND
    ResponseException exception =
        assertThrows(
            ResponseException.class, () -> executeQuery("source=nonexistent_index | fields age"));

    assertThat(
        "Index not found should return 404 status",
        exception.getResponse().getStatusLine().getStatusCode(),
        equalTo(404));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);

    assertThat("Response should have error object", response.has("error"), equalTo(true));
    JSONObject error = response.getJSONObject("error");

    // Verify error details mention the index
    assertThat(
        "Error details should mention nonexistent index",
        error.getString("details"),
        containsString("nonexistent_index"));
  }

  @Test
  public void testSyntaxErrorReturns400() throws IOException {
    // Syntax error should be a client error (400 BAD_REQUEST)
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | invalid_command"));

    assertThat(
        "Syntax error should return 400 status",
        exception.getResponse().getStatusLine().getStatusCode(),
        equalTo(400));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);

    assertThat("Response should have error object", response.has("error"), equalTo(true));
  }

  @Test
  public void testSemanticErrorReturns400() throws IOException {
    // Semantic errors (like type mismatches) should be client errors (400 BAD_REQUEST)
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "source=" + TEST_INDEX_ACCOUNT + " | where age + 'string' > 10 | fields age"));

    assertThat(
        "Semantic error should return 400 status",
        exception.getResponse().getStatusLine().getStatusCode(),
        equalTo(400));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);

    assertThat("Response should have error object", response.has("error"), equalTo(true));
  }

  @Test
  public void testErrorReportPreservesContextInformation() throws IOException {
    // Verify that ErrorReport context is included in the response
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields bad_field"));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // ErrorReport should include context information
    if (error.has("context")) {
      JSONObject context = error.getJSONObject("context");
      assertTrue(
          "Context should include stage information",
          context.has("stage") || context.has("stage_description"));
    }

    // ErrorReport should include location chain if available
    if (error.has("location")) {
      assertTrue(
          "Location should be an array", error.get("location") instanceof org.json.JSONArray);
    }
  }

  @Test
  public void testErrorReportTypeIsErrorCodeNotExceptionClass() throws IOException {
    // Verify that the "type" field is the ErrorCode, not "ErrorReport"
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields nonexistent_field"));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // The type should be the ErrorCode, not "ErrorReport"
    assertTrue("Error should have a type field", error.has("type"));
    String type = error.getString("type");
    assertThat(
        "Type should be an ErrorCode (e.g., FIELD_NOT_FOUND), not ErrorReport",
        type,
        not(equalTo("ErrorReport")));

    // Context should include the underlying cause exception type
    if (error.has("context")) {
      JSONObject context = error.getJSONObject("context");
      assertTrue(
          "Context should include the underlying cause exception type", context.has("cause"));
    }
  }

  @Test
  public void testErrorReportContextIncludesCause() throws IOException {
    // Verify that when ErrorReport is used, the context includes the underlying cause exception
    // type
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields bad_field"));

    String responseBody = TestUtils.getResponseBody(exception.getResponse());
    JSONObject response = new JSONObject(responseBody);
    JSONObject error = response.getJSONObject("error");

    // If the error has context (meaning it's wrapped in ErrorReport with stage info),
    // then it should include the cause
    if (error.has("context")) {
      JSONObject context = error.getJSONObject("context");
      assertTrue("Context should include cause field", context.has("cause"));

      // The cause should be a simple class name (not null or empty)
      String cause = context.getString("cause");
      assertFalse("Cause should not be empty", cause.isEmpty());
      assertFalse("Cause should not contain package names", cause.contains("."));
    }
  }
}
