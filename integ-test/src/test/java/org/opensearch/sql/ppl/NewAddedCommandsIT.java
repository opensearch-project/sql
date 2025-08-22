/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.TestUtils;

public class NewAddedCommandsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.DOG);
  }

  @Test
  public void testJoin() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | join on firstname=holdersName %s",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testLookup() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | lookup %s holdersName as firstname",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testSubsearch() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=[source=%s | where age>35 | fields age] as t", TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);

    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | where exists [ source=%s | where firstname=holdersName]",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);

    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | where firstname in [ source=%s | fields holdersName]",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);

    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | where firstname = [ source=%s | where holdersName='Hattie'"
                      + " | fields holdersName | head 1]",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testAppendcol() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | stats count() by span(age, 10) | appendcol [ stats"
                      + " avg(balance) by span(age, 10) ]",
                  TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testStrftimeFunction() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | eval formatted_time = strftime(1521467703, '%s') | fields"
                      + " formatted_time",
                  TEST_INDEX_BANK, "%Y-%m-%d"));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  private void verifyQuery(JSONObject result) throws IOException {
    if (isCalciteEnabled()) {
      assertFalse(result.getJSONArray("datarows").isEmpty());
    } else {
      JSONObject error = result.getJSONObject("error");
      // STRFTIME gets a different error message since it's not recognized at all in legacy engine
      String errorDetails = error.getString("details");
      assertTrue(
          "Expected error message about Calcite or unsupported function, but got: " + errorDetails,
          errorDetails.contains(
                  "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true")
              || errorDetails.contains("unsupported function name: strftime"));
      // Error type can vary based on how the error is caught
      String errorType = error.getString("type");
      assertTrue(
          "Expected UnsupportedOperationException or SemanticCheckException, but got: " + errorType,
          errorType.equals("UnsupportedOperationException")
              || errorType.equals("SemanticCheckException")
              || errorType.equals(
                  "ExpressionEvaluationException")); // Add this as another possible error type
    }
  }
}
