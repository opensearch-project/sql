/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;

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
    loadIndex(Index.BANK_WITH_STRING_VALUES);
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
  public void testRegexMatch() throws IOException {
    // Test regex_match with pattern that matches substring
    JSONObject result;
    try {

      String query1 =
          String.format(
              "source=%s | eval f=regex_match(name, 'ell') | fields f", TEST_INDEX_STRINGS);
      result = executeQuery(query1);
      result =
          executeQuery(
              String.format(
                  "search source=%s | where firstname = [ source=%s | where holdersName='Hattie'"
                      + " | fields holdersName | head 1]",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      if (isCalciteEnabled()) {
        assertFalse(result.getJSONArray("datarows").isEmpty());
      } else {
        JSONObject error = result.getJSONObject("error");
        assertThat(
            error.getString("details"), containsString("unsupported function name: regex_match"));
      }
    }
  }

  @Test
  public void testEarliestLatestAggregates() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | stats earliest(firstname) as earliest_name,"
                      + " latest(firstname) as latest_name by gender",
                  TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testMaxByMinByAggregates() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | stats max_by(firstname, age) as oldest_name,"
                      + " min_by(firstname, age) as youngest_name by gender",
                  TEST_INDEX_BANK));
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
      assertThat(
          error.getString("details"),
          containsString(
              "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true"));
      assertThat(error.getString("type"), equalTo("UnsupportedOperationException"));
    }
  }
}
