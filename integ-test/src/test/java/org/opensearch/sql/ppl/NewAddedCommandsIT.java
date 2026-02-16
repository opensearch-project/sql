/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MVEXPAND_EDGE_CASES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;

import java.io.IOException;
import org.json.JSONArray;
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
    loadIndex(Index.STRINGS);
    loadIndex(Index.MVEXPAND_EDGE_CASES);
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
  public void testRegexpMatch() throws IOException {
    // Test regexp_match with pattern that matches substring
    try {
      executeQuery(
          String.format(
              "source=%s | eval f=regexp_match(name, 'ell') | fields f", TEST_INDEX_STRINGS));
    } catch (ResponseException e) {
      JSONObject result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testRegexpReplace() throws IOException {
    // Test regexp_replace with pattern that matches substring
    try {
      executeQuery(
          String.format(
              "source=%s | eval f=regexp_replace(name, 'ell', '\1') | fields f",
              TEST_INDEX_STRINGS));
    } catch (ResponseException e) {
      JSONObject result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testAppend() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | stats count() by span(age, 10) | append [ search source=%s |"
                      + " stats avg(balance) by span(age, 10) ]",
                  TEST_INDEX_BANK, TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testStrftimeFunction() throws IOException {
    JSONObject result;
    try {
      executeQuery(
          String.format(
              "search source=%s | eval formatted_time = strftime(1521467703, '%s') | fields"
                  + " formatted_time",
              TEST_INDEX_BANK, "%Y-%m-%d"));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testAddTotalCommand() throws IOException {
    JSONObject result;
    try {
      executeQuery(String.format("search source=%s  | addtotals ", TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testAddColTotalCommand() throws IOException {
    JSONObject result;
    try {
      executeQuery(String.format("search source=%s  | addcoltotals ", TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testTransposeCommand() throws IOException {
    JSONObject result;
    try {
      executeQuery(String.format("search source=%s  | transpose ", TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
  }

  @Test
  public void testFieldFormatCommand() throws IOException {
    JSONObject result;
    try {
      executeQuery(
          String.format(
              "search source=%s  | fieldformat double_balance = balance * 2 ", TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
      verifyQuery(result);
    }
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

  @Test
  public void testMvCombineUnsupportedInV2() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "source=%s | fields state, city, age | mvcombine age", TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testNoMvUnsupportedInV2() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "source=%s | fields account_number, firstname | eval names = array(firstname) |"
                      + " nomv names",
                  TEST_INDEX_BANK));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }
    verifyQuery(result);
  }

  @Test
  public void testMvExpandCommandBasicExpansion() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | mvexpand skills | where username='happy' | fields username,"
                      + " skills.name | sort skills.name",
                  TEST_INDEX_MVEXPAND_EDGE_CASES));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }

    if (isCalciteEnabled()) {
      assertThat(result.getJSONArray("datarows").length(), equalTo(3));

      JSONArray datarows = result.getJSONArray("datarows");
      assertThat(datarows.getJSONArray(0).getString(0), equalTo("happy"));
      assertThat(datarows.getJSONArray(0).getString(1), equalTo("java"));
      assertThat(datarows.getJSONArray(1).getString(1), equalTo("python"));
      assertThat(datarows.getJSONArray(2).getString(1), equalTo("sql"));
    } else {
      JSONObject error = result.getJSONObject("error");
      assertThat(
          error.getString("details"),
          containsString(
              "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true"));
      assertThat(error.getString("type"), equalTo("UnsupportedOperationException"));
    }
  }

  @Test
  public void testMvExpandCommandNullInput() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | mvexpand skills | where username='nullskills' | fields"
                      + " username, skills.name",
                  TEST_INDEX_MVEXPAND_EDGE_CASES));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }

    if (isCalciteEnabled()) {
      assertThat(result.getJSONArray("datarows").length(), equalTo(0));
    } else {
      JSONObject error = result.getJSONObject("error");
      assertThat(
          error.getString("details"),
          containsString(
              "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true"));
      assertThat(error.getString("type"), equalTo("UnsupportedOperationException"));
    }
  }

  @Test
  public void testMvExpandCommandEmptyArray() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | mvexpand skills | where username='empty' | fields username,"
                      + " skills.name",
                  TEST_INDEX_MVEXPAND_EDGE_CASES));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }

    if (isCalciteEnabled()) {
      assertThat(result.getJSONArray("datarows").length(), equalTo(0));
    } else {
      JSONObject error = result.getJSONObject("error");
      assertThat(
          error.getString("details"),
          containsString(
              "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true"));
      assertThat(error.getString("type"), equalTo("UnsupportedOperationException"));
    }
  }

  @Test
  public void testMvExpandCommandNonArrayField() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | mvexpand skills_not_array | where username='u1' | fields"
                      + " username, skills_not_array",
                  TEST_INDEX_MVEXPAND_EDGE_CASES));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }

    if (isCalciteEnabled()) {
      assertThat(result.getJSONArray("datarows").length(), equalTo(1));
      assertThat(result.getJSONArray("datarows").getJSONArray(0).getString(1), equalTo("scala"));
    } else {
      JSONObject error = result.getJSONObject("error");
      assertThat(
          error.getString("details"),
          containsString(
              "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true"));
      assertThat(error.getString("type"), equalTo("UnsupportedOperationException"));
    }
  }

  @Test
  public void testMvExpandCommandLimitBoundary() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | mvexpand skills limit=3 | where username='limituser' | fields"
                      + " username, skills.name",
                  TEST_INDEX_MVEXPAND_EDGE_CASES));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }

    if (isCalciteEnabled()) {
      assertThat(result.getJSONArray("datarows").length(), equalTo(3));
    } else {
      JSONObject error = result.getJSONObject("error");
      assertThat(
          error.getString("details"),
          containsString(
              "is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true"));
      assertThat(error.getString("type"), equalTo("UnsupportedOperationException"));
    }
  }

  @Test
  public void testMvExpandCommandMultiDocument() throws IOException {
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "search source=%s | mvexpand skills | where username='happy' OR username='single'"
                      + " | fields username, skills.name | sort username, skills.name",
                  TEST_INDEX_MVEXPAND_EDGE_CASES));
    } catch (ResponseException e) {
      result = new JSONObject(TestUtils.getResponseBody(e.getResponse()));
    }

    if (isCalciteEnabled()) {
      assertThat(result.getJSONArray("datarows").length(), equalTo(4));

      JSONArray datarows = result.getJSONArray("datarows");
      assertThat(datarows.getJSONArray(0).getString(0), equalTo("happy"));
      assertThat(datarows.getJSONArray(3).getString(0), equalTo("single"));
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
