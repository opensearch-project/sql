/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.util.List;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testSqrtAndPow() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where sqrt(pow(age, 2)) = 30.0 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  // Test
  @Test
  public void testConcat() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat('He', 'llo') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testConcatWithField() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat('Hello', state) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, (Matcher<JSONArray>) List.of());
  }

  @Test
  public void testConcatWs() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where name=concat('Hello', state) | fields name, age",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows());
  }

  @Test
  public void testLength() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where length(name) = 5 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLengthShouldBeInsensitive() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where leNgTh(name) = 5 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLower() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where lower(name) = 'hello' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testUpper() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where upper(name) = upper('hello') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLike() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where like(name, '_ello%%') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testSubstring() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where substring(name, 2, 2) = 'el' | fields name, age",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

}
