/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMVAppendFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  @Test
  public void testMvappendWithMultipleElements() throws IOException {
    JSONObject actual =
        executeQuery(
            source(TEST_INDEX_BANK, "eval result = mvappend(1, 2, 3) | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2, 3)));
  }

  @Test
  public void testMvappendWithSingleElement() throws IOException {
    JSONObject actual =
        executeQuery(
            source(TEST_INDEX_BANK, "eval result = mvappend(42) | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(42)));
  }

  @Test
  public void testMvappendWithArrayFlattening() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval arr1 = array(1, 2), arr2 = array(3, 4), result = mvappend(arr1, arr2) | head"
                    + " 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2, 3, 4)));
  }

  @Test
  public void testMvappendWithMixedArrayAndScalar() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval arr = array(1, 2), result = mvappend(arr, 3, 4) | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2, 3, 4)));
  }

  @Test
  public void testMvappendWithStringValues() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval result = mvappend('hello', 'world') | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("hello", "world")));
  }

  @Test
  public void testMvappendWithMixedTypes() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval result = mvappend(1, 'text', 2.5) | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("1", "text", "2.5")));
  }

  @Test
  public void testMvappendWithIntAndDouble() throws IOException {
    JSONObject actual =
        executeQuery(
            source(TEST_INDEX_BANK, "eval result = mvappend(1, 2.5) | head 1 | fields result"));

    System.out.println(actual);

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2.5)));
  }

  @Test
  public void testMvappendWithRealFields() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval result = mvappend(firstname, lastname) | head 1 | fields firstname, lastname,"
                    + " result"));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("result", "array"));

    verifyDataRows(
        actual,
        rows("Amber JOHnny", "Duke Willmington", List.of("Amber JOHnny", "Duke Willmington")));
  }

  @Test
  public void testMvappendWithFieldsAndLiterals() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval result = mvappend(age, 'years', 'old') | head 1 | fields age, result"));

    verifySchema(actual, schema("age", "int"), schema("result", "array"));
    verifyDataRows(actual, rows(32, List.of("32", "years", "old")));
  }

  @Test
  public void testMvappendWithEmptyArray() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval empty_arr = array(), result = mvappend(empty_arr, 1, 2) | head 1 | fields"
                    + " result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1, 2)));
  }

  @Test
  public void testMvappendWithNestedArrays() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval arr1 = array('a', 'b'), arr2 = array('c'), arr3 = array('d', 'e'), result ="
                    + " mvappend(arr1, arr2, arr3) | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of("a", "b", "c", "d", "e")));
  }

  @Test
  public void testMvappendWithNumericArrays() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval arr1 = array(1.5, 2.5), arr2 = array(3.5), result = mvappend(arr1, arr2, 4.5)"
                    + " | head 1 | fields result"));

    verifySchema(actual, schema("result", "array"));
    verifyDataRows(actual, rows(List.of(1.5, 2.5, 3.5, 4.5)));
  }

  @Test
  public void testMvappendInWhereClause() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval combined = mvappend(firstname, lastname) | where array_length(combined) = 2 |"
                    + " head 1 | fields firstname, lastname, combined"));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("combined", "array"));

    verifyDataRows(
        actual,
        rows("Amber JOHnny", "Duke Willmington", List.of("Amber JOHnny", "Duke Willmington")));
  }

  @Test
  public void testMvappendWithComplexExpression() throws IOException {
    JSONObject actual =
        executeQuery(
            source(
                TEST_INDEX_BANK,
                "eval result = mvappend(array(age), array(age * 2), age + 10) | head 1 | fields"
                    + " age, result"));

    verifySchema(actual, schema("age", "int"), schema("result", "array"));
    verifyDataRows(actual, rows(32, List.of(32, 64, 42)));
  }
}
