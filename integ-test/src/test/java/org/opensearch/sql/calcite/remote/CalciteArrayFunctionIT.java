/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteArrayFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
    loadIndex(Index.ARRAY);
  }

  @Test
  public void testArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1.5, 2, 1.0) | head 1 | fields array",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("array", "array"));

    verifyDataRows(actual, rows(List.of(1, -1.5, 2, 1.0)));
  }

  @Test
  public void testArrayWithString() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 'demo') | head 1 | fields array",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("array", "array"));

    verifyDataRows(actual, rows(List.of("1", "demo")));
  }

  @Test
  public void testArrayWithMix() {
    Class<? extends Exception> expectedException =
        isStandaloneTest() ? RuntimeException.class : ResponseException.class;
    Exception e =
        assertThrows(
            expectedException,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval array = array(1, true) | head 1 | fields array",
                        TEST_INDEX_BANK)));

    verifyErrorMessageContains(
        e,
        "Cannot resolve function: ARRAY, arguments: [INTEGER,BOOLEAN], caused by: fail to create"
            + " array with fixed type");
  }

  @Test
  public void testArrayLength() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1.5, 2, 1.0) | eval length ="
                    + " array_length(array) | head 1 | fields length",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("length", "int"));

    verifyDataRows(actual, rows(4));
  }

  @Test
  public void testForAll() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = forall(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "boolean"));

    verifyDataRows(actual, rows(false));
  }

  @Test
  public void testExists() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = exists(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "boolean"));

    verifyDataRows(actual, rows(true));
  }

  @Test
  public void testFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = filter(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 2)));
  }

  @Test
  public void testTransform() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = transform(array, x -> x + 1) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(2, 3, 4)));
  }

  @Test
  public void testTransformForTwoInput() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = transform(array, (x, i) -> x +"
                    + " i) | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 3, 5)));
  }

  @Test
  public void testTransformForWithDouble() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = transform(array, (x, i) -> x +"
                    + " i * 10.1) | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 12.1, 23.2)));
  }

  @Test
  public void testTransformForWithUDF() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(TIMESTAMP('2000-01-02 00:00:00'),"
                    + " TIMESTAMP('2000-01-03 00:00:00'), TIMESTAMP('2000-01-04 00:00:00')), result"
                    + " = transform(array, (x, i) -> DATEDIFF(x, TIMESTAMP('2000-01-01 23:59:59'))"
                    + " + i * 10.1) | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "array"));

    verifyDataRows(actual, rows(List.of(1, 12.1, 23.2)));
  }

  @Test
  public void testReduce() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc"
                    + " + x), result2 = reduce(array, 10, (acc, x) -> acc + x), result3 ="
                    + " reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10.0) | fields"
                    + " result,result2, result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(
        actual, schema("result", "int"), schema("result2", "int"), schema("result3", "double"));

    verifyDataRows(actual, rows(6, 16, 60));
  }

  @Test
  public void testReduce2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1.0, 2.0, 3.0), result3 = reduce(array, 0, (acc, x)"
                    + " -> acc * 10.0 + x, acc -> acc * 10.0) | fields result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result3", "double"));

    verifyDataRows(actual, rows(1230));
  }

  @Test
  public void testReduce3() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age=28 | eval array = array(1.0, 2.0, 3.0), result3 ="
                    + " reduce(array, age, (acc, x) -> acc * 1.0 + x, acc -> acc * 10.0) | fields"
                    + " result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result3", "double"));

    verifyDataRows(actual, rows(340));
  }

  @Test
  public void testReduceWithUDF() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array('a', 'ab', 'abc'), result3 = reduce(array, 0, (acc,"
                    + " x) -> acc + length(x), acc -> acc * 10.0) | fields result3 | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result3", "double"));

    verifyDataRows(actual, rows(60));
  }

  @Test
  public void testMvjoinWithStringArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('a', 'b', 'c'), ',') | fields result | head"
                    + " 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("a,b,c"));
  }

  @Test
  public void testMvjoinWithStringifiedNumbers() throws IOException {
    // Note: mvjoin only supports string arrays
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('1', '2', '3'), ' | ') | fields result |"
                    + " head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("1 | 2 | 3"));
  }

  @Test
  public void testMvjoinWithMixedStringValues() throws IOException {
    // mvjoin only supports string arrays
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('1', 'text', '2.5'), ';') | fields result |"
                    + " head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("1;text;2.5"));
  }

  @Test
  public void testMvjoinWithEmptyArray() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array(), '-') | fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows(""));
  }

  @Test
  public void testMvjoinWithStringBooleans() throws IOException {
    // mvjoin only supports string arrays
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('true', 'false', 'true'), '|') | fields"
                    + " result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("true|false|true"));
  }

  @Test
  public void testMvjoinWithSpecialDelimiters() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = mvjoin(array('apple', 'banana', 'cherry'), ' AND ') |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", "string"));
    verifyDataRows(actual, rows("apple AND banana AND cherry"));
  }

  @Test
  public void testMvjoinWithArrayFromRealFields() throws IOException {
    // Test mvjoin on arrays created from real fields using array() function
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval names_array = array(firstname, lastname) | eval result ="
                    + " mvjoin(names_array, ',') | fields firstname, lastname, result | head 1",
                TEST_INDEX_BANK));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("result", "string"));
    // Verify that mvjoin correctly joins the firstname and lastname fields
    JSONArray dataRows = actual.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    JSONArray firstRow = dataRows.getJSONArray(0);
    assertEquals(firstRow.getString(0) + "," + firstRow.getString(1), firstRow.getString(2));
  }

  @Test
  public void testMvjoinWithMultipleRealFields() throws IOException {
    // Test mvjoin with arrays created from multiple real fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval info_array = array(city, state, employer) | eval result ="
                    + " mvjoin(info_array, ' | ') | fields city, state, employer, result | head 1",
                TEST_INDEX_BANK));

    verifySchema(
        actual,
        schema("city", "string"),
        schema("state", "string"),
        schema("employer", "string"),
        schema("result", "string"));
    // Verify that mvjoin correctly joins the city, state, and employer fields
    JSONArray dataRows = actual.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    JSONArray firstRow = dataRows.getJSONArray(0);
    assertEquals(
        firstRow.getString(0) + " | " + firstRow.getString(1) + " | " + firstRow.getString(2),
        firstRow.getString(3));
  }
}
