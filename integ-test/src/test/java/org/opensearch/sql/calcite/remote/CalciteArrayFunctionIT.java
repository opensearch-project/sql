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
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteArrayFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.BANK);
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
            + " array with fixed type: inferred array element type");
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
}
