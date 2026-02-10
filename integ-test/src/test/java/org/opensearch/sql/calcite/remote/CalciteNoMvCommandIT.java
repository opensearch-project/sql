/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteNoMvCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  // ---------------------------
  // Sanity (precondition)
  // ---------------------------

  @Test
  public void testSanityDatasetIsLoaded() throws IOException {
    JSONObject result = executeQuery("source=" + TEST_INDEX_BANK + " | head 5");
    int rows = result.getJSONArray("datarows").length();
    Assertions.assertTrue(rows > 0, "Expected bank dataset to have rows, got 0");
  }

  // ---------------------------
  // Happy path (core nomv)
  // ---------------------------

  @Test
  public void testNoMvBasicUsageWithArrayLiterals() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr = array('web', 'production', 'east') | nomv arr | head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows("web\nproduction\neast"));
  }

  @Test
  public void testNoMvWithArrayFromFields() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval names = array(firstname, lastname) | nomv names | head 1 | fields"
            + " firstname, lastname, names";

    JSONObject result = executeQuery(q);

    verifySchema(
        result,
        schema("firstname", null, "string"),
        schema("lastname", null, "string"),
        schema("names", null, "string"));

    verifyDataRows(
        result, rows("Amber JOHnny", "Duke Willmington", "Amber JOHnny\nDuke Willmington"));
  }

  @Test
  public void testNoMvMultipleArraysAppliedInSequence() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr1 = array('a', 'b'), arr2 = array('x', 'y') | nomv arr1 | nomv arr2 |"
            + " head 1 | fields arr1, arr2";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr1", null, "string"), schema("arr2", null, "string"));

    verifyDataRows(result, rows("a\nb", "x\ny"));
  }

  @Test
  public void testNoMvInComplexPipelineWithWhereAndSort() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number < 20 | eval arr = array(firstname, 'test') | nomv arr |"
            + " sort account_number | head 3 | fields account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    verifyDataRows(
        result, rows(1, "Amber JOHnny\ntest"), rows(6, "Hattie\ntest"), rows(13, "Nanette\ntest"));
  }

  @Test
  public void testNoMvFieldUsableInSubsequentOperations() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number = 6 | eval arr = array('test', 'data') | nomv arr | eval"
            + " arr_len = length(arr) | fields account_number, arr, arr_len";

    JSONObject result = executeQuery(q);

    verifySchema(
        result,
        schema("account_number", null, "bigint"),
        schema("arr", null, "string"),
        schema("arr_len", null, "int"));

    verifyDataRows(result, rows(6, "test\ndata", 9));
  }

  @Test
  public void testNoMvWithStatsAfterAggregation() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | stats count() as cnt by age | eval age_str = cast(age as string) | eval arr ="
            + " array(age_str, 'count') | nomv arr | fields cnt, age, arr | sort cnt | head 2";

    JSONObject result = executeQuery(q);

    verifySchema(
        result,
        schema("cnt", null, "bigint"),
        schema("age", null, "int"),
        schema("arr", null, "string"));

    Assertions.assertTrue(result.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testNoMvWithEvalWorksOnComputedArrays() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number = 1 | eval full_name = concat(firstname, ' ', lastname) |"
            + " eval arr = array(full_name, 'suffix') | nomv arr | fields full_name, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("full_name", null, "string"), schema("arr", null, "string"));

    verifyDataRows(
        result, rows("Amber JOHnny Duke Willmington", "Amber JOHnny Duke Willmington\nsuffix"));
  }

  @Test
  public void testNoMvPreservesFieldInPlace() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr = array('a', 'b', 'c') | nomv arr | head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows("a\nb\nc"));

    Assertions.assertEquals(1, result.getJSONArray("schema").length());
  }

  // ---------------------------
  // Edge case / error semantics
  // ---------------------------

  @Test
  public void testNoMvSingleElementArray() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr = array('single') | nomv arr | head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows("single"));
  }

  @Test
  public void testNoMvEmptyArray() throws IOException {
    String q =
        "source=" + TEST_INDEX_BANK + " | eval arr = array() | nomv arr | head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows(""));
  }

  @Test
  public void testNoMvArrayWithNullValues() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr = array('first', 'second', 'third') | nomv arr | head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows("first\nsecond\nthird"));
  }

  @Test
  public void testNoMvScalarFieldError() throws IOException {
    ResponseException ex =
        Assertions.assertThrows(
            ResponseException.class,
            () ->
                executeQuery("source=" + TEST_INDEX_BANK + " | fields firstname | nomv firstname"));

    int status = ex.getResponse().getStatusLine().getStatusCode();
    Assertions.assertEquals(400, status, "Expected 400 for type mismatch");

    String msg = ex.getMessage();

    Assertions.assertTrue(
        msg.contains("MVJOIN") || msg.contains("ARRAY") || msg.contains("type"), msg);
  }

  @Test
  public void testNoMvArrayWithMixedTypes() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number = 1 | eval arr = array('age:', cast(age as string)) | nomv"
            + " arr | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows("age:\n32"));
  }

  @Test
  public void testNoMvLargeArray() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr = array('1', '2', '3', '4', '5', '6', '7', '8', '9', '10') | nomv arr |"
            + " head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows("1\n2\n3\n4\n5\n6\n7\n8\n9\n10"));
  }

  @Test
  public void testNoMvResultUsedInComparison() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval arr = array('test') | nomv arr | where arr = 'test' | head 1 | fields"
            + " account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    Assertions.assertTrue(result.getJSONArray("datarows").length() > 0);
  }

  // ---------------------------
  // Edge case / error semantics
  // ---------------------------

  @Test
  public void testNoMvMissingFieldShouldReturn4xx() throws IOException {
    // Error when field does not exist
    ResponseException ex =
        Assertions.assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_BANK + " | nomv does_not_exist"));

    int status = ex.getResponse().getStatusLine().getStatusCode();

    Assertions.assertEquals(400, status, "Unexpected status. ex=" + ex.getMessage());

    String msg = ex.getMessage();
    Assertions.assertTrue(
        msg.contains("does_not_exist") || msg.contains("field") || msg.contains("Field"), msg);
  }
}
