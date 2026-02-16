/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
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
    loadIndex(Index.BANK_WITH_NULL_VALUES);
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
  public void testNoMvBasicUsageFromRFC() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number=1 | eval names = array(firstname, lastname) | nomv names |"
            + " fields account_number, names";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("names", null, "string"));

    verifyDataRows(result, rows(1, "Amber JOHnny\nDuke Willmington"));
  }

  @Test
  public void testNoMvEvalCreatedFieldFromRFC() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number=1 | eval location = array(city, state) | nomv location |"
            + " fields account_number, location";

    JSONObject result = executeQuery(q);

    verifySchema(
        result, schema("account_number", null, "bigint"), schema("location", null, "string"));

    verifyDataRows(result, rows(1, "Brogan\nIL"));
  }

  // ---------------------------
  // Additional nomv tests
  // ---------------------------

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
  public void testNoMvEmptyArray() throws IOException {
    String q =
        "source=" + TEST_INDEX_BANK + " | eval arr = array() | nomv arr | head 1 | fields arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("arr", null, "string"));

    verifyDataRows(result, rows(""));
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

  @Test
  public void testNoMvMissingFieldShouldReturn4xx() throws IOException {
    ResponseException ex =
        Assertions.assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + TEST_INDEX_BANK + " | nomv does_not_exist"));

    int status = ex.getResponse().getStatusLine().getStatusCode();

    Assertions.assertEquals(400, status, "Unexpected status. ex=" + ex.getMessage());

    String msg = ex.getMessage();
    Assertions.assertTrue(
        msg.contains("does_not_exist")
            || msg.contains("field")
            || msg.contains("Field")
            || msg.contains("ARRAY_COMPACT")
            || msg.contains("ARRAY"),
        msg);
  }

  @Test
  public void testNoMvWithNullInMiddleOfArray() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK_WITH_NULL_VALUES
            + " | where account_number = 25 | eval arr = array(firstname, age, lastname) | nomv"
            + " arr | fields account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    verifyDataRows(result, rows(25, "Virginia\nAyala"));
  }

  @Test
  public void testNoMvWithNullAtBeginningAndEnd() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK_WITH_NULL_VALUES
            + " | where account_number = 25 | eval arr = array(age, firstname, age) | nomv arr |"
            + " fields account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    verifyDataRows(result, rows(25, "Virginia"));
  }

  @Test
  public void testNoMvWithAllNulls() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK_WITH_NULL_VALUES
            + " | where account_number = 25 | eval arr = array(age, age, age) | nomv arr | fields"
            + " account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    verifyDataRows(result, rows(25, ""));
  }

  @Test
  public void testNoMvArrayWithAllNulls() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK_WITH_NULL_VALUES
            + " | where account_number = 25 | eval arr = array(age, age, age) | nomv arr | fields"
            + " account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    verifyDataRows(result, rows(25, ""));
  }

  @Test
  public void testNoMvMultipleRowsRowLocalBehavior() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval tags = array(firstname, lastname) | nomv tags | sort account_number | head"
            + " 3 | fields account_number, tags";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("tags", null, "string"));

    verifyDataRows(
        result,
        rows(1, "Amber JOHnny\nDuke Willmington"),
        rows(6, "Hattie\nBond"),
        rows(13, "Nanette\nBates"));
  }

  @Test
  public void testNoMvNonConsecutiveRowsNoGrouping() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | where account_number = 1 or account_number = 6 or account_number = 13 | eval"
            + " tags = array(firstname, city) | nomv tags | sort account_number | fields"
            + " account_number, tags";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("tags", null, "string"));

    verifyDataRows(
        result,
        rows(1, "Amber JOHnny\nBrogan"),
        rows(6, "Hattie\nDante"),
        rows(13, "Nanette\nNogal"));
  }

  @Test
  public void testNoMvNullFieldValue() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK_WITH_NULL_VALUES
            + " | where account_number = 6 | eval balance_str = cast(balance as string) | eval arr"
            + " = array(balance_str) | nomv arr | fields account_number, arr";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("account_number", null, "bigint"), schema("arr", null, "string"));

    verifyDataRows(result, rows(6, ""));
  }

  @Test
  public void testNoMvArrayWithEmptyStrings() throws IOException {
    String q =
        "source="
            + TEST_INDEX_BANK
            + " | eval tags = array('a', '', 'b') | nomv tags | head 1 | fields tags";

    JSONObject result = executeQuery(q);

    verifySchema(result, schema("tags", null, "string"));

    verifyDataRows(result, rows("a\n\nb"));
  }
}
