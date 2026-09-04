/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NULL_MISSING;
import static org.opensearch.sql.util.Capability.EVAL_MAX_MIN_INT_WIDENING;
import static org.opensearch.sql.util.Capability.EVAL_MAX_MIN_MIXED_TYPES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

public class CalcitePPLEvalMaxMinFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.DOG);
    loadIndex(Index.NULL_MISSING);
  }

  // `new` has no plan-time type, so its reported type is taken from the first result row. Filtering
  // to a single row makes that row deterministic under any shard count. Both directions are kept so
  // the int-selected and bigint-selected results stay covered.
  @Test
  @RequiresCapability(
      value = EVAL_MAX_MIN_INT_WIDENING,
      note = "max(1, 3, age) selects an int-valued result; the AE route reports it as bigint.")
  public void testEvalMaxNumeric() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age = 2 | eval new = max(1, 3, age) | fields age, new",
                TEST_INDEX_DOG));
    verifySchema(result, schema("age", "bigint"), schema("new", "int"));
    verifyDataRows(result, rows(2, 3));
  }

  @Test
  public void testEvalMaxNumericWhenFieldSelected() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age = 4 | eval new = max(1, 3, age) | fields age, new",
                TEST_INDEX_DOG));
    verifySchema(result, schema("age", "bigint"), schema("new", "bigint"));
    verifyDataRows(result, rows(4, 4));
  }

  @Test
  public void testEvalMaxString() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval new = max('apple', 'sam', dog_name) | fields dog_name, new",
                TEST_INDEX_DOG));
    verifySchema(result, schema("dog_name", "string"), schema("new", "string"));
    verifyDataRows(result, rows("rex", "sam"), rows("snoopy", "snoopy"));
  }

  @Test
  @RequiresCapability(
      value = EVAL_MAX_MIN_MIXED_TYPES,
      note =
          "Mixed numeric+string operands -> 'Cannot infer return type for GREATEST' on the AE"
              + " route.")
  public void testEvalMaxNumericAndString() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval new = max(14, age, 'Fred', holdersName) | fields age,"
                    + " holdersName, new",
                TEST_INDEX_DOG));
    verifySchema(
        result, schema("holdersName", "string"), schema("age", "bigint"), schema("new", "string"));
    verifyDataRows(result, rows(2, "Daenerys", "Fred"), rows(4, "Hattie", "Hattie"));
  }

  @Test
  public void testEvalMinNumeric() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age = 2 | eval new = min(14, 3, age) | fields age, new",
                TEST_INDEX_DOG));
    verifySchema(result, schema("age", "bigint"), schema("new", "bigint"));
    verifyDataRows(result, rows(2, 2));
  }

  @Test
  @RequiresCapability(
      value = EVAL_MAX_MIN_INT_WIDENING,
      note = "min(14, 3, age) selects an int-valued result; the AE route reports it as bigint.")
  public void testEvalMinNumericWhenLiteralSelected() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age = 4 | eval new = min(14, 3, age) | fields age, new",
                TEST_INDEX_DOG));
    verifySchema(result, schema("age", "bigint"), schema("new", "int"));
    verifyDataRows(result, rows(4, 3));
  }

  @Test
  public void testEvalMinString() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval new = min('apple', 'sam', dog_name) | fields dog_name, new",
                TEST_INDEX_DOG));
    verifySchema(result, schema("dog_name", "string"), schema("new", "string"));
    verifyDataRows(result, rows("rex", "apple"), rows("snoopy", "apple"));
  }

  @Test
  @RequiresCapability(
      value = EVAL_MAX_MIN_MIXED_TYPES,
      note =
          "Mixed numeric+string operands -> 'Cannot infer return type for GREATEST' on the AE"
              + " route.")
  public void testEvalMinNumericAndString() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval new = min(14, age, 'sam', holdersName) | fields age, holdersName,"
                    + " new",
                TEST_INDEX_DOG));
    verifySchema(
        result, schema("holdersName", "string"), schema("age", "bigint"), schema("new", "bigint"));
    verifyDataRows(result, rows(2, "Daenerys", 2), rows(4, "Hattie", 4));
  }

  @Test
  public void testEvalMaxIgnoresNulls() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval new = max(`int`, 3) | fields `int`, new",
                TEST_INDEX_NULL_MISSING));
    verifySchema(result, schema("int", "int"), schema("new", "int"));
    verifyDataRows(
        result,
        rows(42, 42),
        rows(null, 3),
        rows(null, 3),
        rows(null, 3),
        rows(null, 3),
        rows(null, 3),
        rows(null, 3),
        rows(null, 3),
        rows(null, 3));
  }

  @Test
  public void testEvalMinIgnoresNulls() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where key = 'values' | eval new = min(dbl, 5) | fields dbl, new",
                TEST_INDEX_NULL_MISSING));
    verifySchema(result, schema("dbl", "double"), schema("new", "double"));
    verifyDataRows(result, rows(3.1415, 3.1415));
  }

  // Keeps the null-skipping assertion on its own rows, covering both explicit-null and
  // missing-field documents. Every row here selects the int literal, so the reported type no longer
  // depends on which row arrives first.
  @Test
  @RequiresCapability(
      value = EVAL_MAX_MIN_INT_WIDENING,
      note =
          "min(dbl, 5) over null-valued rows selects the int literal; the AE route reports the"
              + " column using the wider double type.")
  public void testEvalMinIgnoresNullsWhenLiteralSelected() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where key != 'values' | eval new = min(dbl, 5) | fields dbl, new",
                TEST_INDEX_NULL_MISSING));
    verifySchema(result, schema("dbl", "double"), schema("new", "int"));
    verifyDataRows(
        result,
        rows(null, 5),
        rows(null, 5),
        rows(null, 5),
        rows(null, 5),
        rows(null, 5),
        rows(null, 5),
        rows(null, 5),
        rows(null, 5));
  }
}
