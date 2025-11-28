/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DUPLICATION_NULLABLE;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLDedupIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.DUPLICATION_NULLABLE);
  }

  @Test
  public void testDedup() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name | fields name", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(actual, rows("A"), rows("B"), rows("C"), rows("D"), rows("E"));
  }

  @Test
  public void testDedupMultipleFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name, category | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "Y"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("D", "Z"),
        rows("B", "Y"));
  }

  @Test
  public void testDedupKeepEmpty() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("D", "Z"),
        rows("E", null),
        rows(null, "Y"),
        rows(null, "X"),
        rows(null, "Z"),
        rows(null, null));
  }

  @Test
  public void testDedupMultipleFieldsKeepEmpty() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 1 name, category KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "Y"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("D", "Z"),
        rows("B", "Y"),
        rows(null, "Y"),
        rows("E", null),
        rows(null, "X"),
        rows("B", null),
        rows(null, "Z"),
        rows(null, null));
  }

  @Test
  public void testConsecutiveImplicitFallbackV2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | dedup 1 name CONSECUTIVE=true | fields name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyNumOfRows(actual, 8);

    actual =
        executeQuery(
            String.format(
                "source = %s | dedup 1 name KEEPEMPTY=true CONSECUTIVE=true | fields name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyNumOfRows(actual, 12);

    actual =
        executeQuery(
            String.format(
                "source = %s | dedup 2 name CONSECUTIVE=true | fields name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyNumOfRows(actual, 12);

    actual =
        executeQuery(
            String.format(
                "source = %s | dedup 2 name KEEPEMPTY=true CONSECUTIVE=true | fields name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyNumOfRows(actual, 16);
  }

  @Test
  public void testDedup2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name | fields name", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual, rows("A"), rows("A"), rows("B"), rows("B"), rows("C"), rows("C"), rows("D"),
        rows("E"));
  }

  @Test
  public void testDedupMultipleFields2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "X"),
        rows("A", "Y"),
        rows("A", "Y"),
        rows("B", "Y"),
        rows("B", "Z"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("C", "X"),
        rows("D", "Z"));
  }

  @Test
  public void testDedupKeepEmpty2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "Y"),
        rows("B", "Z"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("C", "X"),
        rows("D", "Z"),
        rows("E", null),
        rows(null, "Y"),
        rows(null, "X"),
        rows(null, "Z"),
        rows(null, null));
  }

  @Test
  public void testDedupMultipleFieldsKeepEmpty2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category KEEPEMPTY=true | fields name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("A", "X"),
        rows("A", "X"),
        rows("A", "Y"),
        rows("A", "Y"),
        rows("B", "Y"),
        rows("B", "Z"),
        rows("B", "Z"),
        rows("C", "X"),
        rows("C", "X"),
        rows("D", "Z"),
        rows(null, "Y"),
        rows("E", null),
        rows(null, "X"),
        rows("B", null),
        rows(null, "Z"),
        rows(null, null));
  }

  @Test
  public void testReorderDedupFieldsShouldNotAffectResult() throws IOException {
    JSONObject actual1 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual1,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 category, name | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual2,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual3 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 name, category KEEPEMPTY=true | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual3,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual4 =
        executeQuery(
            String.format(
                "source=%s | dedup 2 category, name KEEPEMPTY=true | fields name, category, id",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifySchemaInOrder(
        actual4,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
  }

  @Test
  public void testDedupComplex() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | dedup 1 name | fields category, name, id", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", "A", 1),
        rows("Z", "B", 1),
        rows("X", "C", 1),
        rows("Z", "D", 1),
        rows(null, "E", 1));
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, name | dedup 1 name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual, rows("X", "A"), rows("Z", "B"), rows("X", "C"), rows("Z", "D"), rows(null, "E"));
    actual =
        executeQuery(
            String.format("source=%s | dedup 1 name, category | fields category, name, id", TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", "A", 1),
        rows("Y", "A", 1),
        rows("Y", "B", 1),
        rows("Z", "B", 1),
        rows("X", "C", 1),
        rows("Z", "D", 1));
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, id, name | dedup 2 name, category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", 1, "A"),
        rows("X", 1, "A"),
        rows("Y", 1, "A"),
        rows("Y", 1, "A"),
        rows("Y", 1, "B"),
        rows("Z", 1, "B"),
        rows("Z", 1, "B"),
        rows("X", 1, "C"),
        rows("X", 1, "C"),
        rows("Z", 1, "D"));
  }

  @Test
  public void testDedupExpr() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval new_name = lower(name) | dedup 1 new_name | fields category, name, id, new_name",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", "A", 1, "a"),
        rows("Z", "B", 1, "b"),
        rows("X", "C", 1, "c"),
        rows("Z", "D", 1, "d"),
        rows(null, "E", 1, "e"));
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, name, id | eval new_name = lower(name), new_category"
                    + " = lower(category) | dedup 1 new_name, new_category | fields category, name, id, new_name, new_category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", "C", 1, "c", "x"),
        rows("Z", "D", 1, "d", "z"),
        rows("X", "A", 1, "a", "x"),
        rows("Y", "B", 1, "b", "y"),
        rows("Y", "A", 1, "a", "y"),
        rows("Z", "B", 1, "b", "z"));
    actual =
        executeQuery(
            String.format(
                "source=%s | eval new_name = lower(name), new_category = lower(category) | dedup 2"
                    + " name, category | fields category, name, id, new_name, new_category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("Y", "A", 1, "a", "y"),
        rows("Y", "A", 1, "a", "y"),
        rows("Z", "B", 1, "b", "z"),
        rows("Z", "B", 1, "b", "z"),
        rows("X", "A", 1, "a", "x"),
        rows("X", "A", 1, "a", "x"),
        rows("Y", "B", 1, "b", "y"),
        rows("Z", "D", 1, "d", "z"),
        rows("X", "C", 1, "c", "x"),
        rows("X", "C", 1, "c", "x"));
    actual =
        executeQuery(
            String.format(
                "source=%s | fields category, id, name | eval new_name = lower(name) | eval"
                    + " new_category = lower(category) | sort name, -category | dedup 2 new_name,"
                    + " new_category | fields category, id, name, new_name, new_category",
                TEST_INDEX_DUPLICATION_NULLABLE));
    verifyDataRows(
        actual,
        rows("X", 1, "C", "c", "x"),
        rows("X", 1, "C", "c", "x"),
        rows("Z", 1, "D", "d", "z"),
        rows("X", 1, "A", "a", "x"),
        rows("X", 1, "A", "a", "x"),
        rows("Y", 1, "B", "b", "y"),
        rows("Y", 1, "A", "a", "y"),
        rows("Y", 1, "A", "a", "y"),
        rows("Z", 1, "B", "b", "z"),
        rows("Z", 1, "B", "b", "z"));
  }
}
