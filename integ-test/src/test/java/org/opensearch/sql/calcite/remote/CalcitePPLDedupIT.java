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
    JSONObject actual = executeQuery(Index.DUPLICATION_NULLABLE.ppl("dedup 1 name | fields name"));
    verifyDataRows(actual, rows("A"), rows("B"), rows("C"), rows("D"), rows("E"));
  }

  @Test
  public void testDedupMultipleFields() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.DUPLICATION_NULLABLE.ppl("dedup 1 name, category | fields name, category"));
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
            Index.DUPLICATION_NULLABLE.ppl("dedup 1 name KEEPEMPTY=true | fields name, category"));
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
            Index.DUPLICATION_NULLABLE.ppl(
                "dedup 1 name, category KEEPEMPTY=true | fields name, category"));
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
  @SuppressWarnings("ThrowableNotThrown")
  public void testConsecutiveThrowException() {
    assertThrowsWithReplace(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                String.format(
                    "source = %s | dedup 1 name CONSECUTIVE=true | fields name",
                    TEST_INDEX_DUPLICATION_NULLABLE)));

    assertThrowsWithReplace(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                String.format(
                    "source = %s | dedup 1 name KEEPEMPTY=true CONSECUTIVE=true | fields name",
                    TEST_INDEX_DUPLICATION_NULLABLE)));

    assertThrowsWithReplace(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                String.format(
                    "source = %s | dedup 2 name CONSECUTIVE=true | fields name",
                    TEST_INDEX_DUPLICATION_NULLABLE)));

    assertThrowsWithReplace(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                String.format(
                    "source = %s | dedup 2 name KEEPEMPTY=true CONSECUTIVE=true | fields name",
                    TEST_INDEX_DUPLICATION_NULLABLE)));
  }

  @Test
  public void testDedup2() throws IOException {
    JSONObject actual = executeQuery(Index.DUPLICATION_NULLABLE.ppl("dedup 2 name | fields name"));
    verifyDataRows(
        actual, rows("A"), rows("A"), rows("B"), rows("B"), rows("C"), rows("C"), rows("D"),
        rows("E"));
  }

  @Test
  public void testDedupMultipleFields2() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.DUPLICATION_NULLABLE.ppl("dedup 2 name, category | fields name, category"));
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
            Index.DUPLICATION_NULLABLE.ppl("dedup 2 name KEEPEMPTY=true | fields name, category"));
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
            Index.DUPLICATION_NULLABLE.ppl(
                "dedup 2 name, category KEEPEMPTY=true | fields name, category"));
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
            Index.DUPLICATION_NULLABLE.ppl("dedup 2 name, category | fields name, category, id"));
    verifySchemaInOrder(
        actual1,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual2 =
        executeQuery(
            Index.DUPLICATION_NULLABLE.ppl("dedup 2 category, name | fields name, category, id"));
    verifySchemaInOrder(
        actual2,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual3 =
        executeQuery(
            Index.DUPLICATION_NULLABLE.ppl(
                "dedup 2 name, category KEEPEMPTY=true | fields name, category, id"));
    verifySchemaInOrder(
        actual3,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
    JSONObject actual4 =
        executeQuery(
            Index.DUPLICATION_NULLABLE.ppl(
                "dedup 2 category, name KEEPEMPTY=true | fields name, category, id"));
    verifySchemaInOrder(
        actual4,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "int"));
  }
}
