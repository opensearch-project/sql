/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DUPLICATION_NULLABLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLDedupIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.DUPLICATION_NULLABLE);
  }

  @Test
  public void testDedup() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 1 name | fields name\n");
    verifyDataRows(actual, rows("A"), rows("B"), rows("C"), rows("D"), rows("E"));
  }

  @Test
  public void testDedupMultipleFields() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 1 name, category | fields name, category\n");
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
  public void testDedupKeepEmpty() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 1 name KEEPEMPTY=true | fields name, category\n");
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
  public void testDedupMultipleFieldsKeepEmpty() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 1 name, category KEEPEMPTY=true | fields name, category\n");
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
  public void testConsecutiveThrowException() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                    "source = " + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 1 name CONSECUTIVE=true | fields name\n"));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                    "source = " + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 1 name KEEPEMPTY=true CONSECUTIVE=true | fields name\n"));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                    "source = " + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name CONSECUTIVE=true | fields name\n"));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            executeQuery(
                    "source = " + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name KEEPEMPTY=true CONSECUTIVE=true | fields name\n"));
  }

  @Test
  public void testDedup2() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name | fields name\n");
    verifyDataRows(
        actual, rows("A"), rows("A"), rows("B"), rows("B"), rows("C"), rows("C"), rows("D"),
        rows("E"));
  }

  @Test
  public void testDedupMultipleFields2() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name, category | fields name, category\n");
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
  public void testDedupKeepEmpty2() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name KEEPEMPTY=true | fields name, category\n");
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
  public void testDedupMultipleFieldsKeepEmpty2() {
    JSONObject actual =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name, category KEEPEMPTY=true | fields name, category\n");
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
  public void testReorderDedupFieldsShouldNotAffectResult() {
    JSONObject actual1 =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name, category | fields name, category, id\n");
    verifySchemaInOrder(
        actual1,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "integer"));
    JSONObject actual2 =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 category, name | fields name, category, id\n");
    verifySchemaInOrder(
        actual2,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "integer"));
    JSONObject actual3 =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 name, category KEEPEMPTY=true | fields name, category, id\n");
    verifySchemaInOrder(
        actual3,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "integer"));
    JSONObject actual4 =
        executeQuery(
                "source=" + TEST_INDEX_DUPLICATION_NULLABLE + " | dedup 2 category, name KEEPEMPTY=true | fields name, category, id\n");
    verifySchemaInOrder(
        actual4,
        schema("name", null, "string"),
        schema("category", null, "string"),
        schema("id", null, "integer"));
  }
}
