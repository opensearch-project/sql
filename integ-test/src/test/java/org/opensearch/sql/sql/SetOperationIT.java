/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.util.Capability.SET_OPERATION;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;
import org.opensearch.sql.util.RequiresCapability;

/** SQL set operations: {@code UNION} and {@code UNION ALL}. */
@RequiresCapability(SET_OPERATION)
public class SetOperationIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
  }

  @Test
  public void testUnionAll() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age FROM %s WHERE age < 30\
                 UNION ALL SELECT age FROM %s WHERE age > 35\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK, TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(response, rows(28), rows(36), rows(36), rows(39));
  }

  /** Same operands as {@link #testUnionAll()}: the duplicate 36 collapses to one row. */
  @Test
  public void testUnionDistinct() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age FROM %s WHERE age < 30\
                 UNION SELECT age FROM %s WHERE age > 35\
                """
                    .formatted(TestsConstants.TEST_INDEX_BANK, TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(response, rows(28), rows(36), rows(39));
  }

  @Test
  public void testMultiWayUnionAll() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age FROM %s WHERE age < 30\
                 UNION ALL SELECT age FROM %s WHERE age > 35\
                 UNION ALL SELECT age FROM %s WHERE age = 33\
                """
                    .formatted(
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(response, rows(28), rows(33), rows(36), rows(36), rows(39));
  }

  /** Same three operands as {@link #testMultiWayUnionAll()}: the duplicate 36 collapses. */
  @Test
  public void testMultiWayUnionDistinct() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                """
                SELECT age FROM %s WHERE age < 30\
                 UNION SELECT age FROM %s WHERE age > 35\
                 UNION SELECT age FROM %s WHERE age = 33\
                """
                    .formatted(
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK),
                "jdbc"));

    verifyDataRows(response, rows(28), rows(33), rows(36), rows(39));
  }

  @Test
  public void testMixedUnionAndUnionAllIsRejected() {
    assertThrows(
        RuntimeException.class,
        () ->
            executeQuery(
                """
                SELECT age FROM %s WHERE age < 30\
                 UNION ALL SELECT age FROM %s WHERE age > 35\
                 UNION SELECT age FROM %s WHERE age = 33\
                """
                    .formatted(
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK),
                "jdbc"));
  }
}
