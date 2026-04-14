/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Integration test for NOT IN excluding null/missing rows (issue #5165). */
public class CalciteNotInNullFilterIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testNotInExcludesNullRows() throws IOException {
    // age values: 32, 36, 28, 33, 36, null, 34
    // NOT IN (32, 28) should return 36, 33, 36, 34 — excluding the null row
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age NOT IN (32, 28) | fields age | sort age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRowsInOrder(result, rows(33), rows(34), rows(36), rows(36));
  }

  @Test
  public void testNotInExcludesNullAndMissingRows() throws IOException {
    // balance values: 39225, null, 32838, 4180, null, null, 48086
    // NOT IN (39225) should return 32838, 4180, 48086 — excluding null/missing rows
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance NOT IN (39225) | fields balance | sort balance",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRowsInOrder(result, rows(4180), rows(32838), rows(48086));
  }

  @Test
  public void testInWithNullRowsIsUnaffected() throws IOException {
    // IN should naturally exclude nulls (positive match never matches null)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age IN (32, 28) | fields age | sort age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRowsInOrder(result, rows(28), rows(32));
  }
}
