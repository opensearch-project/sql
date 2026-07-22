/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;

/** PPL counterpart of the SQL {@code QueryValidationIT} for query-level rejection cases. */
public class QueryValidationIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void deeplyNestedPredicateIsRejectedInsteadOfCrashingNode() throws IOException {
    // Lower the limit so a small, safe-to-parse query triggers the guard (a query large enough
    // to exhaust the default limit could overflow the ANTLR parser itself before the guard runs).
    updateClusterSettings(
        new ClusterSetting(TRANSIENT, Settings.Key.MAX_EXPRESSION_DEPTH.getKeyValue(), "20"));
    try {
      StringBuilder predicate = new StringBuilder("age = 1");
      for (int i = 2; i <= 30; i++) {
        predicate.append(" or age = ").append(i);
      }
      executeQuery("source=opensearch-sql_test_index_account | where " + predicate);
      fail("Expected ResponseException for an over-nested predicate");
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("Expression nesting depth exceeds the maximum allowed"));
    } finally {
      updateClusterSettings(
          new ClusterSetting(TRANSIENT, Settings.Key.MAX_EXPRESSION_DEPTH.getKeyValue(), null));
    }
  }
}
