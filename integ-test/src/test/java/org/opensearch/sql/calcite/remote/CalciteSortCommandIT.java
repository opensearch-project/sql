/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.SortCommandIT;

public class CalciteSortCommandIT extends SortCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  // TODO: Move this test to SortCommandIT once head-then-sort is fixed in v2.
  @Test
  public void testHeadThenSort() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | head 2 | sort age | fields age", TEST_INDEX_BANK));
    verifyOrder(result, rows(32), rows(36));
  }
}
