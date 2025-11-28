/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.StatsCommandIT;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setQuerySizeLimit(2000);
  }

  @Test
  public void testStatsHaving() throws IOException {
    try {
      setQueryBucketSize(2);
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | stats sum(balance) as a by state | where a > 780000",
                  TEST_INDEX_ACCOUNT));
      verifyDataRows(response, rows(782199, "TX"));
    } finally {
      resetQueryBucketSize();
    }
  }
}
