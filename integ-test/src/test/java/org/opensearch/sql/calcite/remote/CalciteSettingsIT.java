/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.ppl.SettingsIT;

public class CalciteSettingsIT extends SettingsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testQuerySizeLimit_NoPushdown() throws IOException {
    withSettings(
        Key.CALCITE_PUSHDOWN_ENABLED,
        "false",
        () -> {
          try {
            super.testQuerySizeLimit_NoPushdown();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testQuerySystemLimit() throws IOException {
    // system limit only impact data-intensive operations
    setQuerySystemLimit(2);
    JSONObject result =
        executeQuery(String.format("search source=%s age>35 | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"), rows("Elinor"), rows("Virginia"));

    // for non data-intensive operations, the rows still control by query.size_limit
    setQuerySizeLimit(1);
    result =
        executeQuery(String.format("search source=%s age>35 | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
    resetQuerySizeLimit();
    resetQuerySystemLimit();
  }

  @Test
  public void testQuerySystemLimitWithJoin() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s age>35 | fields firstname"
                    + " | full join left=l right=r on l.firstname=r.firstname"
                    + " [ search source=%s | fields firstname ]",
                TEST_INDEX_BANK, TEST_INDEX_BANK));
    verifyNumOfRows(result, 7);

    // system limit will impact data-intensive operations
    setQuerySystemLimit(2);
    result =
        executeQuery(
            String.format(
                "search source=%s age>35 | fields firstname"
                    + " | full join left=l right=r on l.firstname=r.firstname"
                    + " [ search source=%s | fields firstname ]",
                TEST_INDEX_BANK, TEST_INDEX_BANK));
    verifyNumOfRows(result, 4);

    // amount of final result should equals to query.size_limit
    setQuerySizeLimit(1);
    result =
        executeQuery(String.format("search source=%s age>35 | fields firstname", TEST_INDEX_BANK));
    verifyNumOfRows(result, 1);
    resetQuerySizeLimit();
    resetQuerySystemLimit();
  }
}
