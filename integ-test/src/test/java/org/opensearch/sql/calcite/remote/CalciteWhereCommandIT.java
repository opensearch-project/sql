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
import org.opensearch.sql.ppl.WhereCommandIT;

public class CalciteWhereCommandIT extends WhereCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testInWithMixedType() throws IOException {
    // Mixed type coercion only work with Calcite enabled
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance in (4180, 5686, '6077') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Hattie"), rows("Dale"), rows("Hughes"));
  }
}
