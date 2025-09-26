/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import org.opensearch.sql.ppl.QueryAnalysisIT;

public class CalciteQueryAnalysisIT extends QueryAnalysisIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Override
  public void nonexistentFieldShouldFailSemanticCheck() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(String.format("search source=%s | fields name", TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e, "Field [name] not found.");
  }
}
