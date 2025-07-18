/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.junit.Assume;
import org.opensearch.sql.ppl.RelevanceFunctionIT;

public class CalciteRelevanceFunctionIT extends RelevanceFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  // For Calcite, this PPL won't throw exception since it supports partial pushdown and has
  // optimization rule `FilterProjectTransposeRule` to push down the filter through the project.
  @Override
  public void not_pushdown_throws_exception() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String query1 =
        "SOURCE="
            + TEST_INDEX_BEER
            + " | EVAL answerId = AcceptedAnswerId + 1"
            + " | WHERE simple_query_string(['Tags'], 'taste') and answerId > 200";
    assertEquals(5, executeQuery(query1).getInt("total"));
  }
}
