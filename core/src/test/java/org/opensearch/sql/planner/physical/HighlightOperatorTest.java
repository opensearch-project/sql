/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

public class HighlightOperatorTest extends PhysicalPlanTestBase {

  @Test
  public void highlight_all_test() {
    PhysicalPlan plan = new HighlightOperator(new TestScan(), DSL.ref("*", STRING));
    List<ExprValue> result = execute(plan);
    assertEquals(5, result.size());
    assertThat(result, containsInAnyOrder(
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "209.160.24.63",
            "action", "GET", "response", 200, "referer", "www.amazon.com")),
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "209.160.24.63",
            "action", "GET", "response", 404, "referer", "www.amazon.com")),
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "112.111.162.4",
            "action", "GET", "response", 200, "referer", "www.amazon.com")),
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "74.125.19.106",
            "action", "POST", "response", 200, "referer", "www.google.com")),
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "74.125.19.106",
            "action", "POST", "response", 500))
    ));
  }
}
