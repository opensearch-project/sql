/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

public class LimitOperatorTest extends PhysicalPlanTestBase {

  @Test
  public void limit() {
    PhysicalPlan plan = new LimitOperator(new TestScan(), 1, 0);
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
  }

  @Test
  public void limit_and_offset() {
    PhysicalPlan plan = new LimitOperator(new TestScan(), 1, 1);
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
  }

  @Test
  public void offset_exceeds_row_number() {
    PhysicalPlan plan = new LimitOperator(new TestScan(), 1, 6);
    List<ExprValue> result = execute(plan);
    assertEquals(0, result.size());
  }
}
