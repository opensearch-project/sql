/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
public class RenameOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  @Test
  public void avg_aggregation_rename() {
    PhysicalPlan plan =
        new RenameOperator(
            new AggregationOperator(
                new TestScan(),
                Collections.singletonList(
                    DSL.named("avg(response)", DSL.avg(DSL.ref("response", INTEGER)))),
                Collections.singletonList(DSL.named("action", DSL.ref("action", STRING)))),
            ImmutableMap.of(DSL.ref("avg(response)", DOUBLE), DSL.ref("avg", DOUBLE)));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "avg", 268d)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "avg", 350d))));
  }

  @Test
  public void rename_int_value() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(ExprValueUtils.integerValue(1));
    PhysicalPlan plan =
        new RenameOperator(
            inputPlan, ImmutableMap.of(DSL.ref("avg(response)", DOUBLE), DSL.ref("avg", DOUBLE)));
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(result, containsInAnyOrder(ExprValueUtils.integerValue(1)));
  }
}
