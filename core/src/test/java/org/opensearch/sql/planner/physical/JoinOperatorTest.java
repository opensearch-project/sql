/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.physical.join.NestedLoopJoinOperator;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class JoinOperatorTest extends PhysicalPlanTestBase {

  public void nested_loop_join_test() {
    PhysicalPlan left = testScan(compoundInputs);
    PhysicalPlan right = testScan(countTestInputs);
    PhysicalPlan joinPlan =
        new NestedLoopJoinOperator(
            left,
            right,
            Join.JoinType.INNER,
            DSL.equal(DSL.ref("errors", INTEGER), DSL.ref("id", INTEGER)));
    List<ExprValue> result = execute(joinPlan);
    assertEquals(7, result.size());
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
}
