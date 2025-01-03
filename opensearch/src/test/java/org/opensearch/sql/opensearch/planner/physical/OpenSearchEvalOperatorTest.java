/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprTextValue;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * To assert the original bahviour of eval operator.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@RunWith(MockitoJUnitRunner.Silent.class)
public class OpenSearchEvalOperatorTest {
  @Mock private PhysicalPlan input;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private NodeClient nodeClient;

  @Test
  public void testOpenSearchEvalAccept() {
    PhysicalPlanNodeVisitor physicalPlanNodeVisitor =
              new PhysicalPlanNodeVisitor<Integer, Object>() {};
    List<Pair<ReferenceExpression, Expression>> ipAddress = List.of(ImmutablePair.of(
            new ReferenceExpression("ipAddress", OpenSearchTextType.of()),
            new LiteralExpression(ExprNullValue.of())));
    OpenSearchEvalOperator evalOperator = new OpenSearchEvalOperator(input, ipAddress, nodeClient);
    assertNull(evalOperator.accept(physicalPlanNodeVisitor, null));
  }

  // Test-case: Make sure the original eval works
  // Test-case: Make sure geoIpClient being called and assert the same mocked value being returned.

  // To have some expression like a+2 == c to make sure the original eval behaviour stay.
  @Test
  public void testEvalOperatorOnGenericOperations() {

    ProjectOperator projectOperator = new ProjectOperator(input,
            List.of(
                    new NamedExpression("firstname", new ReferenceExpression("firstname", List.of("firstname"), OpenSearchTextType.of())),
                    new NamedExpression("age", new ReferenceExpression("age", List.of("age"), ExprCoreType.LONG)),
                    new NamedExpression("firstname", new ReferenceExpression("email", List.of("email"), OpenSearchTextType.of()))),
            Collections.emptyList());

    List<Pair<ReferenceExpression, Expression>> ipAddress = List.of(ImmutablePair.of(
            new ReferenceExpression("ipAddress", OpenSearchTextType.of()),
            new LiteralExpression(ExprNullValue.of())));
    OpenSearchEvalOperator evalOperator = new OpenSearchEvalOperator(projectOperator, ipAddress, nodeClient);
    assertNull(evalOperator.next());

  }
}
