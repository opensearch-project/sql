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
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprTextValue;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * To assert the original behaviour of eval operator.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@RunWith(MockitoJUnitRunner.Silent.class)
public class OpenSearchEvalOperatorTest {

  @Mock private PhysicalPlan input;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private NodeClient nodeClient;

  // Test-case: Make sure the original eval works
  // Test-case: Make sure geoIpClient being called and assert the same mocked value being returned.

  /**
   * The test-case aim to assert OpenSearchEvalOperator behaviour when evaluating generic expression,
   * which is not OpenSearch Engine specific. (Ex: ABS(age) )
   */
  @Test
  public void testEvalOperatorOnGenericOperations() {

    ExprTupleValue valueMap = new ExprTupleValue(new LinkedHashMap<>(Map.of(
            "firstname", new OpenSearchExprTextValue("Amber"),
            "age", new ExprLongValue(32),
            "email", new OpenSearchExprTextValue("amberduke@pyrami.com"))));

    when(input.next()).thenReturn(valueMap);

    List<Pair<ReferenceExpression, Expression>> ipAddress = List.of(
            ImmutablePair.of(
                    new ReferenceExpression("ageInAbs", OpenSearchTextType.of()),
                    DSL.abs(DSL.abs(new ReferenceExpression("age", ExprCoreType.LONG)))));
    OpenSearchEvalOperator evalOperator = new OpenSearchEvalOperator(input, ipAddress, nodeClient);

    // Make sure generic Expression function as expected when wrapped with OpenSearchEvalOperator.
    assertSame(32, evalOperator.next().keyValue("ageInAbs").integerValue());
  }

}
