/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.ml.common.output.MLTrainingOutput;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.client.MLClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.utils.MLCommonsConstants.ACTION;
import static org.opensearch.sql.utils.MLCommonsConstants.ALGO;
import static org.opensearch.sql.utils.MLCommonsConstants.KMEANS;
import static org.opensearch.sql.utils.MLCommonsConstants.PREDICT;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAIN;

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
}
