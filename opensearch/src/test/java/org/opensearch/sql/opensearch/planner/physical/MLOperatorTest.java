/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static java.util.Collections.singletonList;
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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.MLClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@RunWith(MockitoJUnitRunner.Silent.class)
public class MLOperatorTest {
  @Mock private PhysicalPlan input;

  @Mock PlainActionFuture<MLOutput> actionFuture;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private NodeClient nodeClient;

  private MLOperator mlOperator;
  final Map<String, Literal> arguments = new HashMap<>();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MachineLearningNodeClient machineLearningNodeClient;

  void setUp(boolean isPredict) {
    arguments.put("k1", AstDSL.intLiteral(3));
    arguments.put("k2", AstDSL.stringLiteral("v1"));
    arguments.put("k3", AstDSL.booleanLiteral(true));
    arguments.put("k4", AstDSL.doubleLiteral(2.0D));
    arguments.put("k5", AstDSL.shortLiteral((short) 2));
    arguments.put("k6", AstDSL.longLiteral(2L));
    arguments.put("k7", AstDSL.floatLiteral(2F));

    mlOperator = new MLOperator(input, arguments, nodeClient);
    when(input.hasNext()).thenReturn(true).thenReturn(false);
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    resultBuilder.put("k1", new ExprIntegerValue(2));
    when(input.next()).thenReturn(ExprTupleValue.fromExprValueMap(resultBuilder.build()));

    DataFrame dataFrame =
        DataFrameBuilder.load(
            singletonList(
                ImmutableMap.<String, Object>builder()
                    .put("result-k1", 2D)
                    .put("result-k2", 1)
                    .put("result-k3", "v3")
                    .put("result-k4", true)
                    .put("result-k5", (short) 2)
                    .put("result-k6", 2L)
                    .put("result-k7", 2F)
                    .build()));

    MLOutput mlOutput;
    if (isPredict) {
      mlOutput =
          MLPredictionOutput.builder()
              .taskId("test_task_id")
              .status("test_status")
              .predictionResult(dataFrame)
              .build();
    } else {
      mlOutput =
          MLTrainingOutput.builder()
              .taskId("test_task_id")
              .status("test_status")
              .modelId("test_model_id")
              .build();
    }

    when(actionFuture.actionGet(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(mlOutput);
    when(machineLearningNodeClient.run(any(MLInput.class), any())).thenReturn(actionFuture);
  }

  void setUpPredict() {
    arguments.put(ACTION, AstDSL.stringLiteral(PREDICT));
    arguments.put(ALGO, AstDSL.stringLiteral(KMEANS));
    arguments.put("modelid", AstDSL.stringLiteral("dummyID"));
    setUp(true);
  }

  void setUpTrain() {
    arguments.put(ACTION, AstDSL.stringLiteral(TRAIN));
    arguments.put(ALGO, AstDSL.stringLiteral(KMEANS));
    setUp(false);
  }

  @Test
  public void testOpenPredict() {
    setUpPredict();
    try (MockedStatic<MLClient> mlClientMockedStatic = Mockito.mockStatic(MLClient.class)) {
      when(MLClient.getMLClient(any(NodeClient.class))).thenReturn(machineLearningNodeClient);
      mlOperator.open();
      assertTrue(mlOperator.hasNext());
      assertNotNull(mlOperator.next());
      assertFalse(mlOperator.hasNext());
    }
  }

  @Test
  public void testOpenTrain() {
    setUpTrain();
    try (MockedStatic<MLClient> mlClientMockedStatic = Mockito.mockStatic(MLClient.class)) {
      when(MLClient.getMLClient(any(NodeClient.class))).thenReturn(machineLearningNodeClient);
      mlOperator.open();
      assertTrue(mlOperator.hasNext());
      assertNotNull(mlOperator.next());
      assertFalse(mlOperator.hasNext());
    }
  }

  @Test
  public void testAccept() {
    setUpPredict();
    try (MockedStatic<MLClient> mlClientMockedStatic = Mockito.mockStatic(MLClient.class)) {
      when(MLClient.getMLClient(any(NodeClient.class))).thenReturn(machineLearningNodeClient);
      PhysicalPlanNodeVisitor physicalPlanNodeVisitor =
          new PhysicalPlanNodeVisitor<Integer, Object>() {};
      assertNull(mlOperator.accept(physicalPlanNodeVisitor, null));
    }
  }
}
