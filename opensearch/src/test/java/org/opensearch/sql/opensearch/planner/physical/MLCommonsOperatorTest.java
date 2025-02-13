/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.MLPredictionOutput;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.MLClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@RunWith(MockitoJUnitRunner.Silent.class)
public class MLCommonsOperatorTest {
  @Mock private PhysicalPlan input;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private NodeClient nodeClient;

  private MLCommonsOperator mlCommonsOperator;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MachineLearningNodeClient machineLearningNodeClient;

  @BeforeEach
  void setUp() {
    Map<String, Literal> arguments = new HashMap<>();
    arguments.put("k1", AstDSL.intLiteral(3));
    arguments.put("k2", AstDSL.stringLiteral("v1"));
    arguments.put("k3", AstDSL.booleanLiteral(true));
    arguments.put("k4", AstDSL.doubleLiteral(2.0D));
    arguments.put("k5", AstDSL.shortLiteral((short) 2));
    arguments.put("k6", AstDSL.longLiteral(2L));
    arguments.put("k7", AstDSL.floatLiteral(2F));

    mlCommonsOperator = new MLCommonsOperator(input, "kmeans", arguments, nodeClient);
    when(input.hasNext()).thenReturn(true).thenReturn(false);
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    resultBuilder.put("k1", new ExprIntegerValue(2));
    when(input.next()).thenReturn(ExprTupleValue.fromExprValueMap(resultBuilder.build()));

    DataFrame dataFrame =
        DataFrameBuilder.load(
            Collections.singletonList(
                ImmutableMap.<String, Object>builder()
                    .put("result-k1", 2D)
                    .put("result-k2", 1)
                    .put("result-k3", "v3")
                    .put("result-k4", true)
                    .put("result-k5", (short) 2)
                    .put("result-k6", 2L)
                    .put("result-k7", 2F)
                    .build()));
    MLPredictionOutput mlPredictionOutput =
        MLPredictionOutput.builder()
            .taskId("test_task_id")
            .status("test_status")
            .predictionResult(dataFrame)
            .build();

    try (MockedStatic<MLClient> mlClientMockedStatic = Mockito.mockStatic(MLClient.class)) {
      mlClientMockedStatic
          .when(() -> MLClient.getMLClient(any(NodeClient.class)))
          .thenReturn(machineLearningNodeClient);
      when(machineLearningNodeClient
              .trainAndPredict(any(MLInput.class))
              .actionGet(anyLong(), eq(TimeUnit.SECONDS)))
          .thenReturn(mlPredictionOutput);
    }
  }

  @Disabled
  @Test
  public void testOpen() {
    mlCommonsOperator.open();
    assertTrue(mlCommonsOperator.hasNext());
    assertNotNull(mlCommonsOperator.next());
    assertFalse(mlCommonsOperator.hasNext());
  }

  @Test
  public void testAccept() {
    PhysicalPlanNodeVisitor physicalPlanNodeVisitor =
        new PhysicalPlanNodeVisitor<Integer, Object>() {};
    assertNull(mlCommonsOperator.accept(physicalPlanNodeVisitor, null));
  }

  @Test
  public void testConvertArgumentToMLParameter_UnsupportedType() {
    Map<String, Literal> argument = new HashMap<>();
    argument.put("k2", AstDSL.dateLiteral("2020-10-31"));
    assertThrows(
        IllegalArgumentException.class,
        () -> mlCommonsOperator.convertArgumentToMLParameter(argument, "LINEAR_REGRESSION"));
  }
}
