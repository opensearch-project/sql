package org.opensearch.sql.planner.physical;

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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.ml.client.MachineLearningClient;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.ml.common.parameter.MLInput;
import org.opensearch.ml.common.parameter.MLPredictionOutput;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MLCommonsOperatorTest {
  @Mock
  private PhysicalPlan input;

  @Mock(answer =  Answers.RETURNS_DEEP_STUBS)
  private MachineLearningClient machineLearningClient;

  private MLCommonsOperator mlCommonsOperator;

  @BeforeEach
  void setUp() {
    mlCommonsOperator = new MLCommonsOperator(input, "kmeans",
            AstDSL.exprList(AstDSL.argument("k1", AstDSL.intLiteral(3)),
            AstDSL.argument("k2", AstDSL.stringLiteral("v1")),
            AstDSL.argument("k3", AstDSL.booleanLiteral(true)),
            AstDSL.argument("k4", AstDSL.doubleLiteral(2.0D))),
            machineLearningClient);
    when(input.hasNext()).thenReturn(true).thenReturn(false);
    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    resultBuilder.put("k1", new ExprIntegerValue(2));
    when(input.next()).thenReturn(ExprTupleValue.fromExprValueMap(resultBuilder.build()));

    DataFrame dataFrame = DataFrameBuilder
            .load(Collections.singletonList(
                    ImmutableMap.<String, Object>builder().put("result-k1", 2D)
                    .put("result-k2", 1)
                    .put("result-k3", "v3")
                    .put("result-k4", true)
                    .build())
            );

    MLPredictionOutput mlPredictionOutput = MLPredictionOutput.builder()
            .taskId("test_task_id")
            .status("test_status")
            .predictionResult(dataFrame)
            .build();

    when(machineLearningClient.trainAndPredict(any(MLInput.class)).actionGet(anyLong(),
            eq(TimeUnit.SECONDS))).thenReturn(mlPredictionOutput);

  }

  @Test
  public void testOpen() {
    mlCommonsOperator.open();
    assertTrue(mlCommonsOperator.hasNext());
    assertNotNull(mlCommonsOperator.next());
    assertFalse(mlCommonsOperator.hasNext());
  }

  @Test
  public void testAccept() {
    PhysicalPlanNodeVisitor physicalPlanNodeVisitor
            = new PhysicalPlanNodeVisitor<Integer, Object>() {};
    assertNull(mlCommonsOperator.accept(physicalPlanNodeVisitor, null));
  }

  @Test
  public void testConvertArgumentToMLParameter_UnSupportedType() {
    Argument argument = AstDSL.argument("k2", AstDSL.dateLiteral("2020-10-31"));
    assertThrows(IllegalArgumentException.class, () -> mlCommonsOperator
            .convertArgumentToMLParameter(argument, "LINEAR_REGRESSION"));
  }

}
