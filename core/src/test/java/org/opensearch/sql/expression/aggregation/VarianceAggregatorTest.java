/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.expression.DSL.ref;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

@ExtendWith(MockitoExtension.class)
public class VarianceAggregatorTest extends AggregationTest {

  @Mock Expression expression;

  @Mock ExprValue tupleValue;

  @Mock BindingTuple tuple;

  @Test
  public void variance_sample_field_expression() {
    ExprValue result =
        varianceSample(integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    assertEquals(1.6666666666666667, result.value());
  }

  @Test
  public void variance_population_field_expression() {
    ExprValue result =
        variancePop(integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    assertEquals(1.25, result.value());
  }

  @Test
  public void variance_sample_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.varSamp(DSL.multiply(ref("integer_value", INTEGER), DSL.literal(10))), tuples);
    assertEquals(166.66666666666666, result.value());
  }

  @Test
  public void variance_pop_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.varPop(DSL.multiply(ref("integer_value", INTEGER), DSL.literal(10))), tuples);
    assertEquals(125d, result.value());
  }

  @Test
  public void filtered_variance_sample() {
    ExprValue result =
        aggregation(
            DSL.varSamp(ref("integer_value", INTEGER))
                .condition(DSL.greater(ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(1.0, result.value());
  }

  @Test
  public void filtered_variance_pop() {
    ExprValue result =
        aggregation(
            DSL.varPop(ref("integer_value", INTEGER))
                .condition(DSL.greater(ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(0.6666666666666666, result.value());
  }

  @Test
  public void variance_sample_with_missing() {
    ExprValue result = varianceSample(integerValue(2), integerValue(1), missingValue());
    assertEquals(0.5, result.value());
  }

  @Test
  public void variance_population_with_missing() {
    ExprValue result = variancePop(integerValue(2), integerValue(1), missingValue());
    assertEquals(0.25, result.value());
  }

  @Test
  public void variance_sample_with_null() {
    ExprValue result = varianceSample(doubleValue(3d), doubleValue(4d), nullValue());
    assertEquals(0.5, result.value());
  }

  @Test
  public void variance_pop_with_null() {
    ExprValue result = variancePop(doubleValue(3d), doubleValue(4d), nullValue());
    assertEquals(0.25, result.value());
  }

  @Test
  public void variance_sample_with_all_missing_or_null() {
    ExprValue result = varianceSample(missingValue(), nullValue());
    assertTrue(result.isNull());
  }

  @Test
  public void variance_pop_with_all_missing_or_null() {
    ExprValue result = variancePop(missingValue(), nullValue());
    assertTrue(result.isNull());
  }

  @Test
  public void valueOf() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.avg(ref("double_value", DOUBLE)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: avg", exception.getMessage());
  }

  @Test
  public void variance_sample_to_string() {
    Aggregator avgAggregator = DSL.varSamp(ref("integer_value", INTEGER));
    assertEquals("var_samp(integer_value)", avgAggregator.toString());
  }

  @Test
  public void variance_pop_to_string() {
    Aggregator avgAggregator = DSL.varPop(ref("integer_value", INTEGER));
    assertEquals("var_pop(integer_value)", avgAggregator.toString());
  }

  @Test
  public void variance_sample_nested_to_string() {
    Aggregator avgAggregator =
        DSL.varSamp(
            DSL.multiply(
                ref("integer_value", INTEGER), DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(
        String.format("var_samp(*(%s, %d))", ref("integer_value", INTEGER), 10),
        avgAggregator.toString());
  }

  private ExprValue varianceSample(ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.varSamp(expression), mockTuples(value, values));
  }

  private ExprValue variancePop(ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.varPop(expression), mockTuples(value, values));
  }

  private List<ExprValue> mockTuples(ExprValue value, ExprValue... values) {
    List<ExprValue> mockTuples = new ArrayList<>();
    when(tupleValue.bindingTuples()).thenReturn(tuple);
    mockTuples.add(tupleValue);
    for (ExprValue exprValue : values) {
      mockTuples.add(tupleValue);
    }
    return mockTuples;
  }
}
