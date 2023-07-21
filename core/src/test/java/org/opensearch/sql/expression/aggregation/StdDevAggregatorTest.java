/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

@ExtendWith(MockitoExtension.class)
public class StdDevAggregatorTest extends AggregationTest {

  @Mock
  Expression expression;

  @Mock
  ExprValue tupleValue;

  @Mock
  BindingTuple tuple;

  @Test
  public void stddev_sample_field_expression() {
    ExprValue result =
        stddevSample(integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    assertEquals(1.2909944487358056, result.value());
  }

  @Test
  public void stddev_population_field_expression() {
    ExprValue result =
        stddevPop(integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    assertEquals(1.118033988749895, result.value());
  }

  @Test
  public void stddev_sample_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.stddevSamp(DSL.multiply(ref("integer_value", INTEGER), DSL.literal(10))), tuples);
    assertEquals(12.909944487358056, result.value());
  }

  @Test
  public void stddev_population_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.stddevPop(DSL.multiply(ref("integer_value", INTEGER), DSL.literal(10))), tuples);
    assertEquals(11.180339887498949, result.value());
  }

  @Test
  public void filtered_stddev_sample() {
    ExprValue result =
        aggregation(
            DSL.stddevSamp(ref("integer_value", INTEGER))
                .condition(DSL.greater(ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(1.0, result.value());
  }

  @Test
  public void filtered_stddev_population() {
    ExprValue result =
        aggregation(
            DSL.stddevPop(ref("integer_value", INTEGER))
                .condition(DSL.greater(ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(0.816496580927726, result.value());
  }

  @Test
  public void stddev_sample_with_missing() {
    ExprValue result = stddevSample(integerValue(2), integerValue(1), missingValue());
    assertEquals(0.7071067811865476, result.value());
  }

  @Test
  public void stddev_population_with_missing() {
    ExprValue result = stddevPop(integerValue(2), integerValue(1), missingValue());
    assertEquals(0.5, result.value());
  }

  @Test
  public void stddev_sample_with_null() {
    ExprValue result = stddevSample(doubleValue(3d), doubleValue(4d), nullValue());
    assertEquals(0.7071067811865476, result.value());
  }

  @Test
  public void stddev_pop_with_null() {
    ExprValue result = stddevPop(doubleValue(3d), doubleValue(4d), nullValue());
    assertEquals(0.5, result.value());
  }

  @Test
  public void stddev_sample_with_all_missing_or_null() {
    ExprValue result = stddevSample(missingValue(), nullValue());
    assertTrue(result.isNull());
  }

  @Test
  public void stddev_pop_with_all_missing_or_null() {
    ExprValue result = stddevPop(missingValue(), nullValue());
    assertTrue(result.isNull());
  }

  @Test
  public void stddev_sample_to_string() {
    Aggregator aggregator = DSL.stddevSamp(ref("integer_value", INTEGER));
    assertEquals("stddev_samp(integer_value)", aggregator.toString());
  }

  @Test
  public void stddev_pop_to_string() {
    Aggregator aggregator = DSL.stddevPop(ref("integer_value", INTEGER));
    assertEquals("stddev_pop(integer_value)", aggregator.toString());
  }

  @Test
  public void stddev_sample_nested_to_string() {
    Aggregator avgAggregator =
        DSL.stddevSamp(
            DSL.multiply(
                ref("integer_value", INTEGER), DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(
        String.format("stddev_samp(*(%s, %d))", ref("integer_value", INTEGER), 10),
        avgAggregator.toString());
  }

  private ExprValue stddevSample(ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.stddevSamp(expression), mockTuples(value, values));
  }

  private ExprValue stddevPop(ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.stddevPop(expression), mockTuples(value, values));
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
