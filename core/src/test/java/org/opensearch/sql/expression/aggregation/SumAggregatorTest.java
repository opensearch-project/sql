/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.aggregation.SumAggregator.SumState;

class SumAggregatorTest extends AggregationTest {

  @Test
  public void sum_integer_field_expression() {
    ExprValue result = aggregation(DSL.sum(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(10, result.value());
  }

  @Test
  public void sum_long_field_expression() {
    ExprValue result = aggregation(DSL.sum(DSL.ref("long_value", LONG)), tuples);
    assertEquals(10L, result.value());
  }

  @Test
  public void sum_float_field_expression() {
    ExprValue result = aggregation(DSL.sum(DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(10f, result.value());
  }

  @Test
  public void sum_double_field_expression() {
    ExprValue result = aggregation(DSL.sum(DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(10d, result.value());
  }

  @Test
  public void sum_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.sum(
                DSL.multiply(
                    DSL.ref("integer_value", INTEGER),
                    DSL.literal(ExprValueUtils.integerValue(10)))),
            tuples);
    assertEquals(100, result.value());
  }

  @Test
  public void sum_string_field_expression() {
    SumAggregator sumAggregator =
        new SumAggregator(ImmutableList.of(DSL.ref("string_value", STRING)), ExprCoreType.STRING);
    SumState sumState = sumAggregator.create();
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () ->
                sumAggregator.iterate(
                    ExprValueUtils.tupleValue(ImmutableMap.of("string_value", "m")).bindingTuples(),
                    sumState));
    assertEquals("unexpected type [STRING] in sum aggregation", exception.getMessage());
  }

  @Test
  public void filtered_sum() {
    ExprValue result =
        aggregation(
            DSL.sum(DSL.ref("integer_value", INTEGER))
                .condition(DSL.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(9, result.value());
  }

  @Test
  public void sum_with_missing() {
    ExprValue result =
        aggregation(DSL.sum(DSL.ref("integer_value", INTEGER)), tuples_with_null_and_missing);
    assertEquals(3, result.value());
  }

  @Test
  public void sum_with_null() {
    ExprValue result =
        aggregation(DSL.sum(DSL.ref("double_value", DOUBLE)), tuples_with_null_and_missing);
    assertEquals(7.0, result.value());
  }

  @Test
  public void sum_with_all_missing_or_null() {
    ExprValue result =
        aggregation(DSL.sum(DSL.ref("double_value", DOUBLE)), tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void valueOf() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.sum(DSL.ref("double_value", DOUBLE)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: sum", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator sumAggregator = DSL.sum(DSL.ref("integer_value", INTEGER));
    assertEquals("sum(integer_value)", sumAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator sumAggregator =
        DSL.sum(
            DSL.multiply(
                DSL.ref("integer_value", INTEGER), DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(
        String.format("sum(*(%s, %d))", DSL.ref("integer_value", INTEGER), 10),
        sumAggregator.toString());
  }
}
