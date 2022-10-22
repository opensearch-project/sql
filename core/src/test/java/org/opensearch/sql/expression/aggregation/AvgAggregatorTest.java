/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.planner.physical.SessionContext;
import org.opensearch.sql.expression.DSL;

class AvgAggregatorTest extends AggregationTest {

  @Test
  public void avg_field_expression() {
    ExprValue result = aggregation(dsl.avg(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(2.5, result.value());
  }

  @Test
  public void avg_arithmetic_expression() {
    ExprValue result = aggregation(dsl.avg(
        dsl.multiply(DSL.ref("integer_value", INTEGER),
            DSL.literal(ExprValueUtils.integerValue(10)))), tuples);
    assertEquals(25.0, result.value());
  }

  @Test
  public void filtered_avg() {
    ExprValue result = aggregation(dsl.avg(DSL.ref("integer_value", INTEGER))
        .condition(dsl.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))), tuples);
    assertEquals(3.0, result.value());
  }

  @Test
  public void avg_with_missing() {
    ExprValue result =
        aggregation(dsl.avg(DSL.ref("integer_value", INTEGER)), tuples_with_null_and_missing);
    assertEquals(1.5, result.value());
  }

  @Test
  public void avg_with_null() {
    ExprValue result =
        aggregation(dsl.avg(DSL.ref("double_value", DOUBLE)), tuples_with_null_and_missing);
    assertEquals(3.5, result.value());
  }

  @Test
  public void avg_with_all_missing_or_null() {
    ExprValue result =
        aggregation(dsl.avg(DSL.ref("integer_value", INTEGER)), tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void valueOf() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> dsl.avg(DSL.ref("double_value", DOUBLE)).valueOf(valueEnv(), SessionContext.None));
    assertEquals("can't evaluate on aggregator: avg", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator avgAggregator = dsl.avg(DSL.ref("integer_value", INTEGER));
    assertEquals("avg(integer_value)", avgAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator avgAggregator = dsl.avg(dsl.multiply(DSL.ref("integer_value", INTEGER),
        DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(String.format("avg(*(%s, %d))", DSL.ref("integer_value", INTEGER), 10),
        avgAggregator.toString());
  }
}
