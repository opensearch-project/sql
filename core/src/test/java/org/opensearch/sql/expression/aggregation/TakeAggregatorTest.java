/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

class TakeAggregatorTest extends AggregationTest {

  @Test
  public void take_string_field_expression() {
    ExprValue result =
        aggregation(DSL.take(DSL.ref("string_value", STRING), DSL.literal(2)), tuples);
    assertEquals(ImmutableList.of("m", "f"), result.value());
  }

  @Test
  public void take_string_field_expression_with_large_size() {
    ExprValue result =
        aggregation(DSL.take(DSL.ref("string_value", STRING), DSL.literal(10)), tuples);
    assertEquals(ImmutableList.of("m", "f", "m", "n"), result.value());
  }

  @Test
  public void filtered_take() {
    ExprValue result =
        aggregation(
            DSL.take(DSL.ref("string_value", STRING), DSL.literal(10))
                .condition(DSL.equal(DSL.ref("string_value", STRING), DSL.literal("m"))),
            tuples);
    assertEquals(ImmutableList.of("m", "m"), result.value());
  }

  @Test
  public void test_take_null() {
    ExprValue result =
        aggregation(
            DSL.take(DSL.ref("string_value", STRING), DSL.literal(10)),
            tuples_with_null_and_missing);
    assertEquals(ImmutableList.of("m", "f"), result.value());
  }

  @Test
  public void test_take_missing() {
    ExprValue result =
        aggregation(
            DSL.take(DSL.ref("string_value", STRING), DSL.literal(10)),
            tuples_with_null_and_missing);
    assertEquals(ImmutableList.of("m", "f"), result.value());
  }

  @Test
  public void test_take_all_missing_or_null() {
    ExprValue result =
        aggregation(
            DSL.take(DSL.ref("string_value", STRING), DSL.literal(10)),
            tuples_with_all_null_or_missing);
    assertEquals(ImmutableList.of(), result.value());
  }

  @Test
  public void test_take_with_invalid_size() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> aggregation(DSL.take(DSL.ref("string_value", STRING), DSL.literal(0)), tuples));
    assertEquals("size must be greater than 0", exception.getMessage());
  }

  @Test
  public void test_value_of() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.take(DSL.ref("string_value", STRING), DSL.literal(10)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: take", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator takeAggregator = DSL.take(DSL.ref("string_value", STRING), DSL.literal(10));
    assertEquals("take(string_value,10)", takeAggregator.toString());
  }
}
