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
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(2), DSL.literal(0)),
            tuples);
    assertEquals(ImmutableList.of("m", "f"), result.value());
  }

  @Test
  public void take_string_field_expression_with_from() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(2), DSL.literal(1)),
            tuples);
    assertEquals(ImmutableList.of("f", "m"), result.value());
  }

  @Test
  public void take_string_field_expression_with_large_size() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0)),
            tuples);
    assertEquals(ImmutableList.of("m", "f", "m", "n"), result.value());
  }

  @Test
  public void take_string_field_expression_with_large_from() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(10)),
            tuples);
    assertEquals(ImmutableList.of(), result.value());
  }

  @Test
  public void filtered_take() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0))
            .condition(dsl.equal(DSL.ref("string_value", STRING), DSL.literal("m"))), tuples);
    assertEquals(ImmutableList.of("m", "m"), result.value());
  }

  @Test
  public void test_take_null() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0)),
            tuples_with_null_and_missing);
    assertEquals(ImmutableList.of("m", "f"), result.value());
  }

  @Test
  public void test_take_missing() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0)),
            tuples_with_null_and_missing);
    assertEquals(ImmutableList.of("m", "f"), result.value());
  }

  @Test
  public void test_take_all_missing_or_null() {
    ExprValue result =
        aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0)),
            tuples_with_all_null_or_missing);
    assertEquals(ImmutableList.of(), result.value());
  }

  @Test
  public void test_take_with_invalid_size() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> aggregation(dsl.take(DSL.ref("string_value", STRING), DSL.literal(0), DSL.literal(0)),
            tuples));
    assertEquals("size must be greater than 0", exception.getMessage());
  }

  @Test
  public void test_take_with_invalid_from() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> aggregation(
            dsl.take(DSL.ref("string_value", STRING), DSL.literal(1), DSL.literal(-1)), tuples));
    assertEquals("from must be greater than or equal to 0", exception.getMessage());
  }

  @Test
  public void test_value_of() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0))
            .valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: take", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator takeAggregator =
        dsl.take(DSL.ref("string_value", STRING), DSL.literal(10), DSL.literal(0));
    assertEquals("take(string_value,10,0)", takeAggregator.toString());
  }
}