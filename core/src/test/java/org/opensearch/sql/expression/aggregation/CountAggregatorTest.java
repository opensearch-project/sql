/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

class CountAggregatorTest extends AggregationTest {

  @Test
  public void count_integer_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_long_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("long_value", LONG)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_float_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_double_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_date_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("date_value", DATE)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_timestamp_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("timestamp_value", TIMESTAMP)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.count(
                DSL.multiply(
                    DSL.ref("integer_value", INTEGER),
                    DSL.literal(ExprValueUtils.integerValue(10)))),
            tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_string_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("string_value", STRING)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void count_boolean_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("boolean_value", BOOLEAN)), tuples);
    assertEquals(1, result.value());
  }

  @Test
  public void count_struct_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("struct_value", STRUCT)), tuples);
    assertEquals(1, result.value());
  }

  @Test
  public void count_array_field_expression() {
    ExprValue result = aggregation(DSL.count(DSL.ref("array_value", ARRAY)), tuples);
    assertEquals(1, result.value());
  }

  @Test
  public void filtered_count() {
    ExprValue result =
        aggregation(
            DSL.count(DSL.ref("integer_value", INTEGER))
                .condition(DSL.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(3, result.value());
  }

  @Test
  public void distinct_count() {
    ExprValue result =
        aggregation(DSL.distinctCount(DSL.ref("integer_value", INTEGER)), tuples_with_duplicates);
    assertEquals(3, result.value());
  }

  @Test
  public void filtered_distinct_count() {
    ExprValue result =
        aggregation(
            DSL.distinctCount(DSL.ref("integer_value", INTEGER))
                .condition(DSL.greater(DSL.ref("double_value", DOUBLE), DSL.literal(1d))),
            tuples_with_duplicates);
    assertEquals(2, result.value());
  }

  @Test
  public void distinct_count_map() {
    ExprValue result =
        aggregation(DSL.distinctCount(DSL.ref("struct_value", STRUCT)), tuples_with_duplicates);
    assertEquals(3, result.value());
  }

  @Test
  public void distinct_count_array() {
    ExprValue result =
        aggregation(DSL.distinctCount(DSL.ref("array_value", ARRAY)), tuples_with_duplicates);
    assertEquals(3, result.value());
  }

  @Test
  public void count_with_missing() {
    ExprValue result =
        aggregation(DSL.count(DSL.ref("integer_value", INTEGER)), tuples_with_null_and_missing);
    assertEquals(2, result.value());
  }

  @Test
  public void count_with_null() {
    ExprValue result =
        aggregation(DSL.count(DSL.ref("double_value", DOUBLE)), tuples_with_null_and_missing);
    assertEquals(2, result.value());
  }

  @Test
  public void count_star_with_null_and_missing() {
    ExprValue result = aggregation(DSL.count(DSL.literal("*")), tuples_with_null_and_missing);
    assertEquals(3, result.value());
  }

  @Test
  public void count_literal_with_null_and_missing() {
    ExprValue result = aggregation(DSL.count(DSL.literal(1)), tuples_with_null_and_missing);
    assertEquals(3, result.value());
  }

  @Test
  public void valueOf() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.count(DSL.ref("double_value", DOUBLE)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: count", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator countAggregator = DSL.count(DSL.ref("integer_value", INTEGER));
    assertEquals("count(integer_value)", countAggregator.toString());

    countAggregator = DSL.distinctCount(DSL.ref("integer_value", INTEGER));
    assertEquals("count(distinct integer_value)", countAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator countAggregator = DSL.count(DSL.abs(DSL.ref("integer_value", INTEGER)));
    assertEquals(
        String.format("count(abs(%s))", DSL.ref("integer_value", INTEGER)),
        countAggregator.toString());
  }
}
