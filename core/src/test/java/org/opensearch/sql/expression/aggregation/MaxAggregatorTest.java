/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

public class MaxAggregatorTest extends AggregationTest {

  @Test
  public void test_max_integer() {
    ExprValue result = aggregation(DSL.max(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void test_max_long() {
    ExprValue result = aggregation(DSL.max(DSL.ref("long_value", LONG)), tuples);
    assertEquals(4L, result.value());
  }

  @Test
  public void test_max_float() {
    ExprValue result = aggregation(DSL.max(DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(4F, result.value());
  }

  @Test
  public void test_max_double() {
    ExprValue result = aggregation(DSL.max(DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(4D, result.value());
  }

  @Test
  public void test_max_string() {
    ExprValue result = aggregation(DSL.max(DSL.ref("string_value", STRING)), tuples);
    assertEquals("n", result.value());
  }

  @Test
  public void test_max_date() {
    ExprValue result = aggregation(DSL.max(DSL.ref("date_value", DATE)), tuples);
    assertEquals("2040-01-01", result.value());
  }

  @Test
  public void test_max_datetime() {
    ExprValue result = aggregation(DSL.max(DSL.ref("datetime_value", DATETIME)), tuples);
    assertEquals("2040-01-01 07:00:00", result.value());
  }

  @Test
  public void test_max_time() {
    ExprValue result = aggregation(DSL.max(DSL.ref("time_value", TIME)), tuples);
    assertEquals("19:00:00", result.value());
  }

  @Test
  public void test_max_timestamp() {
    ExprValue result = aggregation(DSL.max(DSL.ref("timestamp_value", TIMESTAMP)), tuples);
    assertEquals("2040-01-01 07:00:00", result.value());
  }

  @Test
  public void test_max_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.max(
                DSL.add(
                    DSL.ref("integer_value", INTEGER),
                    DSL.literal(ExprValueUtils.integerValue(0)))),
            tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void filtered_max() {
    ExprValue result =
        aggregation(
            DSL.max(DSL.ref("integer_value", INTEGER))
                .condition(DSL.less(DSL.ref("integer_value", INTEGER), DSL.literal(4))),
            tuples);
    assertEquals(3, result.value());
  }

  @Test
  public void test_max_null() {
    ExprValue result =
        aggregation(DSL.max(DSL.ref("double_value", DOUBLE)), tuples_with_null_and_missing);
    assertEquals(4.0, result.value());
  }

  @Test
  public void test_max_missing() {
    ExprValue result =
        aggregation(DSL.max(DSL.ref("integer_value", INTEGER)), tuples_with_null_and_missing);
    assertEquals(2, result.value());
  }

  @Test
  public void test_max_all_missing_or_null() {
    ExprValue result =
        aggregation(DSL.max(DSL.ref("integer_value", INTEGER)), tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void test_value_of() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.max(DSL.ref("double_value", DOUBLE)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: max", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator maxAggregator = DSL.max(DSL.ref("integer_value", INTEGER));
    assertEquals("max(integer_value)", maxAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator maxAggregator =
        DSL.max(
            DSL.add(
                DSL.ref("integer_value", INTEGER), DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(
        String.format("max(+(%s, %d))", DSL.ref("integer_value", INTEGER), 10),
        maxAggregator.toString());
  }
}
