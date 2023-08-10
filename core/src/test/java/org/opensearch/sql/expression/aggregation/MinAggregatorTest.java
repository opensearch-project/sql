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

public class MinAggregatorTest extends AggregationTest {

  @Test
  public void test_min_integer() {
    ExprValue result = aggregation(DSL.min(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(1, result.value());
  }

  @Test
  public void test_min_long() {
    ExprValue result = aggregation(DSL.min(DSL.ref("long_value", LONG)), tuples);
    assertEquals(1L, result.value());
  }

  @Test
  public void test_min_float() {
    ExprValue result = aggregation(DSL.min(DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(1F, result.value());
  }

  @Test
  public void test_min_double() {
    ExprValue result = aggregation(DSL.min(DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(1D, result.value());
  }

  @Test
  public void test_min_string() {
    ExprValue result = aggregation(DSL.min(DSL.ref("string_value", STRING)), tuples);
    assertEquals("f", result.value());
  }

  @Test
  public void test_min_date() {
    ExprValue result = aggregation(DSL.min(DSL.ref("date_value", DATE)), tuples);
    assertEquals("1970-01-01", result.value());
  }

  @Test
  public void test_min_datetime() {
    ExprValue result = aggregation(DSL.min(DSL.ref("datetime_value", DATETIME)), tuples);
    assertEquals("1970-01-01 19:00:00", result.value());
  }

  @Test
  public void test_min_time() {
    ExprValue result = aggregation(DSL.min(DSL.ref("time_value", TIME)), tuples);
    assertEquals("00:00:00", result.value());
  }

  @Test
  public void test_min_timestamp() {
    ExprValue result = aggregation(DSL.min(DSL.ref("timestamp_value", TIMESTAMP)), tuples);
    assertEquals("1970-01-01 19:00:00", result.value());
  }

  @Test
  public void test_min_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.min(
                DSL.add(
                    DSL.ref("integer_value", INTEGER),
                    DSL.literal(ExprValueUtils.integerValue(0)))),
            tuples);
    assertEquals(1, result.value());
  }

  @Test
  public void filtered_min() {
    ExprValue result =
        aggregation(
            DSL.min(DSL.ref("integer_value", INTEGER))
                .condition(DSL.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(2, result.value());
  }

  @Test
  public void test_min_null() {
    ExprValue result =
        aggregation(DSL.min(DSL.ref("double_value", DOUBLE)), tuples_with_null_and_missing);
    assertEquals(3.0, result.value());
  }

  @Test
  public void test_min_missing() {
    ExprValue result =
        aggregation(DSL.min(DSL.ref("integer_value", INTEGER)), tuples_with_null_and_missing);
    assertEquals(1, result.value());
  }

  @Test
  public void test_min_all_missing_or_null() {
    ExprValue result =
        aggregation(DSL.min(DSL.ref("integer_value", INTEGER)), tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void test_value_of() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.min(DSL.ref("double_value", DOUBLE)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: min", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator minAggregator = DSL.min(DSL.ref("integer_value", INTEGER));
    assertEquals("min(integer_value)", minAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator minAggregator =
        DSL.min(
            DSL.add(
                DSL.ref("integer_value", INTEGER), DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(
        String.format("min(+(%s, %d))", DSL.ref("integer_value", INTEGER), 10),
        minAggregator.toString());
  }
}
