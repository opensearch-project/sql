/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

class LastAggregatorTest extends AggregationTest {

  @Test
  public void last_string_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("string_value", STRING)), tuples);
    assertEquals("n", result.value());
  }

  @Test
  public void last_integer_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(4, result.value());
  }

  @Test
  public void last_long_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("long_value", LONG)), tuples);
    assertEquals(4L, result.value());
  }

  @Test
  public void last_float_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(4F, result.value());
  }

  @Test
  public void last_double_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(4D, result.value());
  }

  @Test
  public void last_boolean_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("boolean_value", BOOLEAN)), tuples);
    assertEquals(true, result.value());
  }

  @Test
  public void last_date_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("date_value", DATE)), tuples);
    assertEquals("2040-01-01", result.value());
  }

  @Test
  public void last_time_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("time_value", TIME)), tuples);
    assertEquals("07:00:00", result.value());
  }

  @Test
  public void last_timestamp_field_expression() {
    ExprValue result = aggregation(DSL.last(DSL.ref("timestamp_value", TIMESTAMP)), tuples);
    assertEquals("2040-01-01 07:00:00", result.value());
  }

  @Test
  public void filtered_last() {
    ExprValue result =
        aggregation(
            DSL.last(DSL.ref("string_value", STRING))
                .condition(DSL.equal(DSL.ref("string_value", STRING), DSL.literal("m"))),
            tuples);
    assertEquals("m", result.value());
  }

  @Test
  public void test_last_null() {
    ExprValue result =
        aggregation(DSL.last(DSL.ref("string_value", STRING)), tuples_with_null_and_missing);
    // Last non-null value is "f" (skips the last null)
    assertEquals("f", result.value());
  }

  @Test
  public void test_last_missing() {
    ExprValue result =
        aggregation(DSL.last(DSL.ref("string_value", STRING)), tuples_with_null_and_missing);
    // Last non-null/non-missing value is "f"
    assertEquals("f", result.value());
  }

  @Test
  public void test_last_all_missing_or_null() {
    ExprValue result =
        aggregation(DSL.last(DSL.ref("string_value", STRING)), tuples_with_all_null_or_missing);
    assertNull(result.value());
  }

  @Test
  public void test_last_with_no_tuples() {
    ExprValue result =
        aggregation(DSL.last(DSL.ref("string_value", STRING)), Collections.emptyList());
    assertNull(result.value());
  }

  @Test
  public void test_last_value_of() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.last(DSL.ref("string_value", STRING)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: last", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator lastAggregator = DSL.last(DSL.ref("string_value", STRING));
    assertEquals("last(string_value)", lastAggregator.toString());
  }
}
