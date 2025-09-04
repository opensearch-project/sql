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

class FirstAggregatorTest extends AggregationTest {

  @Test
  public void first_string_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("string_value", STRING)), tuples);
    assertEquals("m", result.value());
  }

  @Test
  public void first_integer_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(2, result.value());
  }

  @Test
  public void first_long_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("long_value", LONG)), tuples);
    assertEquals(2L, result.value());
  }

  @Test
  public void first_float_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("float_value", FLOAT)), tuples);
    assertEquals(2F, result.value());
  }

  @Test
  public void first_double_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("double_value", DOUBLE)), tuples);
    assertEquals(2D, result.value());
  }

  @Test
  public void first_boolean_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("boolean_value", BOOLEAN)), tuples);
    assertEquals(true, result.value());
  }

  @Test
  public void first_date_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("date_value", DATE)), tuples);
    assertEquals("2000-01-01", result.value());
  }

  @Test
  public void first_time_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("time_value", TIME)), tuples);
    assertEquals("12:00:00", result.value());
  }

  @Test
  public void first_timestamp_field_expression() {
    ExprValue result = aggregation(DSL.first(DSL.ref("timestamp_value", TIMESTAMP)), tuples);
    assertEquals("2020-01-01 12:00:00", result.value());
  }

  @Test
  public void filtered_first() {
    ExprValue result =
        aggregation(
            DSL.first(DSL.ref("string_value", STRING))
                .condition(DSL.equal(DSL.ref("string_value", STRING), DSL.literal("f"))),
            tuples);
    assertEquals("f", result.value());
  }

  @Test
  public void test_first_null() {
    ExprValue result =
        aggregation(DSL.first(DSL.ref("string_value", STRING)), tuples_with_null_and_missing);
    // First non-null value is "m"
    assertEquals("m", result.value());
  }

  @Test
  public void test_first_missing() {
    ExprValue result =
        aggregation(DSL.first(DSL.ref("string_value", STRING)), tuples_with_null_and_missing);
    // First non-null/non-missing value is "m"
    assertEquals("m", result.value());
  }

  @Test
  public void test_first_all_missing_or_null() {
    ExprValue result =
        aggregation(DSL.first(DSL.ref("string_value", STRING)), tuples_with_all_null_or_missing);
    assertNull(result.value());
  }

  @Test
  public void test_first_with_no_tuples() {
    ExprValue result =
        aggregation(DSL.first(DSL.ref("string_value", STRING)), Collections.emptyList());
    assertNull(result.value());
  }

  @Test
  public void test_first_value_of() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.first(DSL.ref("string_value", STRING)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: first", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator firstAggregator = DSL.first(DSL.ref("string_value", STRING));
    assertEquals("first(string_value)", firstAggregator.toString());
  }
}
