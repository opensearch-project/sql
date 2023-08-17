/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

class AvgAggregatorTest extends AggregationTest {

  @Test
  public void avg_field_expression() {
    ExprValue result = aggregation(DSL.avg(DSL.ref("integer_value", INTEGER)), tuples);
    assertEquals(2.5, result.value());
  }

  @Test
  public void avg_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.avg(
                DSL.multiply(
                    DSL.ref("integer_value", INTEGER),
                    DSL.literal(ExprValueUtils.integerValue(10)))),
            tuples);
    assertEquals(25.0, result.value());
  }

  @Test
  public void filtered_avg() {
    ExprValue result =
        aggregation(
            DSL.avg(DSL.ref("integer_value", INTEGER))
                .condition(DSL.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(3.0, result.value());
  }

  @Test
  public void avg_with_missing() {
    ExprValue result =
        aggregation(DSL.avg(DSL.ref("integer_value", INTEGER)), tuples_with_null_and_missing);
    assertEquals(1.5, result.value());
  }

  @Test
  public void avg_with_null() {
    ExprValue result =
        aggregation(DSL.avg(DSL.ref("double_value", DOUBLE)), tuples_with_null_and_missing);
    assertEquals(3.5, result.value());
  }

  @Test
  public void avg_with_all_missing_or_null() {
    ExprValue result =
        aggregation(DSL.avg(DSL.ref("integer_value", INTEGER)), tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void avg_numeric_no_values() {
    ExprValue result = aggregation(DSL.avg(DSL.ref("dummy", INTEGER)), List.of());
    assertTrue(result.isNull());
  }

  @Test
  public void avg_date_no_values() {
    ExprValue result = aggregation(DSL.avg(DSL.ref("dummy", DATE)), List.of());
    assertTrue(result.isNull());
  }

  @Test
  public void avg_timestamp_no_values() {
    ExprValue result = aggregation(DSL.avg(DSL.ref("dummy", TIMESTAMP)), List.of());
    assertTrue(result.isNull());
  }

  @Test
  public void avg_time_no_values() {
    ExprValue result = aggregation(DSL.avg(DSL.ref("dummy", TIME)), List.of());
    assertTrue(result.isNull());
  }

  @Test
  public void avg_date() {
    ExprValue result = aggregation(DSL.avg(DSL.date(DSL.ref("date_value", STRING))), tuples);
    assertEquals(LocalDate.of(2007, 7, 2), result.dateValue());
  }

  @Test
  public void avg_time() {
    ExprValue result = aggregation(DSL.avg(DSL.time(DSL.ref("time_value", STRING))), tuples);
    assertEquals(LocalTime.of(9, 30), result.timeValue());
  }

  @Test
  public void avg_timestamp() {
    var result = aggregation(DSL.avg(DSL.timestamp(DSL.ref("timestamp_value", STRING))), tuples);
    assertEquals(TIMESTAMP, result.type());
    assertEquals(
        LocalDateTime.of(2012, 7, 2, 3, 30),
        result.timestampValue().atZone(UTC_ZONE_ID).toLocalDateTime());
  }

  @Test
  public void valueOf() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.avg(DSL.ref("double_value", DOUBLE)).valueOf(valueEnv()));
    assertEquals("can't evaluate on aggregator: avg", exception.getMessage());
  }

  @Test
  public void avg_on_unsupported_type() {
    var aggregator = new AvgAggregator(List.of(DSL.ref("string", STRING)), STRING);
    var exception = assertThrows(IllegalArgumentException.class, () -> aggregator.create());
    assertEquals("avg aggregation over STRING type is not supported", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator avgAggregator = DSL.avg(DSL.ref("integer_value", INTEGER));
    assertEquals("avg(integer_value)", avgAggregator.toString());
  }

  @Test
  public void test_nested_to_string() {
    Aggregator avgAggregator =
        DSL.avg(
            DSL.multiply(
                DSL.ref("integer_value", INTEGER), DSL.literal(ExprValueUtils.integerValue(10))));
    assertEquals(
        String.format("avg(*(%s, %d))", DSL.ref("integer_value", INTEGER), 10),
        avgAggregator.toString());
  }
}
