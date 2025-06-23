/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.collector;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.span.SpanExpression;

public class RoundingTest {
  @Test
  void time_rounding_illegal_span() {
    SpanExpression span = DSL.span(DSL.ref("time", TIME), DSL.literal(1), "d");
    Rounding rounding = Rounding.createRounding(span);
    assertThrows(
        ExpressionEvaluationException.class, () -> rounding.round(new ExprTimeValue("23:30:00")));
  }

  @Test
  void datetime_rounding_span() {
    SpanExpression dateSpan = DSL.span(DSL.ref("date", DATE), DSL.literal(1), "d");
    Rounding rounding = Rounding.createRounding(dateSpan);
    assertInstanceOf(Rounding.DateRounding.class, rounding);
    SpanExpression timeSpan = DSL.span(DSL.ref("time", TIME), DSL.literal(1), "h");
    rounding = Rounding.createRounding(timeSpan);
    assertInstanceOf(Rounding.TimeRounding.class, rounding);
    SpanExpression timestampSpan = DSL.span(DSL.ref("timestamp", TIMESTAMP), DSL.literal(1), "h");
    rounding = Rounding.createRounding(timestampSpan);
    assertInstanceOf(Rounding.TimestampRounding.class, rounding);
  }

  @Test
  void datetime_rounding_non_core_type_span() {
    SpanExpression dateSpan =
        DSL.span(DSL.ref("date", new MockDateExprType()), DSL.literal(1), "d");
    Rounding rounding = Rounding.createRounding(dateSpan);
    assertInstanceOf(Rounding.DateRounding.class, rounding);
    SpanExpression timeSpan =
        DSL.span(DSL.ref("time", new MockTimeExprType()), DSL.literal(1), "h");
    rounding = Rounding.createRounding(timeSpan);
    assertInstanceOf(Rounding.TimeRounding.class, rounding);
    SpanExpression timestampSpan =
        DSL.span(DSL.ref("timestamp", new MockTimestampExprType()), DSL.literal(1), "h");
    rounding = Rounding.createRounding(timestampSpan);
    assertInstanceOf(Rounding.TimestampRounding.class, rounding);
    SpanExpression datetimeSpan =
        DSL.span(DSL.ref("datetime", new MockDateTimeExprType()), DSL.literal(1), "h");
    rounding = Rounding.createRounding(datetimeSpan);
    assertInstanceOf(Rounding.DatetimeRounding.class, rounding);
  }

  @Test
  void round_unknown_type() {
    SpanExpression span = DSL.span(DSL.ref("unknown", STRING), DSL.literal(1), "");
    Rounding rounding = Rounding.createRounding(span);
    assertNull(rounding.round(ExprValueUtils.integerValue(1)));
  }

  @Test
  void resolve() {
    String illegalUnit = "illegal";
    assertThrows(
        IllegalArgumentException.class,
        () -> Rounding.DateTimeUnit.resolve(illegalUnit),
        "Unable to resolve unit " + illegalUnit);
  }

  static class MockDateExprType implements ExprType {
    @Override
    public String typeName() {
      return "DATE";
    }
  }

  static class MockTimeExprType implements ExprType {
    @Override
    public String typeName() {
      return "TIME";
    }
  }

  static class MockTimestampExprType implements ExprType {
    @Override
    public String typeName() {
      return "TIMESTAMP";
    }
  }

  static class MockDateTimeExprType implements ExprType {
    @Override
    public String typeName() {
      return "DATETIME";
    }
  }
}
