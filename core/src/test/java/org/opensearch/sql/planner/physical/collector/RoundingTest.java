/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.collector;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValueUtils;
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
}
