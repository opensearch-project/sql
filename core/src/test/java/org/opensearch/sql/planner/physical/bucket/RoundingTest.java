/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.planner.physical.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.span.SpanExpression;

public class RoundingTest {
  @Test
  void time_rounding_illegal_span() {
    SpanExpression span = DSL.span(DSL.ref("time", TIME), DSL.literal(1), "d");
    Rounding rounding = Rounding.createRounding(span);
    assertThrows(ExpressionEvaluationException.class,
        () -> rounding.round(new ExprTimeValue("23:30:00")));
  }

  @Test
  void round_unknown_type() {
    SpanExpression span = DSL.span(DSL.ref("unknown", STRING), DSL.literal(1), "");
    Rounding rounding = Rounding.createRounding(span);
    assertNull(rounding.round(ExprValueUtils.integerValue(1)));
    assertNull(rounding.locate(ExprValueUtils.integerValue(1)));
    assertEquals(0, rounding.createBuckets().length);
    assertEquals(0, rounding.fillBuckets(new ExprValue[0], ImmutableMap.of(), "span").length);
  }

  @Test
  void resolve() {
    String illegalUnit = "illegal";
    assertThrows(IllegalArgumentException.class,
        () -> Rounding.DateTimeUnit.resolve(illegalUnit),
        "Unable to resolve unit " + illegalUnit);
  }
}
