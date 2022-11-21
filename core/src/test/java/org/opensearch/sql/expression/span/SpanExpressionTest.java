/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.span;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class SpanExpressionTest extends ExpressionTestBase {
  @Test
  void testSpanByNumeric() {
    SpanExpression span = DSL.span(DSL.ref("integer_value", INTEGER), DSL.literal(1), "");
    assertEquals(INTEGER, span.type());
    assertEquals(ExprValueUtils.integerValue(1), span.valueOf(valueEnv()));

    span = DSL.span(DSL.ref("integer_value", INTEGER), DSL.literal(1.5), "");
    assertEquals(DOUBLE, span.type());
    assertEquals(ExprValueUtils.doubleValue(0.0), span.valueOf(valueEnv()));

    span = DSL.span(DSL.ref("double_value", DOUBLE), DSL.literal(1), "");
    assertEquals(DOUBLE, span.type());
    assertEquals(ExprValueUtils.doubleValue(1.0), span.valueOf(valueEnv()));
  }
}
