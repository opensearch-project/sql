/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.expression.span.SpanExpression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class NamedExpressionTest extends ExpressionTestBase {

  @Test
  void name_an_expression() {
    LiteralExpression delegated = DSL.literal(10);
    NamedExpression namedExpression = DSL.named("10", delegated);

    assertEquals("10", namedExpression.getNameOrAlias());
    assertEquals(delegated.type(), namedExpression.type());
    assertEquals(delegated.valueOf(valueEnv()), namedExpression.valueOf(valueEnv()));
  }

  @Test
  void name_an_expression_with_alias() {
    LiteralExpression delegated = DSL.literal(10);
    NamedExpression namedExpression = DSL.named("10", delegated, "ten");
    assertEquals("ten", namedExpression.getNameOrAlias());
  }

  @Test
  void name_an_named_expression() {
    LiteralExpression delegated = DSL.literal(10);
    Expression expression = DSL.named("10", delegated, "ten");

    NamedExpression namedExpression = DSL.named(expression);
    assertEquals("ten", namedExpression.getNameOrAlias());
  }

  @Test
  void name_a_span_expression() {
    SpanExpression span = DSL.span(DSL.ref("integer_value", INTEGER), DSL.literal(1), "");
    NamedExpression named = DSL.named(span);
    assertEquals(span, named.getDelegated());
  }

  @Test
  void name_a_parse_expression() {
    ParseExpression parse =
        DSL.regex(
            DSL.ref("string_value", STRING),
            DSL.literal("(?<group>\\w{2})\\w"),
            DSL.literal("group"));
    NamedExpression named = DSL.named(parse);
    assertEquals(parse, named.getDelegated());
    assertEquals(parse.getIdentifier().valueOf().stringValue(), named.getName());
  }
}
