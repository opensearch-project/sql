/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class NamedArgumentExpressionTest extends ExpressionTestBase {
  @Test
  void name_an_argument() {
    LiteralExpression value = DSL.literal("search");
    NamedArgumentExpression namedArgument = DSL.namedArgument("query", value);

    assertEquals("query", namedArgument.getArgName());
    assertEquals(value.type(), namedArgument.type());
    assertEquals(value.valueOf(valueEnv()), namedArgument.valueOf(valueEnv()));
  }
}
