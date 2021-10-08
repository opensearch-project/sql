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
    NamedArgumentExpression namedArgument = dsl.namedArgument("query", value);

    assertEquals("query", namedArgument.getArgName());
    assertEquals(value.type(), namedArgument.type());
    assertEquals(value.valueOf(valueEnv()), namedArgument.valueOf(valueEnv()));
  }
}
