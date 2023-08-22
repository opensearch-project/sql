/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CastTest {

  @Test
  void cast_to_int_and_integer_should_convert_to_same_function_impl() {
    assertEquals(
        new Cast(booleanLiteral(true), stringLiteral("INT")).convertFunctionName(),
        new Cast(booleanLiteral(true), stringLiteral("INTEGER")).convertFunctionName());
  }
}
