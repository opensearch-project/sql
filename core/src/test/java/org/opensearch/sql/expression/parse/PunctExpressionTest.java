/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PunctExpressionTest extends ExpressionTestBase {

  @Test
  public void resolve_value() {
    Assertions.assertEquals(stringValue(""),
        DSL.punct(DSL.ref("string_value", STRING), DSL.literal("punct_field"),
            DSL.literal("punct_field")).valueOf(valueEnv()));
    assertEquals(LITERAL_NULL,
        DSL.punct(DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING),
            DSL.literal("punct_field"), DSL.literal("punct_field")).valueOf(valueEnv()));
    assertEquals(LITERAL_NULL,
        DSL.punct(DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING),
            DSL.literal("punct_field"), DSL.literal("punct_field")).valueOf(valueEnv()));
  }

  @Test
  public void resolve_type() {
    assertEquals(STRING,
        DSL.punct(DSL.ref("string_value", STRING), DSL.literal("(?<group>\\w{2})\\w"),
            DSL.literal("group")).type());
  }

  @Test
  public void throws_semantic_exception_if_value_type_is_not_string() {
    assertThrows(
        SemanticCheckException.class,
        () -> DSL.punct(DSL.ref("boolean_value", BOOLEAN), DSL.literal("(?<group>\\w{2})\\w"),
                DSL.literal("group"))
            .valueOf(valueEnv()));
  }
}
