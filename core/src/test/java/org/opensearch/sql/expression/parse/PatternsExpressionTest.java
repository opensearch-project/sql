/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PatternsExpressionTest extends ExpressionTestBase {

  @Mock Environment<Expression, ExprValue> env;

  @Test
  public void resolve_value() {
    when(DSL.ref("log_value", STRING).valueOf(env))
        .thenReturn(
            stringValue(
                "145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501"
                    + " 2721"));
    assertEquals(
        stringValue("... - - [//::: -] \" / /.\"  "),
        DSL.patterns(DSL.ref("log_value", STRING), DSL.literal(""), DSL.literal("punct_field"))
            .valueOf(env));
    assertEquals(
        stringValue("... - - [/Aug/::: -] \"GET /deliverables HTTP/.\"  "),
        DSL.patterns(DSL.ref("log_value", STRING), DSL.literal("[0-9]"), DSL.literal("regex_field"))
            .valueOf(env));
  }

  @Test
  public void resolve_null_and_missing_values() {
    assertEquals(
        LITERAL_NULL,
        DSL.patterns(
                DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING),
                DSL.literal("pattern"),
                DSL.literal("patterns_field"))
            .valueOf(valueEnv()));
    assertEquals(
        LITERAL_NULL,
        DSL.patterns(
                DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING),
                DSL.literal("pattern"),
                DSL.literal("patterns_field"))
            .valueOf(valueEnv()));
  }

  @Test
  public void resolve_type() {
    assertEquals(
        STRING,
        DSL.patterns(DSL.ref("string_value", STRING), DSL.literal("pattern"), DSL.literal("group"))
            .type());
  }

  @Test
  public void throws_semantic_exception_if_value_type_is_not_string() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.patterns(
                    DSL.ref("boolean_value", BOOLEAN), DSL.literal("pattern"), DSL.literal("group"))
                .valueOf(valueEnv()));
  }

  @Test
  public void parse_null_or_missing_expr_value() {
    PatternsExpression patternsExpression =
        DSL.patterns(DSL.ref("string_value", STRING), DSL.literal("pattern"), DSL.literal("group"));
    assertEquals(new ExprStringValue(""), patternsExpression.parseValue(ExprNullValue.of()));
    assertEquals(new ExprStringValue(""), patternsExpression.parseValue(ExprMissingValue.of()));
  }

  @Test
  public void get_named_group_candidates_with_default_field() {
    assertEquals(
        ImmutableList.of(PatternsExpression.DEFAULT_NEW_FIELD),
        PatternsExpression.getNamedGroupCandidates(null));
  }

  @Test
  public void get_named_group_candidates_with_specified_field() {
    assertEquals(
        ImmutableList.of("specified_field"),
        PatternsExpression.getNamedGroupCandidates("specified_field"));
  }
}
