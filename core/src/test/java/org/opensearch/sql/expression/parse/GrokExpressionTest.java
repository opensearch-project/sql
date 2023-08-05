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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class GrokExpressionTest extends ExpressionTestBase {

  @Mock Environment<Expression, ExprValue> env;

  @Test
  public void resolve_grok_groups_and_parsed_values() {
    when(DSL.ref("log_value", STRING).valueOf(env))
        .thenReturn(
            stringValue(
                "145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501"
                    + " 2721"));

    String rawPattern = "%{COMMONAPACHELOG}";
    Map<String, String> expected =
        ImmutableMap.<String, String>builder()
            .put(
                "COMMONAPACHELOG",
                "145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] "
                    + "\"GET /deliverables HTTP/2.0\" 501 2721")
            .put("clientip", "145.128.75.121")
            .put("ident", "-")
            .put("auth", "-")
            .put("timestamp", "29/Aug/2022:13:26:44 -0700")
            .put("MONTHDAY", "29")
            .put("MONTH", "Aug")
            .put("YEAR", "2022")
            .put("TIME", "13:26:44")
            .put("HOUR", "13")
            .put("MINUTE", "26")
            .put("SECOND", "44")
            .put("INT", "-0700")
            .put("verb", "GET")
            .put("request", "/deliverables")
            .put("httpversion", "2.0")
            .put("rawrequest", "")
            .put("response", "501")
            .put("bytes", "2721")
            .build();
    List<String> identifiers = new ArrayList<>(expected.keySet());
    assertEquals(identifiers, GrokExpression.getNamedGroupCandidates(rawPattern));
    identifiers.forEach(
        identifier ->
            assertEquals(
                stringValue(expected.get(identifier)),
                DSL.grok(
                        DSL.ref("log_value", STRING),
                        DSL.literal(rawPattern),
                        DSL.literal(identifier))
                    .valueOf(env)));
  }

  @Test
  public void resolve_null_and_empty_values() {
    assertEquals(
        stringValue(""),
        DSL.grok(
                DSL.ref("string_value", STRING),
                DSL.literal("%{COMMONAPACHELOG}"),
                DSL.literal("request"))
            .valueOf(valueEnv()));
    assertEquals(
        LITERAL_NULL,
        DSL.grok(
                DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING),
                DSL.literal("%{COMMONAPACHELOG}"),
                DSL.literal("request"))
            .valueOf(valueEnv()));
    assertEquals(
        LITERAL_NULL,
        DSL.grok(
                DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING),
                DSL.literal("p%{COMMONAPACHELOG}"),
                DSL.literal("request"))
            .valueOf(valueEnv()));
  }

  @Test
  public void resolve_type() {
    assertEquals(
        STRING,
        DSL.grok(
                DSL.ref("string_value", STRING),
                DSL.literal("%{COMMONAPACHELOG}"),
                DSL.literal("request"))
            .type());
  }

  @Test
  public void throws_semantic_exception_if_value_type_is_not_string() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.grok(
                    DSL.ref("boolean_value", BOOLEAN),
                    DSL.literal("%{COMMONAPACHELOG}"),
                    DSL.literal("request"))
                .valueOf(valueEnv()));
  }
}
