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
class RegexExpressionTest extends ExpressionTestBase {

  @Mock Environment<Expression, ExprValue> env;

  @Test
  public void resolve_regex_groups_and_parsed_values() {
    when(DSL.ref("log_value", STRING).valueOf(env))
        .thenReturn(
            stringValue(
                "130.246.123.197 - - [2018-07-22T03:26:21.326Z] \"GET /beats/metricbeat_1"
                    + " HTTP/1.1\" 200 6850 \"-\" \"Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1)"
                    + " Gecko/20110421 Firefox/6.0a1\""));

    String rawPattern =
        "(?<ip>(\\d{1,3}\\.){3}\\d{1,3}) - - \\[(?<date>\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:"
            + "[0-5]\\d:[0-5]\\d\\.\\d+([+-][0-2]\\d:[0-5]\\d|Z))] \"(?<request>[^\"]+)\" "
            + "(?<status>\\d+) (?<bytes>\\d+) \"-\" \"(?<userAgent>[^\"]+)\"";
    Map<String, String> expected =
        ImmutableMap.of(
            "ip",
            "130.246.123.197",
            "date",
            "2018-07-22T03:26:21.326Z",
            "request",
            "GET /beats/metricbeat_1 HTTP/1.1",
            "status",
            "200",
            "bytes",
            "6850",
            "userAgent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1");
    List<String> identifiers = new ArrayList<>(expected.keySet());
    assertEquals(identifiers, RegexExpression.getNamedGroupCandidates(rawPattern));
    identifiers.forEach(
        identifier ->
            assertEquals(
                stringValue(expected.get(identifier)),
                DSL.regex(
                        DSL.ref("log_value", STRING),
                        DSL.literal(rawPattern),
                        DSL.literal(identifier))
                    .valueOf(env)));
  }

  @Test
  public void resolve_not_parsable_inputs_as_empty_string() {
    assertEquals(
        stringValue(""),
        DSL.regex(
                DSL.ref("string_value", STRING),
                DSL.literal("(?<group>not-matching)"),
                DSL.literal("group"))
            .valueOf(valueEnv()));
  }

  @Test
  public void resolve_null_and_missing_values() {
    assertEquals(
        LITERAL_NULL,
        DSL.regex(
                DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING),
                DSL.literal("(?<group>\\w{2})\\w"),
                DSL.literal("group"))
            .valueOf(valueEnv()));
    assertEquals(
        LITERAL_NULL,
        DSL.regex(
                DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING),
                DSL.literal("(?<group>\\w{2})\\w"),
                DSL.literal("group"))
            .valueOf(valueEnv()));
  }

  @Test
  public void resolve_type() {
    assertEquals(
        STRING,
        DSL.regex(
                DSL.ref("string_value", STRING),
                DSL.literal("(?<group>\\w{2})\\w"),
                DSL.literal("group"))
            .type());
  }

  @Test
  public void throws_semantic_exception_if_value_type_is_not_string() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.regex(
                    DSL.ref("boolean_value", BOOLEAN),
                    DSL.literal("(?<group>\\w{2})\\w"),
                    DSL.literal("group"))
                .valueOf(valueEnv()));
  }
}
