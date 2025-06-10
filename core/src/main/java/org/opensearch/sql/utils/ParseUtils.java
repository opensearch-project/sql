/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.parse.GrokExpression;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.parse.RegexExpression;

/** Utils for {@link ParseExpression}. */
@UtilityClass
public class ParseUtils {
  private static final String NEW_FIELD_KEY = "new_field";
  private static final Map<ParseMethod, ParseExpressionFactory> FACTORY_MAP =
      ImmutableMap.of(
          ParseMethod.REGEX, RegexExpression::new,
          ParseMethod.GROK, GrokExpression::new,
          ParseMethod.PATTERNS, PatternsExpression::new);
  public static final Map<ParseMethod, BuiltinFunctionName> BUILTIN_FUNCTION_MAP =
      ImmutableMap.of(
          ParseMethod.REGEX, BuiltinFunctionName.INTERNAL_REGEXP_EXTRACT,
          ParseMethod.GROK, BuiltinFunctionName.INTERNAL_GROK,
          ParseMethod.PATTERNS, BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_2);

  /**
   * Construct corresponding ParseExpression by {@link ParseMethod}.
   *
   * @param parseMethod method used to parse
   * @param sourceField source text field
   * @param pattern pattern used for parsing
   * @param identifier derived field
   * @return {@link ParseExpression}
   */
  public static ParseExpression createParseExpression(
      ParseMethod parseMethod, Expression sourceField, Expression pattern, Expression identifier) {
    return FACTORY_MAP.get(parseMethod).initialize(sourceField, pattern, identifier);
  }

  /**
   * Get list of derived fields based on parse pattern.
   *
   * @param pattern pattern used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(
      ParseMethod parseMethod, String pattern, Map<String, Literal> arguments) {
    switch (parseMethod) {
      case REGEX:
        return RegexExpression.getNamedGroupCandidates(pattern);
      case GROK:
        return GrokExpression.getNamedGroupCandidates(pattern);
      default:
        return PatternsExpression.getNamedGroupCandidates(
            arguments.containsKey(NEW_FIELD_KEY)
                ? (String) arguments.get(NEW_FIELD_KEY).getValue()
                : null);
    }
  }

  private interface ParseExpressionFactory {
    ParseExpression initialize(
        Expression sourceField, Expression expression, Expression identifier);
  }
}
