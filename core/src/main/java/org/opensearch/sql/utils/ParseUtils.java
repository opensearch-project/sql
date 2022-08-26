/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.expression.parse.PunctExpression;
import org.opensearch.sql.expression.parse.RegexExpression;

/**
 * Utils for {@link ParseExpression}.
 */
@UtilityClass
public class ParseUtils {
  private static final Map<ParseMethod, ParseExpressionFactory> FACTORY_MAP = ImmutableMap.of(
      ParseMethod.REGEX, RegexExpression::new,
      ParseMethod.PUNCT, PunctExpression::new
  );

  /**
   * Construct corresponding ParseExpression by {@link ParseMethod}.
   *
   * @param parseMethod
   * @param expression
   * @param rawPattern
   * @param identifier
   * @return
   */
  public static ParseExpression getParseExpression(ParseMethod parseMethod, Expression expression,
                                                   Expression rawPattern, Expression identifier) {
    return FACTORY_MAP.get(parseMethod).initialize(parseMethod, expression, rawPattern, identifier);
  }

  /**
   * Get capture groups from regex pattern.
   *
   * @param pattern regex pattern
   * @return list of named capture groups in regex pattern
   */
  public static List<String> getNamedGroupCandidates(ParseMethod parseMethod, String pattern) {
    switch (parseMethod) {
      case REGEX:
        return RegexExpression.getNamedGroupCandidates(pattern);
      case PUNCT:
        return PunctExpression.getNamedGroupCandidates(pattern);
      default:
        return ImmutableList.of();
    }
  }

  private interface ParseExpressionFactory {
    ParseExpression initialize(ParseMethod parseMethod, Expression expression,
                               Expression rawPattern, Expression identifier);
  }
}
