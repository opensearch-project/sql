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
import org.opensearch.sql.expression.parse.GrokExpression;
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
      ParseMethod.PUNCT, PunctExpression::new,
      ParseMethod.GROK, GrokExpression::new
  );

  /**
   * Construct corresponding ParseExpression by {@link ParseMethod}.
   *
   * @param parseMethod method used to parse
   * @param sourceField source text field
   * @param pattern     pattern used for parsing
   * @param identifier  derived field
   * @return {@link ParseExpression}
   */
  public static ParseExpression getParseExpression(ParseMethod parseMethod, Expression sourceField,
                                                   Expression pattern, Expression identifier) {
    return FACTORY_MAP.get(parseMethod).initialize(sourceField, pattern, identifier);
  }

  /**
   * Get capture groups from regex pattern.
   *
   * @param pattern pattern used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(ParseMethod parseMethod, String pattern) {
    switch (parseMethod) {
      case REGEX:
        return RegexExpression.getNamedGroupCandidates(pattern);
      case PUNCT:
        return PunctExpression.getNamedGroupCandidates(pattern);
      case GROK:
        return GrokExpression.getNamedGroupCandidates(pattern);
      default:
        return ImmutableList.of();
    }
  }

  private interface ParseExpressionFactory {
    ParseExpression initialize(Expression sourceField, Expression expression,
                               Expression identifier);
  }
}
