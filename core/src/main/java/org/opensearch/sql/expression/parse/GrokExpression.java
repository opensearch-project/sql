/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.grok.Grok;
import org.opensearch.sql.common.grok.GrokCompiler;
import org.opensearch.sql.common.grok.Match;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;

/** GrokExpression with grok patterns. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class GrokExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(GrokExpression.class);
  private static final GrokCompiler grokCompiler = GrokCompiler.newInstance();

  static {
    grokCompiler.registerDefaultPatterns();
  }

  @EqualsAndHashCode.Exclude private final Grok grok;

  /**
   * GrokExpression.
   *
   * @param sourceField source text field
   * @param pattern pattern used for parsing
   * @param identifier derived field
   */
  public GrokExpression(Expression sourceField, Expression pattern, Expression identifier) {
    super("grok", sourceField, pattern, identifier);
    this.grok = grokCompiler.compile(pattern.valueOf().stringValue());
  }

  @Override
  public ExprValue parseValue(ExprValue value) throws ExpressionEvaluationException {
    String rawString = value.stringValue();
    Match grokMatch = grok.match(rawString);
    Map<String, Object> capture = grokMatch.capture();
    Object match = capture.get(identifierStr);
    if (match != null) {
      return new ExprStringValue(match.toString());
    }
    log.debug("failed to extract pattern {} from input ***", grok.getOriginalGrokPattern());
    return new ExprStringValue("");
  }

  /**
   * Get list of derived fields based on parse pattern.
   *
   * @param pattern pattern used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(String pattern) {
    Grok grok = grokCompiler.compile(pattern);
    return grok.namedGroups.stream()
        .map(grok::getNamedRegexCollectionById)
        .filter(group -> !group.equals("UNWANTED"))
        .collect(Collectors.toUnmodifiableList());
  }
}
