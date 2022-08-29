/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode
@ToString
public class GrokExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(GrokExpression.class);
  private static final GrokCompiler grokCompiler = GrokCompiler.newInstance();

  static {
    grokCompiler.registerDefaultPatterns();
  }

  private final Grok grok;

  /**
   * PunctExpression.
   *
   * @param sourceField source text field
   * @param pattern     pattern used for parsing
   * @param identifier  derived field
   */
  public GrokExpression(Expression sourceField, Expression pattern, Expression identifier) {
    super(ParseMethod.GROK, sourceField, pattern, identifier);
    this.grok = grokCompiler.compile(pattern.valueOf(null).stringValue());
  }

  @Override
  ExprValue parseValue(ExprValue value) {
    String rawString = value.stringValue();
    Match grokMatch = grok.match(rawString);
    Map<String, Object> capture = grokMatch.capture();
    Object match = capture.get(identifierStr);
    if (match != null) {
      return new ExprStringValue(match.toString());
    }
    log.warn("failed to extract pattern {} from input {}", grok.getOriginalGrokPattern(),
        rawString);
    return new ExprStringValue("");
  }

  public static List<String> getNamedGroupCandidates(String pattern) {
    Grok grok = grokCompiler.compile(pattern);
    return grok.namedGroups.stream().map(grok::getNamedRegexCollectionById)
        .filter(group -> !group.equals("UNWANTED")).collect(Collectors.toUnmodifiableList());
  }
}
