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
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;

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
   * @param method     method used to parse
   * @param expression text field
   * @param rawPattern pattern
   * @param identifier named capture group to extract
   */
  public GrokExpression(ParseMethod method, Expression expression, Expression rawPattern,
                        Expression identifier) {
    super(method, expression, rawPattern, identifier);
    this.grok = grokCompiler.compile(rawPattern.valueOf(null).stringValue());
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(expression);
    if (value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }
    String rawString = value.stringValue();
    Match grokMatch = grok.match(rawString);
    Map<String, Object> capture = grokMatch.capture();
    try {
      Object match = capture.get(identifierStr);
      if (match != null) {
        return new ExprStringValue(match.toString());
      }
      log.warn("failed to extract pattern {} from input {}", grok.getOriginalGrokPattern(),
          rawString);
      return new ExprStringValue("");
    } catch (ExpressionEvaluationException e) {
      throw new SemanticCheckException(
          String.format("failed to parse field \"%s\" with type [%s]", expression, value.type()));
    }
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitParse(this, context);
  }

  public static List<String> getNamedGroupCandidates(String pattern) {
    Grok grok = grokCompiler.compile(pattern);
    return grok.namedGroups.stream().map(grok::getNamedRegexCollectionById)
        .filter(group -> !group.equals("UNWANTED")).collect(Collectors.toUnmodifiableList());
  }
}
