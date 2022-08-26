/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
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
public class RegexExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(RegexExpression.class);
  private static final Pattern GROUP_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
  @Getter
  @EqualsAndHashCode.Exclude
  private final Pattern pattern;

  /**
   * RegexExpression.
   *
   * @param method     method used to parse
   * @param expression text field
   * @param rawPattern pattern
   * @param identifier named capture group to extract
   */
  public RegexExpression(ParseMethod method, Expression expression, Expression rawPattern,
                         Expression identifier) {
    super(method, expression, rawPattern, identifier);
    this.pattern = Pattern.compile(rawPattern.valueOf(null).stringValue());
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(expression);
    if (value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }
    String rawString = value.stringValue();
    String identifierStr = identifier.valueOf(null).stringValue();
    try {
      Matcher matcher = pattern.matcher(rawString);
      if (matcher.matches()) {
        return new ExprStringValue(matcher.group(identifierStr));
      }
      log.warn("failed to extract pattern {} from input {}", pattern.pattern(), rawString);
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
    ImmutableList.Builder<String> namedGroups = ImmutableList.builder();
    Matcher m = GROUP_PATTERN.matcher(pattern);
    while (m.find()) {
      namedGroups.add(m.group(1));
    }
    return namedGroups.build();
  }
}
