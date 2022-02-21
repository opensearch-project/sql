/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.utils.ParseUtils;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode
@ToString
public class ParseExpression implements Expression {
  @Getter
  private final Expression expression;
  private final String rawPattern;
  @Getter
  private final String identifier;
  @Getter
  @EqualsAndHashCode.Exclude
  private final Pattern pattern;

  /**
   * ParseExpression.
   *
   * @param expression text field
   * @param rawPattern regex
   * @param identifier named capture group to extract
   */
  public ParseExpression(Expression expression, String rawPattern, String identifier) {
    this.expression = expression;
    this.rawPattern = rawPattern;
    this.identifier = identifier;
    this.pattern = Pattern.compile(rawPattern);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(expression);
    try {
      return ParseUtils.parseValue(value, pattern, identifier);
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
}
