/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import com.google.common.collect.ImmutableList;
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
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.utils.ParseUtils;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode
@ToString
public class ParseExpression extends FunctionExpression {
  @Getter
  private final Expression expression;
  private final Expression rawPattern;
  @Getter
  private final Expression identifier;
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
  public ParseExpression(Expression expression, Expression rawPattern, Expression identifier) {
    super(FunctionName.of("parse"), ImmutableList.of(expression, rawPattern, identifier));
    this.expression = expression;
    this.rawPattern = rawPattern;
    this.identifier = identifier;
    this.pattern = Pattern.compile(rawPattern.valueOf(null).stringValue());
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(expression);
    try {
      return ParseUtils.parseValue(value, pattern, identifier.valueOf(null).stringValue());
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
