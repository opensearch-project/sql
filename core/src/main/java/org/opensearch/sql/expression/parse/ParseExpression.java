/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * ParseExpression.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class ParseExpression extends FunctionExpression {
  @Getter
  protected final Expression sourceField;
  protected final Expression pattern;
  @Getter
  protected final Expression identifier;
  protected final String identifierStr;

  /**
   * ParseExpression.
   *
   * @param functionName name of function expression
   * @param sourceField  source text field
   * @param pattern      pattern used for parsing
   * @param identifier   derived field
   */
  public ParseExpression(String functionName, Expression sourceField, Expression pattern,
                         Expression identifier) {
    super(FunctionName.of(functionName), ImmutableList.of(sourceField, pattern, identifier));
    this.sourceField = sourceField;
    this.pattern = pattern;
    this.identifier = identifier;
    this.identifierStr = identifier.valueOf().stringValue();
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(sourceField);
    if (value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }
    try {
      return parseValue(value);
    } catch (ExpressionEvaluationException e) {
      throw new SemanticCheckException(
          String.format("failed to parse field \"%s\" with type [%s]", sourceField, value.type()));
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

  abstract ExprValue parseValue(ExprValue value) throws ExpressionEvaluationException;
}
