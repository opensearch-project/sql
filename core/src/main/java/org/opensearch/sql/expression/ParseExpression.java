/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

/**
 * Named argument expression that represents function argument with name.
 */
@Getter
@EqualsAndHashCode
@ToString
public class ParseExpression implements Expression {
  private static final Logger log = LogManager.getLogger(ParseExpression.class);

  private final Expression expression;
  private final String rawPattern;
  private final String identifier;
  @EqualsAndHashCode.Exclude
  private final Pattern pattern;

  public ParseExpression(Expression expression, String rawPattern, String identifier) {
    this.expression = expression;
    this.rawPattern = rawPattern;
    this.identifier = identifier;
    this.pattern = Pattern.compile(rawPattern);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(expression);
    if (value == null || value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }

    String rawString = value.stringValue();
    Matcher matcher = pattern.matcher(rawString);
    if (matcher.matches()) {
      return new ExprStringValue(matcher.group(identifier));
    }
    log.warn("failed to extract pattern {} from input {}", rawPattern, rawString);
    return new ExprStringValue("");
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
