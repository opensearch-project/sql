/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Pattern;
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
public class PunctExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(PunctExpression.class);
  private static final Pattern IGNORED_CHARS = Pattern.compile("[\\w\\s]");

  /**
   * PunctExpression.
   *
   * @param method     method used to parse
   * @param expression text field
   * @param rawPattern pattern
   * @param identifier named capture group to extract
   */
  public PunctExpression(ParseMethod method, Expression expression, Expression rawPattern,
                         Expression identifier) {
    super(method, expression, rawPattern, identifier);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(expression);
    if (value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }
    String rawString = value.stringValue();
    try {
      return new ExprStringValue(IGNORED_CHARS.matcher(rawString).replaceAll(""));
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
    return ImmutableList.of("punct");
  }
}
