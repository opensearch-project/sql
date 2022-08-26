/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode
@ToString
public abstract class ParseExpression extends FunctionExpression {
  @Getter
  protected final Expression expression;
  protected final Expression rawPattern;
  @Getter
  protected final Expression identifier;
  protected final String identifierStr;
  @Getter
  protected final ParseMethod method;

  /**
   * ParseExpression.
   *
   * @param method     method used to parse
   * @param expression text field
   * @param rawPattern pattern
   * @param identifier named capture group to extract
   */
  public ParseExpression(ParseMethod method, Expression expression, Expression rawPattern,
                         Expression identifier) {
    super(FunctionName.of(method.getName()), ImmutableList.of(expression, rawPattern, identifier));
    this.method = method;
    this.expression = expression;
    this.rawPattern = rawPattern;
    this.identifier = identifier;
    this.identifierStr = identifier.valueOf(null).stringValue();
  }
}
