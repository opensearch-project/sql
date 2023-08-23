/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * OpenSearch functions are FunctionExpression objects used by the core analyzer TODO: move to the
 * OpenSearch module as part of the analyzer and optimizer refactor
 */
public class OpenSearchFunction extends FunctionExpression {
  private final FunctionName functionName;
  private final List<Expression> arguments;

  /**
   * Determines if SCORE needs to be tracked on the function for relevance-based search used by the
   * planner for the OpenSearch request.
   */
  @Getter @Setter private boolean isScoreTracked;

  /**
   * Required argument constructor.
   *
   * @param functionName name of the function
   * @param arguments a list of expressions
   */
  public OpenSearchFunction(FunctionName functionName, List<Expression> arguments) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.isScoreTracked = false;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(
        String.format(
            "OpenSearch defined function [%s] is only supported in WHERE and HAVING clause.",
            functionName));
  }

  @Override
  public ExprType type() {
    return BOOLEAN;
  }

  @Override
  public String toString() {
    List<String> args =
        arguments.stream()
            .map(
                arg ->
                    String.format(
                        "%s=%s",
                        ((NamedArgumentExpression) arg).getArgName(),
                        ((NamedArgumentExpression) arg).getValue().toString()))
            .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }
}
