/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.expression.ip;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Marker class to identify functions only compatible with OpenSearch storage engine. Any attempt to
 * invoke the method different from OpenSearch will result in UnsupportedOperationException.
 */
public class OpenSearchFunctionExpression extends FunctionExpression {

  private final ExprType returnType;

  public OpenSearchFunctionExpression(
      FunctionName functionName, List<Expression> arguments, ExprType returnType) {
    super(functionName, arguments);
    this.returnType = returnType;
  }

  @Override
  public ExprValue valueOf() {
    return null;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(
        "OpenSearch runtime specific function, no default implementation available");
  }

  @Override
  public ExprType type() {
    return returnType;
  }
}
