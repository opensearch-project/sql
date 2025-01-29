/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.List;
import java.util.Optional;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;

public final class FunctionUtils {

  public static Optional<ExprValue> getNamedArgumentValue(
      List<Expression> arguments, String argName) {
    return arguments.stream()
        .filter(
            expression ->
                expression instanceof NamedArgumentExpression
                    && ((NamedArgumentExpression) expression)
                        .getArgName()
                        .equalsIgnoreCase(argName))
        .map(expression -> ((NamedArgumentExpression) expression).getValue().valueOf())
        .findFirst();
  }
}
