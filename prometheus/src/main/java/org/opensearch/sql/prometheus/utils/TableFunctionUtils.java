/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;

/** Utility class for common table function methods. */
@UtilityClass
public class TableFunctionUtils {

  /**
   * Validates if function arguments are valid in both the cases when the arguments are passed by
   * position or name.
   *
   * @param arguments arguments of function provided in the input order.
   * @param argumentNames ordered argument names of the function.
   */
  public static void validatePrometheusTableFunctionArguments(
      List<Expression> arguments, List<String> argumentNames) {
    Boolean argumentsPassedByName =
        arguments.stream()
            .noneMatch(arg -> StringUtils.isEmpty(((NamedArgumentExpression) arg).getArgName()));
    Boolean argumentsPassedByPosition =
        arguments.stream()
            .allMatch(arg -> StringUtils.isEmpty(((NamedArgumentExpression) arg).getArgName()));
    if (!(argumentsPassedByName || argumentsPassedByPosition)) {
      throw new SemanticCheckException("Arguments should be either passed by name or position");
    }

    if (arguments.size() != argumentNames.size()) {
      throw new SemanticCheckException(
          generateErrorMessageForMissingArguments(
              argumentsPassedByPosition, arguments, argumentNames));
    }
  }

  /**
   * Get Named Arguments of Table Function Arguments. If they are passed by position create new ones
   * or else return the same arguments passed.
   *
   * @param arguments arguments of function provided in the input order.
   * @param argumentNames ordered argument names of the function.
   */
  public static List<Expression> getNamedArgumentsOfTableFunction(
      List<Expression> arguments, List<String> argumentNames) {
    boolean argumentsPassedByPosition =
        arguments.stream()
            .allMatch(arg -> StringUtils.isEmpty(((NamedArgumentExpression) arg).getArgName()));
    if (argumentsPassedByPosition) {
      List<Expression> namedArguments = new ArrayList<>();
      for (int i = 0; i < arguments.size(); i++) {
        namedArguments.add(
            new NamedArgumentExpression(
                argumentNames.get(i), ((NamedArgumentExpression) arguments.get(i)).getValue()));
      }
      return namedArguments;
    }
    return arguments;
  }

  private static String generateErrorMessageForMissingArguments(
      Boolean areArgumentsPassedByPosition,
      List<Expression> arguments,
      List<String> argumentNames) {
    if (areArgumentsPassedByPosition) {
      return String.format(
          "Missing arguments:[%s]",
          String.join(",", argumentNames.subList(arguments.size(), argumentNames.size())));
    } else {
      Set<String> requiredArguments = new HashSet<>(argumentNames);
      Set<String> providedArguments =
          arguments.stream()
              .map(expression -> ((NamedArgumentExpression) expression).getArgName())
              .collect(Collectors.toSet());
      requiredArguments.removeAll(providedArguments);
      return String.format("Missing arguments:[%s]", String.join(",", requiredArguments));
    }
  }
}
