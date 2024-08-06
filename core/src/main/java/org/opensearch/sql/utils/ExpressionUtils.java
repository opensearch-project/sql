/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Generated;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;

/** Utils for {@link Expression}. */
@UtilityClass
public class ExpressionUtils {

  public static String PATH_SEP = ".";

  /** Format the list of {@link Expression}. */
  public static String format(List<Expression> expressionList) {
    return expressionList.stream().map(Expression::toString).collect(Collectors.joining(","));
  }

  /**
   * Find the children expressions matching the given predicate from an {@link Expression}. This
   * method could help you to traverse an expression and return all sub expressions under the
   * condition you provided.
   *
   * @param expr the expression to traverse
   * @param condition the condition to test
   * @return all sub expressions matching the condition
   */
  public static <T extends Expression> List<T> findSubExpressions(
      Expression expr, Predicate<Expression> condition) {
    List<T> results = new ArrayList<>();
    findSubExpressionsHelper(expr, condition, results);
    return results;
  }

  private static <T extends Expression> void findSubExpressionsHelper(
      Expression expr, Predicate<Expression> condition, List<T> results) {
    if (condition.test(expr)) {
      results.add((T) expr);
    }
    if (expr instanceof FunctionExpression) {
      for (Expression child : ((FunctionExpression) expr).getArguments()) {
        findSubExpressionsHelper(child, condition, results);
      }
    }
  }

  /**
   * Traverse an {@link Expression} to consume when the given predicate matched.
   *
   * <p>Add @Generated annotation since the jacoco has a bug to report test coverage for the Lambda
   * {@code action.accept()}.
   *
   * @param expr the expression to traverse
   * @param condition the condition to test
   * @param action execute the action when the condition matched.
   */
  @Generated
  public static <T extends Expression> void actionOnCheck(
      Expression expr, Predicate<Expression> condition, Consumer<String> action) {
    if (expr instanceof FunctionExpression) {
      if (!condition.test(expr)) {
        return;
      }
      for (Expression child : ((FunctionExpression) expr).getArguments()) {
        actionOnCheck(child, condition, action);
      }
    } else if (condition.test(expr)) {
      action.accept(expr.toString());
    }
  }
}
