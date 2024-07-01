/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.ast.Node;
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
   * util method could help you to traverse a tree-like expression (e.g {@link FunctionExpression}).
   */
  public static <T extends Expression> List<T> findExpressions(
      Expression expr, Predicate<Expression> condition) {
    List<T> results = new ArrayList<>();
    findExpressionsHelper(expr, condition, results);
    return results;
  }

  private static <T extends Expression> void findExpressionsHelper(
      Expression expr, Predicate<Expression> condition, List<T> results) {
    if (condition.test(expr)) {
      results.add((T) expr);
    }
    if (expr instanceof FunctionExpression) {
      for (Expression child : ((FunctionExpression) expr).getArguments()) {
        findExpressionsHelper(child, condition, results);
      }
    }
  }

  /** Find the all the nodes from a tree that matches the given predicate. */
  public static <T extends Node> List<T> findNodes(Node node, Predicate<Node> condition) {
    List<T> results = new ArrayList<>();
    findNodesHelper(node, condition, results);
    return results;
  }

  public static <T extends Node> void findNodesHelper(
      Node node, Predicate<Node> condition, List<T> results) {
    if (condition.test(node)) {
      results.add((T) node);
    }
    if (node.getChild() != null) {
      for (Node child : node.getChild()) {
        findNodesHelper(child, condition, results);
      }
    }
  }
}
