/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalSort;

@UtilityClass
public class OptimizationRuleUtils {

  /**
   * Does the sort list only contain {@link ReferenceExpression}.
   *
   * @param logicalSort LogicalSort.
   * @return true only contain ReferenceExpression, otherwise false.
   */
  public static boolean sortByFieldsOnly(LogicalSort logicalSort) {
    return logicalSort.getSortList().stream()
        .map(sort -> sort.getRight() instanceof ReferenceExpression)
        .reduce(true, Boolean::logicalAnd);
  }

  /**
   * Find reference expression from expression.
   * @param expressions a list of expression.
   *
   * @return a list of ReferenceExpression
   */
  public static Set<ReferenceExpression> findReferenceExpressions(
      List<NamedExpression> expressions) {
    Set<ReferenceExpression> projectList = new HashSet<>();
    for (NamedExpression namedExpression : expressions) {
      projectList.addAll(findReferenceExpression(namedExpression));
    }
    return projectList;
  }

  /**
   * Find reference expression from expression.
   * @param expression expression.
   *
   * @return a list of ReferenceExpression
   */
  public static List<ReferenceExpression> findReferenceExpression(
      NamedExpression expression) {
    List<ReferenceExpression> results = new ArrayList<>();
    expression.accept(new ExpressionNodeVisitor<Object, Object>() {
      @Override
      public Object visitReference(ReferenceExpression node, Object context) {
        return results.add(node);
      }
    }, null);
    return results;
  }
}
