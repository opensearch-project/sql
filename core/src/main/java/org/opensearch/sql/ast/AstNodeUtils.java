/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.subquery.SubqueryExpression;

/** Utility class for AST node operations shared among visitor classes. */
@UtilityClass
public class AstNodeUtils {

  /**
   * Checks if an AST node contains a subquery expression.
   *
   * @param expr The AST node to check
   * @return true if the node or any of its children contains a subquery expression
   */
  public static boolean containsSubqueryExpression(Node expr) {
    if (expr == null) {
      return false;
    }
    if (expr instanceof SubqueryExpression) {
      return true;
    }
    if (expr instanceof Let) {
      Let l = (Let) expr;
      return containsSubqueryExpression(l.getExpression());
    }
    for (Node child : expr.getChild()) {
      if (containsSubqueryExpression(child)) {
        return true;
      }
    }
    return false;
  }
}
