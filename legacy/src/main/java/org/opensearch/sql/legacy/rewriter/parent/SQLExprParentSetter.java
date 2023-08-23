/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.parent;

import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLNotExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

/** Add the parent for the node in {@link SQLQueryExpr} if it is missing. */
public class SQLExprParentSetter extends MySqlASTVisitorAdapter {

  /** Fix null parent problem which is required by SQLIdentifier.visit() */
  @Override
  public boolean visit(SQLInSubQueryExpr subQuery) {
    subQuery.getExpr().setParent(subQuery);
    return true;
  }

  /** Fix null parent problem which is required by SQLIdentifier.visit() */
  @Override
  public boolean visit(SQLInListExpr expr) {
    expr.getExpr().setParent(expr);
    return true;
  }

  /** Fix the expr in {@link SQLNotExpr} without parent. */
  @Override
  public boolean visit(SQLNotExpr notExpr) {
    notExpr.getExpr().setParent(notExpr);
    return true;
  }
}
