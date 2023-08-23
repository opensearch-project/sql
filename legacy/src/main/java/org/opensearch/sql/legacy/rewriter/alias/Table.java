/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.alias;

import static com.alibaba.druid.sql.ast.expr.SQLBinaryOperator.Divide;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.google.common.base.Strings;

/** Util class for table expression parsing */
class Table {

  private final SQLExprTableSource tableExpr;

  Table(SQLExprTableSource tableExpr) {
    this.tableExpr = tableExpr;
  }

  boolean hasAlias() {
    return !alias().isEmpty();
  }

  String alias() {
    return Strings.nullToEmpty(tableExpr.getAlias());
  }

  void removeAlias() {
    tableExpr.setAlias(null);
  }

  /** Extract table name in table expression */
  String name() {
    SQLExpr expr = tableExpr.getExpr();
    if (expr instanceof SQLIdentifierExpr) {
      return ((SQLIdentifierExpr) expr).getName();
    } else if (isTableWithType(expr)) {
      return ((SQLIdentifierExpr) ((SQLBinaryOpExpr) expr).getLeft()).getName();
    }
    return expr.toString();
  }

  /** Return true for table name along with type name, for example 'accounts/_doc' */
  private boolean isTableWithType(SQLExpr expr) {
    return expr instanceof SQLBinaryOpExpr
        && ((SQLBinaryOpExpr) expr).getLeft() instanceof SQLIdentifierExpr
        && ((SQLBinaryOpExpr) expr).getOperator() == Divide;
  }
}
