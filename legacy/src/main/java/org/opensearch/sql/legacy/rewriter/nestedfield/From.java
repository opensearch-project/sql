/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.nestedfield;

import static com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType.COMMA;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

/** Table (OpenSearch Index) expression in FROM statement. */
class From extends SQLClause<SQLTableSource> {

  From(SQLTableSource expr) {
    super(expr);
  }

  /**
   * Collect nested field(s) information and then remove them from FROM statement. Assumption: only
   * 1 regular table in FROM (which is the first one) and nested field(s) has alias.
   */
  @Override
  void rewrite(Scope scope) {
    if (!isJoin()) {
      return;
    }

    // At this point, FROM expr is SQLJoinTableSource.
    if (!isCommaJoin()) {
      scope.setActualJoinType(((SQLJoinTableSource) expr).getJoinType());
      ((SQLJoinTableSource) expr).setJoinType(COMMA);
    }

    if (parentAlias(scope).isEmpty()) {
      // Could also be empty now since normal JOIN tables may not have alias
      if (scope.getActualJoinType() != null) {
        ((SQLJoinTableSource) expr).setJoinType(scope.getActualJoinType());
      }
      return;
    }

    collectNestedFields(scope);
    if (scope.isAnyNestedField()) {
      eraseParentAlias();
      keepParentTableOnly();
    } else if (scope.getActualJoinType() != null) {
      // set back the JoinType to original value if non COMMA JOIN on regular tables
      ((SQLJoinTableSource) expr).setJoinType(scope.getActualJoinType());
    }
  }

  private String parentAlias(Scope scope) {
    scope.setParentAlias(((SQLJoinTableSource) expr).getLeft().getAlias());
    return emptyIfNull(scope.getParentAlias());
  }

  /** Erase alias otherwise NLPchina has problem parsing nested field like 't.employees.name' */
  private void eraseParentAlias() {
    left().expr.setAlias(null);
  }

  private void keepParentTableOnly() {
    MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) expr.getParent();
    query.setFrom(left().expr);
    left().expr.setParent(query);
  }

    /**
     * <pre>
     * Collect path alias and full path mapping of nested field in FROM clause.
     * Sample:
     * FROM team t, t.employees e ...
     * <p>
     * Join
     * /    \
     * team t    Join
     * /    \
     * t.employees e  ...
     * <p>
     * t.employees is nested because path "t" == parentAlias "t"
     * Save path alias to full path name mapping {"e": "employees"} to Scope
     * </pre>
     */
    private void collectNestedFields(Scope scope) {
        From clause = this;
        for (; clause.isCommaJoin(); clause = clause.right()) {
            clause.left().addIfNestedField(scope);
        }
        clause.addIfNestedField(scope);
    }

  private boolean isCommaJoin() {
    return expr instanceof SQLJoinTableSource && ((SQLJoinTableSource) expr).getJoinType() == COMMA;
  }

  private boolean isJoin() {
    return expr instanceof SQLJoinTableSource;
  }

  private From left() {
    return new From(((SQLJoinTableSource) expr).getLeft());
  }

  private From right() {
    return new From(((SQLJoinTableSource) expr).getRight());
  }

  private void addIfNestedField(Scope scope) {
    if (!(expr instanceof SQLExprTableSource
        && ((SQLExprTableSource) expr).getExpr() instanceof SQLIdentifierExpr)) {
      return;
    }

    Identifier table = new Identifier((SQLIdentifierExpr) ((SQLExprTableSource) expr).getExpr());
    if (table.path().equals(scope.getParentAlias())) {
      scope.addAliasFullPath(emptyIfNull(expr.getAlias()), table.name());
    }
  }

  private String emptyIfNull(String str) {
    return str == null ? "" : str;
  }
}
