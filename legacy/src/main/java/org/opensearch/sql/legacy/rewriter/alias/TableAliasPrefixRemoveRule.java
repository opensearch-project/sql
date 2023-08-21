/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.alias;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.opensearch.sql.legacy.rewriter.RewriteRule;
import org.opensearch.sql.legacy.rewriter.subquery.utils.FindSubQuery;

/** Rewrite rule for removing table alias or table name prefix in field name. */
public class TableAliasPrefixRemoveRule implements RewriteRule<SQLQueryExpr> {

  /** Table aliases in FROM clause. Store table name for those without alias. */
  private final Set<String> tableAliasesToRemove = new HashSet<>();

  @Override
  public boolean match(SQLQueryExpr root) {
    if (hasSubQuery(root)) {
      return false;
    }
    collectTableAliasesThatCanBeRemoved(root);
    return !tableAliasesToRemove.isEmpty();
  }

  @Override
  public void rewrite(SQLQueryExpr root) {
    removeTableAliasPrefixInColumnName(root);
  }

  private boolean hasSubQuery(SQLQueryExpr root) {
    FindSubQuery visitor = new FindSubQuery();
    root.accept(visitor);
    return visitor.hasSubQuery();
  }

  private void collectTableAliasesThatCanBeRemoved(SQLQueryExpr root) {
    visitNonJoinedTable(
        root,
        tableExpr -> {
          Table table = new Table(tableExpr);
          if (table.hasAlias()) {
            tableAliasesToRemove.add(table.alias());
            table.removeAlias();
          } else {
            tableAliasesToRemove.add(table.name());
          }
        });
  }

  private void removeTableAliasPrefixInColumnName(SQLQueryExpr root) {
    visitColumnName(
        root,
        idExpr -> {
          Identifier field = new Identifier(idExpr);
          if (field.hasPrefix() && tableAliasesToRemove.contains(field.prefix())) {
            field.removePrefix();
          }
        });
  }

  private void visitNonJoinedTable(SQLQueryExpr root, Consumer<SQLExprTableSource> visit) {
    root.accept(
        new MySqlASTVisitorAdapter() {
          @Override
          public boolean visit(SQLJoinTableSource x) {
            // Avoid visiting table name in any JOIN including comma/inner/left join
            //  between 2 indices or between index and nested field.
            // For the latter case, alias is taken care of in {@link NestedFieldRewriter}.
            return false;
          }

          @Override
          public void endVisit(SQLExprTableSource tableExpr) {
            visit.accept(tableExpr);
          }
        });
  }

  private void visitColumnName(SQLQueryExpr expr, Consumer<SQLIdentifierExpr> visit) {
    expr.accept(
        new MySqlASTVisitorAdapter() {
          @Override
          public boolean visit(SQLExprTableSource x) {
            return false; // Avoid rewriting identifier in table name
          }

          @Override
          public void endVisit(SQLIdentifierExpr idExpr) {
            visit.accept(idExpr);
          }
        });
  }
}
