/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.ordinal;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlSelectGroupByExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.SQLExprParser;
import java.util.List;
import org.opensearch.sql.legacy.parser.ElasticSqlExprParser;
import org.opensearch.sql.legacy.rewriter.RewriteRule;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;

/**
 * Rewrite rule for changing ordinal alias in order by and group by to actual select field. Since we
 * cannot clone or deepcopy the Druid SQL objects, we need to generate the two syntax tree from the
 * original query to map Group By and Order By fields with ordinal alias to Select fields in newly
 * generated syntax tree.
 *
 * <p>This rewriter assumes that all the backticks have been removed from identifiers. It also
 * assumes that table alias have been removed from SELECT, WHERE, GROUP BY, ORDER BY fields.
 */
public class OrdinalRewriterRule implements RewriteRule<SQLQueryExpr> {

  private final String sql;

  public OrdinalRewriterRule(String sql) {
    this.sql = sql;
  }

  @Override
  public boolean match(SQLQueryExpr root) {
    SQLSelectQuery sqlSelectQuery = root.getSubQuery().getQuery();
    if (!(sqlSelectQuery instanceof MySqlSelectQueryBlock)) {
      // it could be SQLUnionQuery
      return false;
    }

    MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlSelectQuery;
    if (!hasGroupByWithOrdinals(query) && !hasOrderByWithOrdinals(query)) {
      return false;
    }
    return true;
  }

  @Override
  public void rewrite(SQLQueryExpr root) {
    // we cannot clone SQLSelectItem, so we need similar objects to assign to GroupBy and OrderBy
    // items
    SQLQueryExpr sqlExprGroupCopy = toSqlExpr();
    SQLQueryExpr sqlExprOrderCopy = toSqlExpr();

    changeOrdinalAliasInGroupAndOrderBy(root, sqlExprGroupCopy, sqlExprOrderCopy);
  }

  private void changeOrdinalAliasInGroupAndOrderBy(
      SQLQueryExpr root, SQLQueryExpr exprGroup, SQLQueryExpr exprOrder) {
    root.accept(
        new MySqlASTVisitorAdapter() {

          private final String groupException = "Invalid ordinal [%s] specified in [GROUP BY %s]";
          private final String orderException = "Invalid ordinal [%s] specified in [ORDER BY %s]";

          private final List<SQLSelectItem> groupSelectList =
              ((MySqlSelectQueryBlock) exprGroup.getSubQuery().getQuery()).getSelectList();

          private final List<SQLSelectItem> orderSelectList =
              ((MySqlSelectQueryBlock) exprOrder.getSubQuery().getQuery()).getSelectList();

          @Override
          public boolean visit(MySqlSelectGroupByExpr groupByExpr) {
            SQLExpr expr = groupByExpr.getExpr();
            if (expr instanceof SQLIntegerExpr) {
              Integer ordinalValue = ((SQLIntegerExpr) expr).getNumber().intValue();
              SQLExpr newExpr = checkAndGet(groupSelectList, ordinalValue, groupException);
              groupByExpr.setExpr(newExpr);
              newExpr.setParent(groupByExpr);
            }
            return false;
          }

          @Override
          public boolean visit(SQLSelectOrderByItem orderByItem) {
            SQLExpr expr = orderByItem.getExpr();
            Integer ordinalValue;

            if (expr instanceof SQLIntegerExpr) {
              ordinalValue = ((SQLIntegerExpr) expr).getNumber().intValue();
              SQLExpr newExpr = checkAndGet(orderSelectList, ordinalValue, orderException);
              orderByItem.setExpr(newExpr);
              newExpr.setParent(orderByItem);
            } else if (expr instanceof SQLBinaryOpExpr
                && ((SQLBinaryOpExpr) expr).getLeft() instanceof SQLIntegerExpr) {
              // support ORDER BY IS NULL/NOT NULL
              SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) expr;
              SQLIntegerExpr integerExpr = (SQLIntegerExpr) binaryOpExpr.getLeft();

              ordinalValue = integerExpr.getNumber().intValue();
              SQLExpr newExpr = checkAndGet(orderSelectList, ordinalValue, orderException);
              binaryOpExpr.setLeft(newExpr);
              newExpr.setParent(binaryOpExpr);
            }

            return false;
          }
        });
  }

  private SQLExpr checkAndGet(List<SQLSelectItem> selectList, Integer ordinal, String exception) {
    if (ordinal > selectList.size()) {
      throw new VerificationException(String.format(exception, ordinal, ordinal));
    }

    return selectList.get(ordinal - 1).getExpr();
  }

  private boolean hasGroupByWithOrdinals(MySqlSelectQueryBlock query) {
    if (query.getGroupBy() == null) {
      return false;
    } else if (query.getGroupBy().getItems().isEmpty()) {
      return false;
    }

    return query.getGroupBy().getItems().stream()
        .anyMatch(
            x ->
                x instanceof MySqlSelectGroupByExpr
                    && ((MySqlSelectGroupByExpr) x).getExpr() instanceof SQLIntegerExpr);
  }

  private boolean hasOrderByWithOrdinals(MySqlSelectQueryBlock query) {
    if (query.getOrderBy() == null) {
      return false;
    } else if (query.getOrderBy().getItems().isEmpty()) {
      return false;
    }

    /**
     *
     *
     * <pre>
     * The second condition checks valid AST that meets ORDER BY IS NULL/NOT NULL condition
     *
     *            SQLSelectOrderByItem
     *                      |
     *             SQLBinaryOpExpr (Is || IsNot)
     *                    /  \
     *    SQLIdentifierExpr  SQLNullExpr
     *  </pre>
     */
    return query.getOrderBy().getItems().stream()
        .anyMatch(
            x ->
                x.getExpr() instanceof SQLIntegerExpr
                    || (x.getExpr() instanceof SQLBinaryOpExpr
                        && ((SQLBinaryOpExpr) x.getExpr()).getLeft() instanceof SQLIntegerExpr));
  }

  private SQLQueryExpr toSqlExpr() {
    SQLExprParser parser = new ElasticSqlExprParser(sql);
    SQLExpr expr = parser.expr();
    return (SQLQueryExpr) expr;
  }
}
