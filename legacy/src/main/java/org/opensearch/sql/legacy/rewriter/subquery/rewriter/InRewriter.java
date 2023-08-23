/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.subquery.rewriter;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.opensearch.sql.legacy.rewriter.subquery.RewriterContext;

/**
 *
 *
 * <pre>
 * IN Subquery Rewriter.
 * For example,
 * SELECT * FROM A WHERE a IN (SELECT b FROM B) and c > 10 should be rewritten to
 * SELECT A.* FROM A JOIN B ON A.a = B.b WHERE c > 10 and B.b IS NOT NULL.
 * </pre>
 */
public class InRewriter implements Rewriter {
  private final SQLInSubQueryExpr inExpr;
  private final RewriterContext ctx;
  private final MySqlSelectQueryBlock queryBlock;

  public InRewriter(SQLInSubQueryExpr inExpr, RewriterContext ctx) {
    this.inExpr = inExpr;
    this.ctx = ctx;
    this.queryBlock = (MySqlSelectQueryBlock) inExpr.getSubQuery().getQuery();
  }

  @Override
  public boolean canRewrite() {
    return !inExpr.isNot();
  }

  /**
   *
   *
   * <pre>
   * Build Where clause from input query.
   * <p>
   * With the input query.
   *         Query
   *      /    |     \
   *  SELECT  FROM  WHERE
   *  |        |    /   |  \
   *  *         A  c>10 AND INSubquery
   *                         /    \
   *                        a    Query
   *                        /      \
   *                           SELECT FROM
   *                             |     |
   *                             b     B
   * <p>
   * </pre>
   */
  @Override
  public void rewrite() {
    SQLTableSource from = queryBlock.getFrom();
    addJoinTable(from);

    SQLExpr where = queryBlock.getWhere();
    if (null == where) {
      ctx.addWhere(generateNullOp());
    } else if (where instanceof SQLBinaryOpExpr) {
      ctx.addWhere(and(generateNullOp(), (SQLBinaryOpExpr) where));
    } else {
      throw new IllegalStateException("unsupported where class type " + where.getClass());
    }
  }

  /**
   * Build the Null check expression. For example,<br>
   * SELECT * FROM A WHERE a IN (SELECT b FROM B)<br>
   * should return B.b IS NOT NULL
   */
  private SQLBinaryOpExpr generateNullOp() {
    SQLBinaryOpExpr binaryOpExpr = new SQLBinaryOpExpr();
    binaryOpExpr.setLeft(fetchJoinExpr());
    binaryOpExpr.setRight(new SQLNullExpr());
    binaryOpExpr.setOperator(SQLBinaryOperator.IsNot);

    return binaryOpExpr;
  }

  /**
   * Add the {@link SQLTableSource} with {@link JoinType} and {@link SQLBinaryOpExpr} to the {@link
   * RewriterContext}.
   */
  private void addJoinTable(SQLTableSource right) {
    SQLBinaryOpExpr binaryOpExpr =
        new SQLBinaryOpExpr(inExpr.getExpr(), SQLBinaryOperator.Equality, fetchJoinExpr());
    ctx.addJoin(right, JoinType.JOIN, binaryOpExpr);
  }

  private SQLExpr fetchJoinExpr() {
    if (queryBlock.getSelectList().size() > 1) {
      throw new IllegalStateException(
          "Unsupported subquery with multiple select " + queryBlock.getSelectList());
    }
    return queryBlock.getSelectList().get(0).getExpr();
  }
}
