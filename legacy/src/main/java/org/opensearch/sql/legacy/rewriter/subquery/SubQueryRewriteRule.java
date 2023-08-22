/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.subquery;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import java.sql.SQLFeatureNotSupportedException;
import org.opensearch.sql.legacy.rewriter.RewriteRule;
import org.opensearch.sql.legacy.rewriter.subquery.rewriter.SubqueryAliasRewriter;
import org.opensearch.sql.legacy.rewriter.subquery.utils.FindSubQuery;

/** Subquery Rewriter Rule. */
public class SubQueryRewriteRule implements RewriteRule<SQLQueryExpr> {
  private FindSubQuery findAllSubQuery = new FindSubQuery();

  @Override
  public boolean match(SQLQueryExpr expr) throws SQLFeatureNotSupportedException {
    expr.accept(findAllSubQuery);

    if (isContainSubQuery(findAllSubQuery)) {
      if (isSupportedSubQuery(findAllSubQuery)) {
        return true;
      } else {
        throw new SQLFeatureNotSupportedException(
            "Unsupported subquery. Only one EXISTS or IN is supported");
      }
    } else {
      return false;
    }
  }

  @Override
  public void rewrite(SQLQueryExpr expr) {
    expr.accept(new SubqueryAliasRewriter());
    new SubQueryRewriter().convert(expr.getSubQuery());
  }

  private boolean isContainSubQuery(FindSubQuery allSubQuery) {
    return !allSubQuery.getSqlExistsExprs().isEmpty()
        || !allSubQuery.getSqlInSubQueryExprs().isEmpty();
  }

  private boolean isSupportedSubQuery(FindSubQuery allSubQuery) {
    if ((allSubQuery.getSqlInSubQueryExprs().size() == 1
            && allSubQuery.getSqlExistsExprs().size() == 0)
        || (allSubQuery.getSqlInSubQueryExprs().size() == 0
            && allSubQuery.getSqlExistsExprs().size() == 1)) {
      return true;
    }
    return false;
  }
}
